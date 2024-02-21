# Databricks notebook source
# MAGIC %pip install --upgrade gradio==3.38.0 fastapi==0.104 uvicorn==0.24
# MAGIC %pip install typing-extensions==4.8.0 --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
os.environ['OPENAI_API_KEY'] = dbutils.secrets.get('agent_studio','open_ai')
os.environ['DATABRICKS_TOKEN'] = dbutils.secrets.get('agent_studio','databricks_token')
os.environ['DATABRICKS_HOST'] = dbutils.secrets.get('agent_studio','databricks_host')
os.environ['AGENT_STUDIO_PATH'] = dbutils.secrets.get('agent_studio','folder_path')

from Core.AgentCreator import AgentParser
import gradio as gr

cluster_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('clusterId')

# COMMAND ----------

# MAGIC %md ### Helper functions

# COMMAND ----------



def submit_prompt(prompt):
  global message_thread 
  
  headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}

  message_thread.append(
    {
      "role": "user",
      "content": prompt
    }
  )

  payload = {
    "inputs": [{'messages': message_thread}]
  }

  response = requests.post(chatbot_model_serving_endpoint, headers=headers, json=payload)

  if response.status_code == 200:
      resp = response.json()
      bot_message = resp['predictions']['choices'][0]['message']['content']
      
      message_thread = resp['predictions']['thread']
      return bot_message
  else:
      error = response.json()
      raise ValueError(f'Error submitting job: {error}')
  



# COMMAND ----------

def generate_output(message: str,
        chat_history: list[tuple[str, str]],
        # system_prompt: str,
        max_new_tokens: int = 300,
        temperature: float = 0.8,
        top_p: float = 0.95,
        top_k: int = 50):
    
    output = submit_prompt(message)
    return output

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Let's host it in gradio

# COMMAND ----------

import json
from dataclasses import dataclass

import uvicorn
from fastapi import FastAPI

# COMMAND ----------

@dataclass
class ProxySettings:
    proxy_url: str
    port: str
    url_base_path: str


class DatabricksApp:

    def __init__(self, port):
        # self._app = data_app
        self._port = port
        import IPython
        self._dbutils = IPython.get_ipython().user_ns["dbutils"]
        self._display_html = IPython.get_ipython().user_ns["displayHTML"]
        self._context = json.loads(self._dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
        # need to do this after the context is set
        self._cloud = self.get_cloud()
        # create proxy settings after determining the cloud
        self._ps = self.get_proxy_settings()
        self._fastapi_app = self._make_fastapi_app(root_path=self._ps.url_base_path.rstrip("/"))
        self._streamlit_script = None
        # after everything is set print out the url

    def _make_fastapi_app(self, root_path) -> FastAPI:
        fast_api_app = FastAPI(root_path=root_path)

        @fast_api_app.get("/")
        def read_main():
            return {
                "routes": [
                    {"method": "GET", "path": "/", "summary": "Landing"},
                    {"method": "GET", "path": "/status", "summary": "App status"},
                    {"method": "GET", "path": "/dash", "summary": "Sub-mounted Dash application"},
                ]
            }

        @fast_api_app.get("/status")
        def get_status():
            return {"status": "ok"}

        return fast_api_app

    def get_proxy_settings(self) -> ProxySettings:
        if self._cloud.lower() not in ["aws", "azure"]:
            raise Exception("only supported in aws or azure")

        org_id = self._context["tags"]["orgId"]
        org_shard = ""
        # org_shard doesnt need a suffix of "." for dnsname its handled in building the url
        if self._cloud.lower() == "azure":
            org_shard_id = int(org_id) % 20
            org_shard = f".{org_shard_id}"
        cluster_id = self._context["tags"]["clusterId"]
        url_base_path = f"/driver-proxy/o/{org_id}/{cluster_id}/{self._port}"

        from dbruntime.databricks_repl_context import get_context
        host_name = get_context().browserHostName
        proxy_url = f"https://{host_name}/driver-proxy/o/{org_id}/{cluster_id}/{self._port}/"

        return ProxySettings(
            proxy_url=proxy_url,
            port=self._port,
            url_base_path=url_base_path
        )

    @property
    def app_url_base_path(self):
        return self._ps.url_base_path

    def mount_gradio_app(self, gradio_app):
        import gradio as gr
        # gradio_app.queue()
        gr.mount_gradio_app(self._fastapi_app, gradio_app, f"/gradio")
        # self._fastapi_app.mount("/gradio", gradio_app)
        self.display_url(self.get_gradio_url())

    def get_cloud(self):
        if self._context["extraContext"]["api_url"].endswith("azuredatabricks.net"):
            return "azure"
        return "aws"

    def get_gradio_url(self):
        # must end with a "/" for it to not redirect
        return f'<a href="{self._ps.proxy_url}gradio/">Click to go to Gradio App!</a>'

    def display_url(self, url):
        self._display_html(url)

    def run(self):
        print(self.app_url_base_path)
        uvicorn.run(self._fastapi_app, host="0.0.0.0", port=self._port)

# COMMAND ----------

def create_agent(yaml_str):
  p = AgentParser(cluster_id)#'0822-172318-4b9whfq4'
  gr.Info("Launching Agent. This will take a minute. A second notification will present when it's complete.")
  msg = p.create_agent(yaml_str)
  gr.Info(msg)


# COMMAND ----------

import os
import yaml

directory = os.path.join(os.environ.get('AGENT_STUDIO_PATH'), "Tool/yaml/")
#"/Volumes/robert_mosley/sql_ai/files/agent_studio/tool"
data_array = []

# Iterate through the files in the directory
for filename in os.listdir(directory):
    filepath = os.path.join(directory, filename)
    
    # Check if the file has a YAML extension
    if filename.endswith(".yaml") or filename.endswith(".yml"):
        # Load the YAML object from the file
        with open(filepath, "r") as file:
            yaml_obj = yaml.safe_load(file)
        
        # Append the YAML object to the data array
        data_array.append(yaml_obj)

# Use the data_array as needed
tools_ref = {t['tool']: t for t in data_array}
tools_dict = {t['tool']: t['documentation'] for t in data_array}

# COMMAND ----------

agent_dict = {
  "name":"",
  "description": "",
  "provider":"", #openai, azure, databricks
  "model":"",
  "instruction": "",
  "tools":[],
  "pip_requirements":[]
}

def set_values():
  return agent_dict['name'], agent_dict['description'], agent_dict['provider'], agent_dict['model'], agent_dict['instruction']

def yaml_update(yaml_text):
  global agent_dict
  print(f"YAML: {yaml_text}")
  agent_dict = yaml.safe_load(yaml_text)
  print(agent_dict)
  return set_values()

def update_field(field_name):
  def set_field(input):
    global agent_dict
    agent_dict[field_name] = input
    return yaml_string()
  return set_field

# COMMAND ----------

import yaml

def yaml_string():
  return yaml.dump(agent_dict, sort_keys=False, indent=4, width=2147483647)

def set_tool_desc(tool):
  if tool == '':
    return gr.Markdown.update('')
  else:
    return gr.Markdown.update(tools_dict[tool])
  
def parse_param(param):
  return {
    "index":int(param['ordinal_position']) - 1,
    "name":param['parameter_name'],
    "value":param['default_value'],
    "type":param['parameter_type'],
    "kind":param['parameter_kind'],
  }

def add_agent_tool(selected_tool, yaml_text):
  if (selected_tool == ''):
    gr.Error('No Tool Selected!')
    return yaml_text
  else:
    new_tool = tools_ref[selected_tool]

    new_tool_params = new_tool['parameters']

    agent_dict['tools'].append({
      "tool": new_tool['tool'],
      "description": new_tool['documentation'],
      "parameters":{p["name"]: p["default"] for p in new_tool_params}
    })

    agent_dict["pip_requirements"] = list(set(agent_dict["pip_requirements"] + new_tool["pip_requirements"]))

    return yaml_string()

#agent_tools = [{"index"=0, "name"="tool_name", "parameters"=[]}]

# COMMAND ----------

tools_ref

# COMMAND ----------

import gradio as gr

with gr.Blocks(theme=gr.themes.Base()) as agent_studio:
  with gr.Group():
    with gr.Row():
      name = gr.Textbox(agent_dict['name'], label='Name')
    with gr.Row():
      provider = gr.Dropdown(label='Provider', choices=['openai', 'databricks'], interactive=True)
      model = gr.Textbox(label='Model')
    with gr.Row():
      description = gr.Textbox(label='Description', lines=5)
    with gr.Row():
      instruction = gr.Textbox(label='Instruction', lines=5)
  with gr.Group():
    with gr.Row():
      tool_list = gr.Dropdown(show_label=False, choices=tools_dict.keys(), scale=2)
      addtool_button = gr.Button('Add', size='sm', scale=0)
    with gr.Row():
      tool_desc = gr.Markdown("Select a tool to see it's description.")
      tool_list.change(fn=set_tool_desc,
        inputs=[tool_list],
        outputs=[tool_desc])
  with gr.Group():
    with gr.Row():
      agent_yaml = gr.Textbox(yaml_string, label='Agent yaml', lines=10)
  with gr.Group():  
    with gr.Row():
      register = gr.Button('Log, Register & Deploy')

  name.blur(fn=update_field("name"), inputs=[name], outputs=[agent_yaml])
  provider.change(fn=update_field("provider"), inputs=[provider], outputs=[agent_yaml])
  model.blur(fn=update_field("model"), inputs=[model], outputs=[agent_yaml])
  description.blur(fn=update_field("description"), inputs=[description], outputs=[agent_yaml])
  instruction.blur(fn=update_field("instruction"), inputs=[instruction], outputs=[agent_yaml])
      
  addtool_button.click(fn=add_agent_tool, inputs=[tool_list, agent_yaml], outputs=[agent_yaml])
  agent_yaml.input(fn=yaml_update, inputs=[agent_yaml], outputs=[name, description, provider, model, instruction])

  register.click(fn=create_agent, inputs=[agent_yaml])
  
          
      

# COMMAND ----------

app_port = 8765

# COMMAND ----------

print(spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId"))

# COMMAND ----------

cluster_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None)
workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")

print(f"Use this URL to access the chatbot app: ")
print(f"https://dbc-dp-{workspace_id}.cloud.databricks.com/driver-proxy/o/{workspace_id}/{cluster_id}/{app_port}/gradio/")

# COMMAND ----------

dbx_app = DatabricksApp(app_port)

# demo.queue()
dbx_app.mount_gradio_app(agent_studio)

import nest_asyncio
nest_asyncio.apply()
dbx_app.run()
