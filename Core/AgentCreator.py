import os
import json
import yaml
import requests
import time

class AgentParser():
  def __init__(self, cluster_id):
    self.cluster_id = cluster_id
    self.studio_location = os.getenv("AGENT_STUDIO_PATH") #'/Workspace/Repos/robert.mosley@databricks.com/DatabricksChat_lc/'
    self.agent_location = os.path.join(self.studio_location, 'Agent/')
    self.db_api_token = os.getenv("DATABRICKS_TOKEN")
    self.databricks_instance = os.getenv("DATABRICKS_HOST")
    self.endpoint_config = '''endpoint_config = {
  "environment_vars": {
    "OPENAI_API_KEY": "{{secrets/agent_studio/open_ai}}",
    "DATABRICKS_TOKEN": "{{secrets/agent_studio/databricks_token}}",
    "DATABRICKS_HOST": "{{secrets/agent_studio/databricks_host}}",
  }
}'''
    self.master_template = '''# Databricks notebook source
# MAGIC %pip install mlflow[databricks]

# COMMAND ----------

# DBTITLE 1,Install Requirements
%pip install langchain-openai langchain-community
{pip_requirements}

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Objects for Agent Model
from Core.DatabricksModel import ChatModel, log_model, register_model, create_endpoint
{import_tools}

# COMMAND ----------
 
# DBTITLE 1,Set Environment Variables
import os
os.environ['OPENAI_API_KEY']  = dbutils.secrets.get('agent_studio','open_ai')
os.environ['DATABRICKS_TOKEN']  = dbutils.secrets.get('agent_studio','databricks_token')
os.environ['DATABRICKS_HOST']  = dbutils.secrets.get('agent_studio','databricks_host')

# COMMAND ----------

# DBTITLE 1,Instantiate Agent Tools
{declare_tools}

# COMMAND ----------

# DBTITLE 1,Create Agent Model
{agent_config}

model = ChatModel(config, helpers)

# COMMAND ----------

# DBTITLE 1,Log Model Experiment
run_id = log_model(model, '{short_model_name}', pip_req = {model_pip}, agent_studio_path = "{agent_studio_path}")

# COMMAND ----------

# DBTITLE 1,Register Model
registered_model = register_model(run_id, "{long_model_name}", 'prod')

# COMMAND ----------

# DBTITLE 1,Create/Update Endpoint
endpoint_name = "{short_model_name}"

{endpoint_config}

response = create_endpoint(endpoint_name, registered_model, endpoint_config)
'''

  def __generate_code(self, agent_dict):
    pip = agent_dict.get('pip_requirements',[])
    tools = agent_dict.get('tools', [])

    pip_requirements = self.__generate_pip_requirements(pip)
    import_tools = self.__generate_imports(tools)
    declare_tools = self.__generate_tools(tools)
    agent_config = self.__generate_config(agent_dict)
    long_model_name = agent_dict['name']
    short_model_name = long_model_name.split('.')[-1]
    endpoint_config = self.endpoint_config
    agent_studio_path = self.studio_location

    return self.master_template.format(pip_requirements = pip_requirements, 
                                       model_pip = str(pip),
                                       import_tools = import_tools,
                                       declare_tools = declare_tools,
                                       agent_config = agent_config,
                                       short_model_name = short_model_name,
                                       long_model_name = long_model_name,
                                       endpoint_config = endpoint_config,
                                       agent_studio_path = agent_studio_path)




  def __generate_pip_requirements(self, pip):

    pip_build = [f"# MAGIC %pip install {pip_rec}" for pip_rec in pip]
    pip_str = '\n'.join(pip_build)

    return pip_str
  
  def __generate_imports(self, tools):
    imports = [f"from Tool.{t['tool'].split('.')[0]} import {t['tool'].split('.')[1]}" for t in tools]

    return '\n'.join(imports)
  
  def __generate_tools(self, tools):
    tool_list = []
    helper_list = []
    for index, tool in enumerate(tools):
      pars = []
      for p in tool["parameters"].keys():
        par_value = tool['parameters'][p]
        #checks to see if the value is a string and then wraps it with double quotes.
        if isinstance(par_value, str):
          par_value = f'"{par_value}"'
        pars.append(f"{p} = {par_value}")

      tool_str = f"tool_{index} = {tool['tool'].split('.')[1]}({', '.join(pars)})"
      tool_list.append(tool_str)
      helper_list.append(f"tool_{index}")
    #vs = DBVectorSearch(databricks_token, workspaceUrl, "dbdemos_vs_endpoint", "main.rag_chatbot.databricks_documentation_vs_index", ['id', 'url', 'content'], function_descriptor = "This repository holds Databricks documentation.")

    ret = '\n'.join(tool_list)
    ret += '\n\n'
    ret += f"helpers = [{', '.join(helper_list)}]"
    
    return ret
  
  def __generate_config(self, agent):
    import json

    config = {
      "provider": agent.get('provider', 'openai'),
      "model": agent.get('model', 'gpt-4-1106-preview'),
      "endpoint_type": agent.get('endpoint_type', 'chat'),
      "instruction_prompt": agent.get('instruction', ''),
      "log_directory": agent.get('log_directory', '')
    }

    return f"config = {json.dumps(config)}"

  def create_agent(self, agent_yaml):
    #print(f'LLM Response: {response}')
    agent_dict = yaml.safe_load(agent_yaml)

    yaml_path = f"{self.agent_location}yaml/{agent_dict['name'].replace('.', '_')}.yaml"
    yaml_path = self.__create_file(yaml_path, agent_yaml, 'text/yaml')

    notebook_path = f"{self.agent_location}{agent_dict['name'].replace('.', '_')}"
    content = self.__generate_code(agent_dict)

    file_path = self.__create_python_notebook(notebook_path, content)
    run_result = self.__run_notebook(file_path)

    return run_result



  def __create_python_notebook(self, file_path: str, content: str):
    """Creates a Databricks notebook and returns the file path."""

    headers = {'Authorization': f'Bearer {self.db_api_token}', 'Content-Type': 'application/json'}
    payload = {
      "path": file_path,
      "format": "SOURCE",
      "language": "PYTHON",
      "content": self.__to_base64(content),
      "overwrite": True
    }
    response = requests.post(f'{self.databricks_instance}/api/2.0/workspace/import', headers=headers, json=payload)

    if response.status_code == 200:
        print(f'File Created: {file_path}')
        return file_path
    else:
        error = response.json()
        raise ValueError(f'Error submitting job: {error}')

  def __create_file(self, file_path: str, content: str, content_type: str):
    """Creates a Databricks notebook and returns the file path."""

    headers = {'Authorization': f'Bearer {self.db_api_token}', 'Content-Type': content_type}
    payload = {
      "path": file_path,
      "format": "AUTO",
      "content": self.__to_base64(content),
      "overwrite": True
    }
    response = requests.post(f'{self.databricks_instance}/api/2.0/workspace/import', headers=headers, json=payload)

    if response.status_code == 200:
        print(f'File Created: {file_path}')
        return file_path
    else:
        error = response.json()
        raise ValueError(f'Error submitting job: {error}')
  
  def __to_base64(self, content: str):
    import base64

    content_bytes = content.encode('ascii')
    base64_bytes = base64.b64encode(content_bytes)
    base64_string = base64_bytes.decode('ascii')

    return base64_string

  def __from_base64(self, content: str):
    import base64

    content_bytes = content.encode('ascii')
    base64_bytes = base64.b64decode(content_bytes)
    base64_string = base64_bytes.decode('ascii')

    return base64_string

  def __run_notebook(self, file_path: str):
    """Runs a Databricks notebook and returns the result."""
    print(f'Running notebook: {file_path}')
    run_id = self.__start_notebook_task(file_path)
    print(f'run_id: {run_id}')

    while True:
      result, state, message = self.__get_run_detail(run_id)
      print(result)
      if state != '':
        return f'{state}: {message}'
      time.sleep(5)

  def __start_notebook_task(self, file_path: str):
    """Starts a Databricks notebook and returns the job_id."""

    headers = {'Authorization': f'Bearer {self.db_api_token}', 'Content-Type': 'application/json'}
    payload = {
      "tasks": [
        {
          "task_key": "notebook_task_1",
          "existing_cluster_id": self.cluster_id,
          "notebook_task": {
            "notebook_path": file_path,
            "source": "WORKSPACE",
          }
        }
      ],
      "run_name": "Building Agent: " + os.path.basename(file_path)
    }
    response = requests.post(f'{self.databricks_instance}/api/2.1/jobs/runs/submit', headers=headers, json=payload)

    if response.status_code == 200:
        job_id = response.json()['run_id']
        return job_id
    else:
        error = response.json()
        raise ValueError(f'Error submitting job: {error}')

  def __get_run_detail(self, run_id: int):

    headers = {'Authorization': f'Bearer {self.db_api_token}', 'Content-Type': 'application/json'}
    response = requests.get(f'{self.databricks_instance}/api/2.0/jobs/runs/get?run_id={run_id}', headers=headers)

    if response.status_code == 200:
        result = response.json()['state']
        state = result.get('result_state', '')
        message = result.get('state_message', '')
        return result, state, message

    else:
        error = response.json()
        raise ValueError(f'Error getting job status and result: {error}')