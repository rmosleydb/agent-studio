import mlflow
from mlflow.pyfunc import PythonModel
import pandas as pd
import numpy as np
import requests, json, os
import uuid
from Core.Tool import ChatBot


#os.environ['OPENAI_KEY']        = dbutils.secrets.get('tjc','openaicp')


class ChatModel(mlflow.pyfunc.PythonModel):
  
  def __init__(self, config = {}, helpers = []):
    self.config = config
    self.helpers = helpers

    self.llm_method = config.get('llm_method', 'openai') #openai, azureai or gateway
    self.model = config.get('model', 'gpt-4-1106-preview')
    self.gateway = config.get('gateway', 'databricks')
    self.route = config.get('route', '')
    self.instruction_prompt = config.get('instruction_prompt', 'You are an assistant.')
    self.log_dir = config.get('log_directory', '')

  def load_context(self, context):
    self.bot = ChatBot(self.helpers, self.instruction_prompt, model = self.model)

  def get_input(self, model_input):
    import pandas as pd
    import numpy as np
    if isinstance(model_input, pd.DataFrame):
      print("dataframe")
      input_list = model_input.to_dict(orient='records')
    elif isinstance(model_input, np.ndarray):
      print("array")
      input_list = model_input[:, 0].tolist()
    elif isinstance(model_input, str):
      print("string")
      input_list = [model_input]
    else: input_list = model_input

    print(input_list)
    return input_list[0]
  
  def predict(self, context, model_input, params):
    input_fields = self.get_input(model_input)
    #return messages

    if isinstance(input_fields, str):
      input_fields = eval(input_fields)

    if input_fields.get('feedback', False):
      return {}

    response = self.bot.run_thread(input_fields['messages'])

    output = {
      'run_id': self.bot.conversation_id,
      'choices': [
        {
          'index': 0,
          'message': {
            'role': 'assistant',
            'content': response
          }
        }
      ],
      'thread': self.bot.output_thread()
    }

    return output
  

#helper functions

def register_model(run_id, model_name, model_alias = 'prod'):
  #model_name = f"robert_mosley.sql_ai.rag_agent"

  from mlflow import MlflowClient
  mlflow.set_registry_uri('databricks-uc')
  client = MlflowClient()

  latest_model = mlflow.register_model(f'runs:/{run_id}/model', model_name)
  client.set_registered_model_alias(name=model_name, alias=model_alias, version=latest_model.version)

  return latest_model

  
def log_model(python_model, prefix, sample_message = 'Hello.', pip_req = [], agent_studio_path='.'):
  import pandas as pd
  import datetime
  import mlflow
  from mlflow.models import infer_signature


  with mlflow.start_run(run_name=f'{prefix}_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}') as run:

    sample_input = [{
      "messages":
        [
          {'role':'user', 'content': sample_message}
        ], 
      "feedback": 
        {
          "run_id": "123ABC", 
          "score":8, 
          "thumbs_up": True,
          "correction":"This is the corrected output.",
          "comment":"This is feedback."
        }
    },
    {
        "messages":[
          {'role':'user', 
            'content': '', 
            "tool_calls": [
              {
                "id": "call_fqpuqimveokh",
                "type": "function",
                "function": {
                  "name": "helper_0--search_for_api",
                  "arguments": "{\"query_text\": \"start job\"}"
                }
              }
            ],
            "tool_call_id": "call_fqpuqimveokh",
            "name": "helper_0--search_for_api",
           }
        ]
    },
    {"messages":[]},
    {
        "feedback": 
        {
          "run_id": "123ABC" 
        }
    }]
    
    sample_output = [{
        'run_id': 'ABC123',
        'choices': [
          {
            'index': 0,
            'message': {
              'role': 'assistant',
              'content': 'response'
            }
          }
        ],
        'thread': [{'role':'user', 'content':'prompt'}], 
      },
      {}]
    
    inference_config = {"max_new_tokens": 1000, "temperature": .8}
    
    signature = infer_signature(sample_input, sample_output)#, params=inference_config)
    #input_example={"inputs": [{'messages':[{'role':'user', 'content':'prompt'}]}]}
    #input_example=pd.DataFrame({"inputs": [sample_input]})

    #'mlflow[databricks]',
    pip_requirements=[
      'mlflow',
      'langchain',
      'langchain-core',
      'pydantic',
      'openai',
      'langchain-openai',
      'langchain-community',
      'uuid',
      'psutil',
      'databricks-sql-connector',
      'databricks-vectorsearch'
      ]
    
    pip_requirements.extend(pip_req)

    pip_requirements = list(set(pip_requirements))

    mlflow.pyfunc.log_model(
      "model", 
      python_model=python_model,
      code_path = [os.path.join(agent_studio_path, 'Core/'), os.path.join(agent_studio_path, 'Tool/')],
      signature = signature,
      input_example=sample_input[0],
      metadata={"task": "llm/v1/chat"},
      pip_requirements = pip_requirements
      )
    
    print(run.info.run_id)

    return run.info.run_id
  
def create_endpoint(endpoint_name, model, config = {}):
  import requests, json, datetime

  model_name = config.get('model_name', None)
  if not model_name:
    model_name = model.name

  model_version = config.get('model_version', None)
  if not model_version:
    model_version = model.version

  endpoint_config = {
    "name": endpoint_name, 
    "config": 
      {
        "served_models": [{
                "model_name": model_name,
                "model_version": model_version,
                "workload_size": config.get('workload_size', "Small"),
                "workload_type": config.get('workload_type', 'CPU'),
                "scale_to_zero_enabled": config.get('scale_to_zero_enabled', "True"),
                "environment_vars": config.get('environment_vars', {})
          }],
        "auto_capture_config": {
                "catalog_name": model_name.split('.')[0],
                "schema_name": model_name.split('.')[1],
                "table_name_prefix": model_name.split('.')[2],
                "enabled": True
          }
        },
      "tags": [
        {
          "key": "agent_studio"
        },
        {
          "key": "removeAfter",
          "value": (datetime.datetime.now() + datetime.timedelta(days=60)).strftime("%Y-%m-%d") #"2024-03-01"
        }
      ]
      }

  print(json.dumps(endpoint_config))

  workspace_url = os.environ['DATABRICKS_HOST']
  databricks_token = os.environ['DATABRICKS_TOKEN']
  endpoints_host = f"{workspace_url}/api/2.0/serving-endpoints"

  headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
        }

  resp = requests.post(endpoints_host, headers=headers, data=json.dumps(endpoint_config))
  # resp.raise_for_status()

  resp_json = resp.json()


  if resp_json.get('error_code', '') == 'RESOURCE_ALREADY_EXISTS':
    #process config update
    resp = requests.put(f'{endpoints_host}/{endpoint_name}/config', headers=headers, data=json.dumps(endpoint_config['config']))
    resp_json = resp.json()
    
  print(resp_json)
  return resp_json

      