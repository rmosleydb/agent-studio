# Databricks notebook source
# MAGIC %pip install mlflow[databricks]

# COMMAND ----------

# DBTITLE 1,Install Requirements
# MAGIC %pip install langchain-openai langchain-community
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Objects for Agent Model
from Core.DatabricksModel import ChatModel, log_model, register_model, create_endpoint
from Tool.TextToSQL import UnityCatalog_Schema

# COMMAND ----------

# DBTITLE 1,Set Environment Variables
 
import os
os.environ['OPENAI_API_KEY']  = dbutils.secrets.get('agent_studio','open_ai')
os.environ['DATABRICKS_TOKEN']  = dbutils.secrets.get('agent_studio','databricks_token')
os.environ['DATABRICKS_HOST']  = dbutils.secrets.get('agent_studio','databricks_host')

# COMMAND ----------

# DBTITLE 1,Instantiate Agent Tools
tool_0 = UnityCatalog_Schema(warehouse_id = "475b94ddc7cd5211", catalog = "main", schema = "default")

helpers = [tool_0]

# COMMAND ----------

# DBTITLE 1,Create Agent Model
config = {"provider": "openai", "model": "4", "endpoint_type": "chat", "instruction_prompt": "You are a text to sql agent. Use the tools available to navigate available tables, querying them to answer the user's questions.", "log_directory": ""}

model = ChatModel(config, helpers)

# COMMAND ----------

# DBTITLE 1,Log Model Experiment
run_id = log_model(model, 'sample_agent', pip_req = [], agent_studio_path = "/Workspace/Repos/robert.mosley@databricks.com/agent-studio/")

# COMMAND ----------

# DBTITLE 1,Register Model
registered_model = register_model(run_id, "main.default.sample_agent", 'prod')

# COMMAND ----------

# DBTITLE 1,Create/Update Endpoint
endpoint_name = "sample_agent"

endpoint_config = {
  "environment_vars": {
    "OPENAI_API_KEY": "{{secrets/agent_studio/open_ai}}",
    "DATABRICKS_TOKEN": "{{secrets/agent_studio/databricks_token}}",
    "DATABRICKS_HOST": "{{secrets/agent_studio/databricks_host}}",
  }
}

response = create_endpoint(endpoint_name, registered_model, endpoint_config)
