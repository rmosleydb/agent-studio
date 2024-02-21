# Databricks notebook source
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
workspaceUrl = spark.conf.get('spark.databricks.workspaceUrl')

# COMMAND ----------

import requests

headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}

payload = {
      "scope": 'agent_studio',
      'scope_bakend_type': "DATABRICKS"
    }


response = requests.post(f'https://{workspaceUrl}/api/2.0/secrets/scopes/create', headers=headers, json=payload)
response = requests.get(f'https://{workspaceUrl}/api/2.0/secrets/list?scope=agent_studio', headers=headers)

print(response.json())

# COMMAND ----------

import requests

headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}

payload = {
      "scope": 'agent_studio',
      "key": "open_ai",
      "string_value": "" #open ai key
}
      


response = requests.post(f'https://{workspaceUrl}/api/2.0/secrets/put', headers=headers, json=payload)


print(response.json())

# COMMAND ----------

import requests

headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}

payload = {
      "scope": 'agent_studio',
      "key": "databricks_token",
      "string_value": ""
}
      


response = requests.post(f'https://{workspaceUrl}/api/2.0/secrets/put', headers=headers, json=payload)


print(response.json())

# COMMAND ----------

import requests

headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}

payload = {
      "scope": 'agent_studio',
      "key": "databricks_host",
      "string_value": f"https://{workspaceUrl}"
}
      


response = requests.post(f'https://{workspaceUrl}/api/2.0/secrets/put', headers=headers, json=payload)


print(response.json())

# COMMAND ----------

import requests

headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}

payload = {
      "scope": 'agent_studio',
      "key": "folder_path",
      "string_value": "/Workspace/Repos/robert.mosley@databricks.com/agent-studio/" #folder path to agent studio
}
      


response = requests.post(f'https://{workspaceUrl}/api/2.0/secrets/put', headers=headers, json=payload)


print(response.json())
