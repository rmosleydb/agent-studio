import requests
import json
import random
import string
import time
import os

class PythonExecutor():
  def __init__(self, cluster_id: str, working_directory: str):
    """Run Python scripts against a Databricks Cluster. Specify the cluster id and a working directory to load scripts into."""
    self.db_api_token = os.getenv("DATABRICKS_TOKEN")
    self.databricks_instance = os.getenv("DATABRICKS_HOST")
    self.cluster_id = cluster_id
    self.working_directory = working_directory

  def __create_python_notebook(self, content: str):
    """Creates a Databricks notebook and returns the file path."""
    pool = string.ascii_letters
    file_name = ''.join(random.choices(pool, k=8))
    file_path = f'{self.working_directory}{file_name}'

    headers = {'Authorization': f'Bearer {self.db_api_token}', 'Content-Type': 'application/json'}
    payload = {
      "path": file_path,
      "format": "SOURCE",
      "language": "PYTHON",
      "content": self.__to_base64(content),
      "overwrite": False
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

  def __delete_python_notebook(self, file_path: str):
    """Deletes a Databricks notebook."""

    headers = {'Authorization': f'Bearer {self.db_api_token}', 'Content-Type': 'application/json'}
    payload = {
      "path": file_path
    }
    response = requests.post(f'{self.databricks_instance}/api/2.0/workspace/delete', headers=headers, json=payload)

    if response.status_code == 200:
        return 'File Deleted.'
    else:
        error = response.json()
        raise ValueError(f'Error submitting job: {error}')

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
      ]
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
      
  def run_python_script(self, script_content: str):
    """Runs a python script on a Databricks cluster."""
    file_path = self.__create_python_notebook(script_content)
    run_result = self.__run_notebook(file_path)
    self.__delete_python_notebook(file_path)
    return run_result