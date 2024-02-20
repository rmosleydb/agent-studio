#pip install databricks-vectorsearch

import requests
import json
import random
import string
import time
import os

from databricks.vector_search.client import VectorSearchClient
import pandas as pd
import os

class Executor():
  def __init__(self):
    """Tool that enables execution of any Databricks API endpoint."""
    self.db_api_token = os.getenv("DATABRICKS_TOKEN")
    self.databricks_instance = os.getenv("DATABRICKS_HOST")
    self.headers = {'Authorization': f'Bearer {self.db_api_token}', 'Content-Type': 'application/json'}
  
  def execute_databricks_api_command(self, relative_url: str, http_method: str = "GET", json_payload: str = None):
    """Executes a command against a Databricks API Endpoint. 
    relative_url: relative url path to the api endpoint. example - '/api/2.0/pipelines'
    http_method: GET, POST, PUT, DELETE, PATCH, etc
    json_payload: the json payload in string form that will be submitted to the api endpoint"""

    payload = None
    if json_payload:
      try:
        payload = json.loads(json_payload)
      except:
        payload = eval(json_payload)
    
    response = requests.request(http_method.upper(),f'{self.databricks_instance}{relative_url}', headers = self.headers, json = payload)

    return response.text


class Search():

    def __init__(self, vector_search_endpoint_name: str, index_name: str):
        """Provide the Vector Search Index of API documentation to enable searches for endpoint documenation."""
        self.db_api_token = os.getenv("DATABRICKS_TOKEN")
        self.databricks_instance = os.getenv("DATABRICKS_HOST")
        self.vector_search_endpoint_name = vector_search_endpoint_name
        self.index_name = index_name

        self.vsc = VectorSearchClient(
            workspace_url=self.databricks_instance, 
            personal_access_token=self.db_api_token
        )

        self.vs_index = self.vsc.get_index(
            endpoint_name=vector_search_endpoint_name,
            index_name=index_name,
        )



    def search_for_api(self, query_text: str):
        """This function retrieves 10 databricks apis by performing a similarity search against a vector database of Databricks API Documentation. To get detailed documentation for a particular api endpoint, use retrieve_api_doc.
        """

        search = self.vs_index.similarity_search(
            query_text=query_text,
            columns=['api_title'],
            num_results = 10
        )

        dic = self.__vs_to_dict(search, True)

        return str(dic)
    
    def __vs_to_dict(self, search_results, drop_score = False):
        manifest = search_results['manifest']
        result = search_results['result']

        cols = [c['name'] for c in manifest['columns']]

        df = pd.DataFrame(result.get('data_array', []), columns=cols)
        if drop_score:
            df = df.drop(columns='score')
        dic = df.to_dict(orient='records')
        return dic
    
    def __retrieve_api_doc(self, api_title: str):

        results = self.vs_index.similarity_search(
            query_text=api_title,
            columns=['api_url', 'http_method', 'api_title', 'api_description', 'api_details', 'api_examples'],
            num_results = 3,
            filters = {"api_title": api_title }
        )

        dic = self.__vs_to_dict(results, True)

        return dic
    
    def retrieve_api_doc(self, api_title: str):
        """This function retrieves the full documentation for the databricks api with the specified title. If the exact title isn't specified, an empty dataset will be returned - [].
        """

        return str(self.__retrieve_api_doc(api_title))
        