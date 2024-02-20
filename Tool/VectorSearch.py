#pip install databricks-vectorsearch

from databricks.vector_search.client import VectorSearchClient
import pandas as pd
import os

class Retriever():
    def __init__(self, vector_search_endpoint_name: str, index_name: str, return_columns: list[str], function_descriptor: str = ""):
        """Tool that searches any vector database. Use the Function Descriptor to add tool details for the LLM using it."""
        self.db_api_token = os.getenv("DATABRICKS_TOKEN")
        self.databricks_instance = os.getenv("DATABRICKS_HOST")
        self.vector_search_endpoint_name = vector_search_endpoint_name
        self.index_name = index_name
        self.return_columns = return_columns

        self.vsc = VectorSearchClient(
            workspace_url=self.databricks_instance, 
            personal_access_token=self.db_api_token
        )

        self.vs_index = self.vsc.get_index(
            endpoint_name=vector_search_endpoint_name,
            index_name=index_name,
        )

        self.function_descriptor = f"{function_descriptor} - Columns available for filtering are {str(return_columns)}."



    def retrieve_documents(self, query_text: str, result_count: str = 3, filter_str: str = None):
        """This function retrieves a set of documents that match the query_text (similarity search). By default, 3 results are returned. Optionally provide a filter to screen the results based on the columns available. Provide the filter in string form (It will be converted to a dict).
        
        Sample filters: (the provided column names are not necessarily relevant to this function.)
        {"id": 20 } - searches records with an id column of 20
        {"DEPT": ["OPS", "HR"]} - searches records with a DEPT column of value OPS or HR
        {"DEPT NOT": ["OPS", "HR"]} - searches record with a DEPT column having any value except OPS or HR.
        """
        if filter_str:
            results = self.vs_index.similarity_search(
                query_text=query_text,
                columns=self.return_columns,
                num_results = result_count,
                filters = eval(filter_str)
                )
        else:
            results = self.vs_index.similarity_search(
                query_text=query_text,
                columns=self.return_columns,
                num_results = result_count
            )
        return str(results)