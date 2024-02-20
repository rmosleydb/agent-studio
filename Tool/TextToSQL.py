import requests
import json
import random
import string
import time
import os

class UnityCatalog_Schema():
  def __init__(self, warehouse_id: str, catalog: str, schema: str):
    """Specify a UC Schema and navigate UC metadata to generate Text to SQL."""
    self.db_api_token = os.getenv("DATABRICKS_TOKEN")
    self.databricks_instance = os.getenv("DATABRICKS_HOST")
    self.warehouse_id = warehouse_id
    self.catalog = catalog
    self.schema = schema
  
  def list_table(self):
    """Returns a list of all available tables."""

    sql = f"""
          select table_name, comment 
          from system.information_schema.tables
          where table_catalog = '{self.catalog}' and table_schema = '{self.schema}'
          """
    return self.run_sql_statement(sql, 100)
  
  def table_definition(self, tables: str):
    """Receives a list of comma separated table names and returns table definitions for the tables."""

    tables_arr = tables.replace(' ', '').split(',')
    tables = "','".join(tables_arr)

    table_schemas = f"""
        with constraints as (
          select k.*, cs.constraint_type, u.table_catalog referential_table_catalog, u.table_schema referential_table_schema, u.table_name referential_table_name
          from system.information_schema.key_column_usage k
            inner join system.information_schema.table_constraints cs on k.constraint_catalog = cs.constraint_catalog and k.constraint_schema = cs.constraint_schema and k.constraint_name = cs.constraint_name
            left outer join (select distinct constraint_catalog, constraint_schema, constraint_name, table_catalog, table_schema, table_name from system.information_schema.constraint_column_usage) u on k.constraint_catalog = u.constraint_catalog and k.constraint_schema = u.constraint_schema and k.constraint_name = u.constraint_name and cs.constraint_type = 'FOREIGN KEY'
          where k.table_catalog = '{self.catalog}' and k.table_schema = '{self.schema}' and k.table_name in ('{tables}')
        )
        select c.* except(table_catalog, table_schema, character_octet_length, numeric_precision,	numeric_precision_radix,	numeric_scale,	datetime_precision,	interval_type,	interval_precision, identity_start,	identity_increment,	identity_maximum,	identity_minimum,	identity_cycle, is_system_time_period_start,	is_system_time_period_end,	system_time_period_timestamp_generation,	is_updatable)
          , cs.constraint_name
          , cs.ordinal_position constraint_ordinal_position
          , cs.constraint_type
          , cs.referential_table_catalog
          , cs.referential_table_schema
          , cs.referential_table_name
        from system.information_schema.columns c 
          left outer join constraints cs on c.table_catalog = cs.table_catalog and c.table_schema = cs.table_schema and c.table_name = cs.table_name and c.column_name = cs.column_name
        where c.table_catalog = '{self.catalog}' and c.table_schema = '{self.schema}'and c.table_name in ('{tables}')
        order by table_name, ordinal_position;"""
    
    return self.run_sql_statement(table_schemas, 1000)
    
    
  
  def sample_table_records(self, tables: str, limit: int = 3):
    """Receives a list of comma separated table names and the number (limit) of records (10 maximum) for each table and returns sample records for each table."""

    tables_arr = tables.replace(' ', '').split(',')

    ret = ""
    for tbl in tables_arr:
      sql = f'select * from {self.catalog}.{self.schema}.{tbl}'

      ret += f'Sample records for {table}: \n'
      ret += self.run_sql_statement(sql, min(10,limit)) + "\n\n"
      
    return ret

  def run_sql_statement(self, sql: str, row_limit: int=10):
    """Receives SQL, runs it, and returns the result. To limit the number of records returned, use the row_limit parameter. Do not limit it in the sql. """
    
    headers = {'Authorization': f'Bearer {self.db_api_token}', 'Content-Type': 'application/json'}
    payload = {
      "warehouse_id": self.warehouse_id,
      "statement": sql,
      "wait_timeout": "30s",
      "on_wait_timeout": "CANCEL",
      "catalog": self.catalog,
      "schema": self.schema,
      "row_limit": row_limit
    }

    response = requests.post(f'{self.databricks_instance}/api/2.0/sql/statements/', headers=headers, json=payload)

    if response.status_code == 200:
        result = response.json()
        #print(result)
        columns = result['manifest']['schema']['columns']
        rows = result['result']['data_array']

        #return results in pipe delimited format
        col_names = [col['name'] for col in columns]
        ret = '|'.join(col_names) + '\n'
        for row in rows:
          ret += '|'.join(str(cell) for cell in row) + '\n'

        return ret

    else:
        error = response.json()['message']
        raise ValueError(f'Error executing SQL statement: {error}')