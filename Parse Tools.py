# Databricks notebook source
import os
studio_dir = dbutils.secrets.get('agent_studio','folder_path')
tool_dir = os.path.join(studio_dir, 'Tool/')
yaml_dir = os.path.join(tool_dir, 'yaml/')

# COMMAND ----------

def getPips(file_path):

  with open(file_path, 'r') as file:
      file_contents = file.read()

  lines = file_contents.split('\n')
  pip_lines = [line for line in lines if line.startswith('#pip')]

  pip_items = []
  for line in pip_lines:
    pip_items.extend(line.split(' '))

  pip_items = list(set(pip_items))
  pip_items = [item for item in pip_items if item != 'install' and not item.startswith('#') and not item.startswith('-')]

  return pip_items

# COMMAND ----------

files = os.listdir(tool_dir)

pip = []

for file in files:
    file_path = os.path.join(tool_dir, file)
    if os.path.isfile(file_path):
      pip.extend(getPips(file_path))

pip = ' '.join(list(set(pip)))
print(pip)

# COMMAND ----------

# MAGIC %pip install {pip}
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
studio_dir = dbutils.secrets.get('agent_studio','folder_path')
tool_dir = os.path.join(studio_dir, 'Tool/')
yaml_dir = os.path.join(tool_dir, 'yaml/')

# COMMAND ----------

def getContents(file_path):

  with open(file_path, 'r') as file:
      file_contents = file.read()

  return file_contents


def getPips(file_path):

  with open(file_path, 'r') as file:
      file_contents = file.read()

  lines = file_contents.split('\n')
  pip_lines = [line for line in lines if line.startswith('#pip')]

  pip_items = []
  for line in pip_lines:
    pip_items.extend(line.split(' '))

  pip_items = list(set(pip_items))
  pip_items = [item for item in pip_items if item != 'install' and not item.startswith('#') and not item.startswith('-')]

  return pip_items

# COMMAND ----------

files = os.listdir(tool_dir)

tools = []

for file in files:
    file_path = os.path.join(tool_dir, file)
    if os.path.isfile(file_path):
      tools.append({
        "path": file_path,
        "contents": getContents(file_path),
        "pip_requirements": getPips(file_path)
      })

# COMMAND ----------

import pandas as pd
spark_df = spark.createDataFrame(tools)

# COMMAND ----------

import json, requests, base64
from pyspark.sql.functions import udf, col, split, element_at, size, array_except, array, explode, concat, lit, trim, filter, concat_ws, contains, length, startswith


df_module = (spark_df
        .withColumn("path_split", split(col("path"), "/"))
        .withColumn("folder", element_at(col("path_split"),-2))
        .withColumn("file", element_at(col("path_split"),-1))
        .withColumn("module", split(col("file"), "\.")[0])
        .drop('path_split')
        )
display(df_module)

# COMMAND ----------

df_tools = (df_module
            .withColumn("array", split(col("contents"), "class"))
            .withColumn("imports", array(col("array")[0]))
            .withColumn("import_lines", split(col("imports")[0], "\n"))
            .withColumn("classes", array_except(col("array"), col("imports")))
            .withColumn("class_count", size(col("classes")))
            .where("class_count > 0")
            .drop(col("array"))
            .withColumn("class", explode(col("classes")))
            .withColumn("class_name", trim(split(col("class"), "\(")[0]))
            .withColumn("class_code", concat(col("imports")[0], lit("\nclass"), col("class")))
            .withColumn("tool", concat(col("module"), lit("."), col('class_name')))
            )
df_tools = df_tools.select("tool", "folder", 'file', 'module', "path", 'class_name', "class_code", "pip_requirements")
display(df_tools)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, struct
from pyspark.sql.types import ArrayType, MapType, StringType, IntegerType
from inspect import signature, getdoc
import json

#@pandas_udf("string")
def get_documentation(script, class_name):
  try:
    exec(script)
    s = getdoc(eval(f"{class_name}.__init__"))
    return s
  except Exception as e:
    print(class_name + ': ' + str(e))
    return "Failed to Parse."

#@pandas_udf("string")
def get_parameters(script, class_name):
  try:
    exec(script)
    s = signature(eval(f"{class_name}.__init__"))
  
    params = []
    for idx, pr in enumerate(s.parameters):
    #for pr in s.parameters:
      p = s.parameters[pr]
      if p.name != "self":
        params.append( {
          "name": p.name,
          "index": idx,
          "default": p.default if p.default != p.empty else None,
          "annotation": p.annotation.__name__ if p.annotation != p.empty else None,
          "kind": p.kind.description
        })
    return params
  except Exception as e:
    print(class_name + ': ' + str(e))
    return [{}]


# COMMAND ----------

tools = df_tools.toPandas().to_dict(orient="records")

# COMMAND ----------

for tool in tools:
  tool['documentation'] = get_documentation(tool['class_code'], tool['class_name'])
  tool['parameters'] = get_parameters(tool['class_code'], tool['class_name'])
  tool['pip_requirements'] = tool['pip_requirements'].tolist()

# COMMAND ----------

from pyspark.sql import Row
df_param = spark.createDataFrame(Row(**x) for x in tools)
display(df_param)

# COMMAND ----------

df_final = df_param.select("tool", "module", "class_name", "documentation", "path", "folder", "file", "pip_requirements", "parameters")

# COMMAND ----------

tools = []
for tool in df_final.collect():
  td = tool.asDict()
  tools.append(td)

# COMMAND ----------

print(tools)

# COMMAND ----------

import yaml

#tools = df_final.toPandas().to_dict(orient='records')

for tool in tools:
    file_name = tool['tool'].replace('.', '__')
    #output_dir = "/Volumes/robert_mosley/sql_ai/files/agent_studio/tool/"
    output_path = os.path.join(yaml_dir, file_name + '.yaml')

    # Create the directory if it doesn't exist
    os.makedirs(yaml_dir, exist_ok=True)
    
    with open(output_path, 'w') as file:
        yaml.dump(tool, file, sort_keys=False, indent=4, width=2147483647)
        
    print(f"Dictionary written to YAML file: {output_path}")
