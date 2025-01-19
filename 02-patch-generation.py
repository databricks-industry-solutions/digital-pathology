# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/digital-pathology. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/digital-pathology.

# COMMAND ----------

# DBTITLE 1,[RUNME clusters config specifies cluster lib]
## uncomment below to run this nb separately from RUNME nb
# %pip install openslide-python
# dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,cluster init file: openslide-tools.sh would install this
# !apt-get install -y openslide-tools

# COMMAND ----------

# MAGIC %md
# MAGIC # Distributed patch generation
# MAGIC In this notebook we use spark's `pandas_udfs` to efficiently distribute patch generation process. Now that we have annotations and meta-data stored in delta, we can distribute patch generation using `pandas_udf`s in spark. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initial Configuration

# COMMAND ----------

# %run ./config/0-config $project_name=digital_pathology $overwrite_old_patches=yes $max_n_patches=2000

# COMMAND ----------

import json
import os
from pprint import pprint

catalog_name = 'dbdemos'
project_name='digital_pathology' 
# project_name2use = f"{project_name}".replace('-','_') #for UC
user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
user_uid = abs(hash(user)) % (10 ** 5)

# config_path=f"/dbfs/FileStore/{user_uid}_{project_name}_configs.json"
# config_path=f"/Volumes/mmt/{project_name2use}/files/{user_uid}_{project_name2use}_configs.json"
config_path=f"/Volumes/{catalog_name}/{project_name}/files/{user_uid}_{project_name}_configs.json"

try:
  with open(config_path,'rb') as f:
    settings = json.load(f)
except FileNotFoundError:
  print('please run ./config notebook and try again')
  assert False

# COMMAND ----------

# DBTITLE 1,set paths
WSI_PATH=settings['data_path']
BASE_PATH=settings['base_path']
IMG_PATH = settings['img_path']
ANNOTATION_PATH = BASE_PATH+"/annotations"

# COMMAND ----------

# DBTITLE 1,define parameters
PATCH_SIZE=settings['patch_size']
LEVEL=settings['level']
MAX_N_PATCHES=settings['max_n_patches'] 

# We set this value to limit the number of patches generated. You can modify this to process more/less patches

# COMMAND ----------

# MAGIC %md
# MAGIC We have defined many functions used in the notebook and subsequent notebooks in `./definitions` notebook. By running this notebook these definitions are executed and available to be used here.

# COMMAND ----------

# MAGIC %run ./definitions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load pre-processed annotations and create train/test sets
# MAGIC Now we load annotations and also assign train/test labels to each patch for the downstream training.

# COMMAND ----------

# DBTITLE 1,Load annotations
from pyspark.sql.functions import *

## read from UC Volumes
# coordinates_df = spark.read.load(f'{ANNOTATION_PATH}/delta/patch_labels')

## read from UC table
# coordinates_df = spark.read.table(f"mmt.{project_name2use}.patch_labels")
coordinates_df = spark.read.table(f"{catalog_name}.{project_name}.patch_labels")

df_patch_info = (
  spark.createDataFrame(dbutils.fs.ls(WSI_PATH))
  .withColumn('sid',lower(regexp_replace('name', '.tif', '')))
  .join(coordinates_df, on='sid')
  .withColumn('train_test',when(rand()<0.3, 'test').otherwise('train'))
  .withColumn('rnd',rand())
  .orderBy('rnd')
  .limit(MAX_N_PATCHES)
  .drop('rnd')
  .cache()
)

# COMMAND ----------

print(f'there are {df_patch_info.count()} patches to process.')
display(df_patch_info)

# COMMAND ----------

# MAGIC %md
# MAGIC let's take a look at the label distribution to ensure we have a balanced dataset

# COMMAND ----------

df_patch_info.groupBy('train_test').avg('label').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create patches from WSI images
# MAGIC
# MAGIC In this step, we simply distribute the tiling process based on specified coordinates. This is achieved by applying `dist_patch_extract` defined in `PatchGenerator` class from the helper notebook, which leverages `pandas_udfs` to distribute patch extraction from openSlide

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1024")

# COMMAND ----------

PatchGenerator.__doc__, IMG_PATH

# COMMAND ----------

patch_generator=PatchGenerator(wsi_path=WSI_PATH,level=LEVEL,patch_size=PATCH_SIZE, img_path=IMG_PATH)

# COMMAND ----------

# DBTITLE 1,Create a dataframe of processed patches
dataset_df = (
  df_patch_info
  .repartition(64)
  .withColumn('img_path',concat_ws('/',lit(IMG_PATH),col('train_test'),col('label')))
  .mapInPandas(patch_generator.dist_patch_save, schema='label:integer, x_center: integer, y_center: integer, processed_img:string')
)

# COMMAND ----------

# MAGIC %md
# MAGIC Note that in the above command we simply created the spark execution plan and no action has been invoked yet. The following command will invoke and action which is to create a dataframe of extracted patches.  

# COMMAND ----------

# DBTITLE 1,calls the execution of mapinpandas
# display(dataset_df)

# COMMAND ----------

dataset_df.count()

# this typically takes between 10-25mins depending on your cluster config.

# COMMAND ----------

df_patch_info.repartition(64).withColumn('img_path',concat_ws('/',lit(IMG_PATH),col('train_test'),col('label'))).display()

# COMMAND ----------

# DBTITLE 1,display the images using the path info
spark.read.format('binaryFile').load(f'{IMG_PATH}/*/*/*.jpg').withColumn('sid',regexp_extract('path','(\\w+)_(\\d+)', 0)).display()
