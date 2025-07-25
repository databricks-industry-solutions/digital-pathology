# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/digital-pathology. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/digital-pathology.

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest annotation data to Unity Catalog
# MAGIC In this section we load pre-processed annotation files - tabular data containing slide name, `x`,`y` coordinates of the tile and corresponding label (`0` for no metastasis and `1` for metastasis).
# MAGIC We use pre-processed annotations from [BaiduResearch](https://github.com/baidu-research/NCRF). This repository, contains the coordinates of pre-sampled patches used in [the paper](https://openreview.net/forum?id=S1aY66iiM) which uses conditional random fields in conjunction with CNNs to achieve the highest accuracy for detecting metastasis on WSI images:
# MAGIC
# MAGIC Each one is a csv file, where each line within the file is in the format like Tumor_024,25417,127565 that the last two numbers are (x, y) coordinates of the center of each patch at level 0. `tumor_train.txt` and `normal_train.txt` contains 200,000 coordinates respectively, and `tumor_valid.txt` and `normal_valid.txt` contains 20,000 coordinates respectively. Note that, coordinates of hard negative patches, typically around tissue boundary regions, are also included within `normal_train.txt` and `normal_valid.txt`. With the original WSI and pre-sampled coordinates, we can now generate image patches for training deep CNN models.
# MAGIC
# MAGIC [see here](https://github.com/baidu-research/NCRF#patch-images) for more information.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initial Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC **IMPORTANT NOTE:**    
# MAGIC **Before proceeding, it is worth reviewing the `./config/0-config` and `./config/runme_config.json` files to check that the catalog, schema (project_name), and volume paths are set up as desired with a corresponding cluster. This is epecially important if you are running the notebook separately. `./config/runme_config.json` gets overwritten in `RUNME.py` when catalog and/or schema (project_name) are specified via widget updates cf. defaults.**

# COMMAND ----------

# DBTITLE 1,Run 0-config file to set configs
# MAGIC %run ./config/0-config $project_name=digital_pathology $overwrite_old_patches=yes $max_n_patches=2000

# COMMAND ----------

# DBTITLE 1,Check parameters extracted from 0-config.
catalog_name, project_name

# COMMAND ----------

# DBTITLE 1,Retrieve Configs
import json
import os
from pprint import pprint

user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
user_uid = abs(hash(user)) % (10 ** 5)  

config_path=f"/Volumes/{catalog_name}/{project_name}/files/{user_uid}_{project_name}_configs.json"

try:
  with open(config_path,'rb') as f:
    settings = json.load(f)
except FileNotFoundError:
  print('please run ./config notebook and try again')
  assert False

# COMMAND ----------

# DBTITLE 1,Set paths
WSI_PATH = settings['data_path']
BASE_PATH = settings['base_path']
IMG_PATH = settings['img_path']
ANNOTATION_PATH = BASE_PATH+"/annotations"

# COMMAND ----------

# DBTITLE 1,Copy init.sh file to UC Vol BASE_PATH
import os
import subprocess

# Define the source and destination paths
source_path = "openslide-tools.sh"
destination_path = f"{BASE_PATH}/openslide-tools.sh" #'/Volumes/dbdemos/digital_pathology/files/openslide-tools.sh'

# Ensure the destination directory exists
os.makedirs(os.path.dirname(destination_path), exist_ok=True)

# We check that the file was copied successfully by displaying the contents of the destination file
subprocess.run(["cat", destination_path], check=True)

# COMMAND ----------

# DBTITLE 1,reset paths (for cleaner demo run)
dbutils.fs.rm(f'{ANNOTATION_PATH}/', recurse=True) # would have to rerun 02_* notebooks

# alternatively; dbutils.fs.rm(f'{ANNOTATION_PATH}/delta/patch_labels/', recurse=True)

# COMMAND ----------

# DBTITLE 1,Check & Create UC Volumes paths for image files
for path in [BASE_PATH, ANNOTATION_PATH,f'{IMG_PATH}/train/1',f'{IMG_PATH}/test/1',f'{IMG_PATH}/train/0',f'{IMG_PATH}/test/0']:
  if not os.path.exists((f'dbfs:/{path}')): # to work with UC volumes
    print(f"path {path} does not exist")
    dbutils.fs.mkdirs(path)
    print(f"created path {path}")
  else:
    print(f"path {path} exists")
    
html_str=f"""<p>WSI_PATH={WSI_PATH}<br>BASE_PATH=<b>{BASE_PATH}</b><br>ANNOTATION_PATH=<b>{ANNOTATION_PATH}</b><br>IMG_PATH=<b>{IMG_PATH}</b></p>"""
displayHTML(html_str)

# COMMAND ----------

# DBTITLE 1,Review UC Volumes BASE_PATH
display(dbutils.fs.ls(BASE_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Download annotations

# COMMAND ----------

# DBTITLE 1,Load and write data to ANNOTATION PATH
SolAccUtil(project_name).load_remote_data('https://raw.githubusercontent.com/baidu-research/NCRF/master/coords/tumor_train.txt',ANNOTATION_PATH)
SolAccUtil(project_name).load_remote_data('https://raw.githubusercontent.com/baidu-research/NCRF/master/coords/normal_train.txt',ANNOTATION_PATH)
display(dbutils.fs.ls(ANNOTATION_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC let's take a look at the content of the file

# COMMAND ----------

# DBTITLE 1,Check file content
print(dbutils.fs.head(f'{ANNOTATION_PATH}/tumor_train.txt'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create annotation dataframes
# MAGIC Now we create a dataframe of tumor/normal coordinates based on the annotation data and write the result in delta tables to be used in the next stage for creating patches.

# COMMAND ----------

# DBTITLE 1,Import pyspark.sql functions and types
from pyspark.sql import functions as F, types as T

# COMMAND ----------

# DBTITLE 1,Set Schema
schema = (
  T.StructType()
  .add("sid",T.StringType(),False)
  .add('x_center',T.IntegerType(),True)
  .add('y_center',T.IntegerType(),True)
)

# COMMAND ----------

# DBTITLE 1,Read files from UC
# load tumor patch coordinates and assign label = 0
df_coords_normal = spark.read.csv(f'{ANNOTATION_PATH}/normal_train.txt', schema=schema).withColumn('label', F.lit(0))

# load tumor patch coordinates and assign label = 1
df_coords_tumor = spark.read.csv(f'{ANNOTATION_PATH}/tumor_train.txt',schema=schema).withColumn('label', F.lit(1))

# union patches together
df_coords = df_coords_normal.union(df_coords_tumor).selectExpr('lower(sid) as sid','x_center','y_center','label')
# display(df_coords)

# COMMAND ----------

# DBTITLE 1,Check sample image coordinates
display(df_coords.sample(fraction=0.005, seed=482, withReplacement=False))

# COMMAND ----------

# DBTITLE 1,Check data counts by label
display(df_coords. groupby('label').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write dataframes to Unity Catalog
# MAGIC Now we write the resulting dataframe to Unity Catalog as delta files/tables. Later we use this dataset to join annotaions with slides and genereated patches.

# COMMAND ----------

# DBTITLE 1,Write as delta files to ANNOTATION_PATH
df_coords.write.format('delta').mode('overWrite').save(f'{ANNOTATION_PATH}/delta/patch_labels')

# COMMAND ----------

# DBTITLE 1,Optimize delta files
sql(f'OPTIMIZE delta.`{ANNOTATION_PATH}/delta/patch_labels`')

# COMMAND ----------

# DBTITLE 1,check patch_labels dataset
display(dbutils.fs.ls(f'{ANNOTATION_PATH}/delta/patch_labels'))

# COMMAND ----------

# DBTITLE 1,Write data also as delta table to Catalog.Schema
## save as delta Table in UC as well 
df_coords.write.format('delta').mode('overWrite').option("mergeSchema", "true").saveAsTable(f"{BASE_PATH.removeprefix('/Volumes/').removesuffix('/files').replace('/','.')}.patch_labels")
