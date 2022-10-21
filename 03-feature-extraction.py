# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/digital-pathology. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/digital-pathology.

# COMMAND ----------

# MAGIC %md
# MAGIC # Distributed feature extraction
# MAGIC In this notebook we use spark's `pandas_udfs` to effiently distribute feature extraction process. The extaracted features are then can be used to visually inspect the structure of extracted patches.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://cloud.google.com/tpu/docs/images/inceptionv3onc--oview.png">
# MAGIC 
# MAGIC We use embeddings based on a pre-trained deep neural network (in this example, [InceptionV3](https://arxiv.org/abs/1512.00567)) to extract features from each patch.
# MAGIC Associated methods for feature extraction are defined within `./definitions` notebook in this package.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initial Configuration

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1024")

# COMMAND ----------

import json
import os
from pprint import pprint

project_name='digital-pathology'
user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
user_uid = abs(hash(user)) % (10 ** 5)
config_path=f"/dbfs/FileStore/{user_uid}_{project_name}_configs.json"

try:
  with open(config_path,'rb') as f:
    settings = json.load(f)
except FileNotFoundError:
  print('please run ./config notebook and try again')
  assert False

# COMMAND ----------

from pyspark.sql.functions import *

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

# COMMAND ----------

annotaion_df=spark.read.load(f'{ANNOTATION_PATH}/delta/patch_labels').withColumn('imid',concat_ws('-',col('sid'),col('x_center'),col('y_center')))
display(annotaion_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create a dataframe of processed patches
# MAGIC Now we create dataframe of processed patches with their associated annotations

# COMMAND ----------

# DBTITLE 1,Create a dataframe of images
patch_df= (
  spark.read.format('binaryFile').load(f'{IMG_PATH}/*/*/*.jpg').repartition(32)
  .withColumn('imid',regexp_extract('path','(\\w+)_(\\d+)-(\\d+)-(\\d+)', 0))
)

# COMMAND ----------

patch_df.display()

# COMMAND ----------

# DBTITLE 1,Create a dataframe of processed patches with metadata
dataset_df = (
  annotaion_df
  .join(patch_df,on='imid')
  .selectExpr('uuid() as id','sid as slide_id','x_center','y_center','label','content')
)

# COMMAND ----------

dataset_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract features from imgaes
# MAGIC Now that we have a dataframe of all patches, we pass each patch through the pre-trained model and use the networks output (embeddings) as features to be used for dimensionality reduction.

# COMMAND ----------

# MAGIC %run ./definitions

# COMMAND ----------

# MAGIC %md
# MAGIC Now, we simply apply the `featurize_raw_img_series_udf` function which is defined in `./definitions` notebook to extracted embeddings from each image in a distributed fashion, using `pandas_udf` functionality within Apache Spark.

# COMMAND ----------

features_df=dataset_df.select('*',featurize_raw_img_series_udf('content').alias('features'))

# COMMAND ----------

features_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC As you can see bellow, the resulting dataframe contains the content of each patch, as well as associated annotation and extracted features, all in one table.

# COMMAND ----------

features_df.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Cereate an image library in Delta
# MAGIC Now to persist our results, we write the resulting dataframe into deltalake for furture access.

# COMMAND ----------

features_df.write.format('delta').mode('overWrite').option("mergeSchema", "true").save(f"{BASE_PATH}/delta/features")

# COMMAND ----------

display(dbutils.fs.ls(f"{BASE_PATH}/delta/features"))
