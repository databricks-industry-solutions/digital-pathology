# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/digital-pathology. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/digital-pathology.

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature exploration: Dimensionality reduction
# MAGIC In this notebook, we explore the structure of extracted features using [Uniform Manifold Approximation and Projection (UMAP)](https://umap-learn.readthedocs.io/en/latest/) method.
# MAGIC To learn more visit: https://github.com/lmcinnes/umap
# MAGIC
# MAGIC In this notebook we use UMAP embeddings to visually inspect the extracted features from our generated patches which are stored in deltalake and examine the correlation between clusters of patches and labels. This method, in conjunction with clustering methods such as k-means can be used to determine the label of unlabeled patches based on the cluster they belong to (assuming a subset of patches have associated annotations), and help discover new patterns in the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initial Configuration

# COMMAND ----------

# %pip install umap-learn umap-learn[plot] xarray
# %pip install pandas matplotlib datashader bokeh holoviews scikit-image colorcet 

# %pip install --upgrade pip

# Install xarray and other dependencies
# %pip install --force-reinstall -v "numpy==1.25.2"

# %pip install xarray==2023.12.0 scikit-learn>=0.22.0 importlib-metadata scipy>=1.4.1 pyspark>=3.1.2 numpy>=1.23.5 scipy>=1.4.1 --upgrade
# %pip install xarray==2023.12.0 scikit-learn>=0.22.0 importlib-metadata scipy>=1.4.1 pyspark>=3.1.2 scipy>=1.4.1 --upgrade

# Install umap-learn and related packages
# %pip install umap-learn>=0.5.6 umap-learn[plot]>=0.5.6

# # Install dask
# %pip install "dask[dataframe]" --upgrade 
# dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install umap-learn umap-learn[plot] xarray
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pkg_resources
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Get the list of installed packages
installed_packages = pkg_resources.working_set
packages_list = sorted(["%s==%s" % (i.key, i.version) for i in installed_packages])

# Define the schema
schema = StructType([StructField("package", StringType(), True)])

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Create DataFrame with the defined schema
df = spark.createDataFrame([(pkg,) for pkg in packages_list], schema)

# Display the DataFrame
display(df)

# COMMAND ----------

import json
import os
from pprint import pprint

project_name='digital-pathology' #original
project_name2use = f"{project_name}".replace('-','_') ## for UC
user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
user_uid = abs(hash(user)) % (10 ** 5)
# config_path=f"/dbfs/FileStore/{user_uid}_{project_name}_configs.json"
config_path=f"/Volumes/mmt/{project_name2use}/files/{user_uid}_{project_name2use}_configs.json"


try:
  with open(config_path,'rb') as f:
    settings = json.load(f)
except FileNotFoundError:
  print('please run ./config notebook and try again')
  assert False

# COMMAND ----------

import mlflow
WSI_PATH=settings['data_path']
BASE_PATH=settings['base_path']
ANNOTATION_PATH = BASE_PATH+"/annotations"
IMG_PATH = settings['img_path']
mlflow.set_experiment(settings['experiment_name'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Get data ready for clustering and visualization

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import os


import numpy as np
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Load extracted features from delta
# img_features_df=spark.read.load(f"{BASE_PATH}/delta/features")

img_features_df=spark.read.table(f"mmt.{project_name2use}.features")

# COMMAND ----------

img_features_df.count()

# COMMAND ----------

img_features_df.select(size('features')).limit(1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that Note that the output of Inception has dim 8\*8\*2048 which is equal to the dimensions of the last layer of InceptionV3.

# COMMAND ----------

img_features_pdf=img_features_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we reshape the feature matrix into an `m*n` matrix for UMAP (m=number of samples and n=number of features)

# COMMAND ----------

n=len(img_features_pdf.features[0])
m=img_features_pdf.shape[0]
print(n,m)

# COMMAND ----------

features_mat=np.concatenate(img_features_pdf.features.values,axis=0).reshape(m,n)
features_mat.shape

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Dimensionality Reduction Using UMAP 

# COMMAND ----------

dbutils.widgets.text(name='n_neighbors',defaultValue='15')
dbutils.widgets.text(name='min_dist',defaultValue='0.1')

# COMMAND ----------

import pandas
import matplotlib
import datashader
import bokeh
import holoviews
import skimage  # 'scikit-image' should be imported as 'skimage'
import colorcet

import umap
import umap.plot

# COMMAND ----------

n_neighbors=int(dbutils.widgets.get('n_neighbors'))
min_dist=float(dbutils.widgets.get('min_dist'))

# COMMAND ----------

def get_embeddings(features_mat,n_neighbors=n_neighbors,min_dist=min_dist,n_components=2):
  params ={'n_neighbors':n_neighbors,
               'min_dist':min_dist,
               'n_components':n_components,
          }
  mapper = umap.UMAP(**params).fit(features_mat)
  mlflow.end_run()
  for key,value in mapper.get_params().items():
    mlflow.log_param(key,value)
  return(mapper)

# COMMAND ----------

mpper_2d=get_embeddings(features_mat,n_components=2)

# COMMAND ----------

embeddings2d_df=pd.concat([pd.DataFrame(mpper_2d.embedding_,columns=['c1','c2']),img_features_pdf[['id','x_center','y_center','label','slide_id']]],axis=1)

# COMMAND ----------

embeddings2d_df

# COMMAND ----------

import plotly.express as px
fig = px.scatter(embeddings2d_df,x='c1',y='c2',color='label',hover_name='slide_id',width=1000,height=700)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC It appears from the above plot, that normal patches cluster into two classes (one at the right and the other on the left). Visual inspection and looking at the slide_id of each point, we notice that the cluster on the left is mainly formed by patches extracted from tumor slides (and the patch is a normal patch - no metastasis indicated). 

# COMMAND ----------

# MAGIC %md
# MAGIC for future reference we also log the plot as an mlflow artifact.

# COMMAND ----------

# DBTITLE 1,log the plot in mlflow
dbutils.fs.put('file:/umap2d.html',fig.to_html(),overwrite=True)
mlflow.log_artifact('/umap2d.html','umap2d-plot')

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can also take a look at the clusters in 3d by using a 3d encoding.

# COMMAND ----------

mpper_3d=get_embeddings(features_mat,n_components=3)

# COMMAND ----------

embeddings3d_df=pd.concat([pd.DataFrame(mpper_3d.embedding_,columns=['c1','c2','c3']),img_features_pdf[['id','x_center','y_center','label','slide_id']]],axis=1)

# COMMAND ----------

import plotly.express as px
fig = px.scatter_3d(embeddings3d_df,x='c1',y='c2',z='c3',color='label',hover_name='slide_id',width=1000,height=700)
fig.show()

# COMMAND ----------


