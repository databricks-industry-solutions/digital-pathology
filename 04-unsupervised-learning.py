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
# MAGIC ## 0. Set & Retrieve Configuration

# COMMAND ----------

# DBTITLE 1,Install required dependencies for Umap
# Read the requirements file
with open('unsupervisedLearning_requirements.txt', 'r') as file:
    packages = file.readlines()

# Install each package individually
for package in packages:
    package = package.strip()
    if package:
        try:
            %pip install {package}
        except Exception as e:
            print(f"Failed to install {package}: {e}")

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Retrieve Configs
import json
import os
from pprint import pprint

# Read User Specified RUNME_Config -- this will contain default values if .json not updated
with open("./config/runme_config.json", "r") as f:
  config = json.load(f)
  
catalog_name = config["catalog_name"]
project_name = config["schema_name"] #same as "schema_name"

print(f"catalog_name: {catalog_name}")
print(f"project_name: {project_name}")

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

# DBTITLE 1,Extract Paths from Configs & define MLflow settings
import mlflow
# Set the registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

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

# DBTITLE 1,Load extracted features from UC delta table
img_features_df=spark.read.table(f"{catalog_name}.{project_name}.features")

# COMMAND ----------

# DBTITLE 1,check row count of img_features_df
img_features_df.distinct().count()

# COMMAND ----------

# DBTITLE 1,check size of column feature
img_features_df.select(size('features')).limit(1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that Note that the output of Inception has dim 8\*8\*2048 which is equal to the dimensions of the last layer of InceptionV3.

# COMMAND ----------

# DBTITLE 1,convert img_features_df to pandasDF
img_features_pdf=img_features_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we reshape the feature matrix into an `m*n` matrix for UMAP (m=number of samples and n=number of features)

# COMMAND ----------

# DBTITLE 1,get m_samples and n_features
n=len(img_features_pdf.features[0])
m=img_features_pdf.shape[0]
print(n,m)

# COMMAND ----------

# DBTITLE 1,reshape
features_mat=np.concatenate(img_features_pdf.features.values,axis=0).reshape(m,n)
features_mat.shape

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Dimensionality Reduction Using UMAP 

# COMMAND ----------

# DBTITLE 1,Define Umap parameters - for interactive exploration
dbutils.widgets.text(name='n_neighbors',defaultValue='15')
dbutils.widgets.text(name='min_dist',defaultValue='0.1')

# COMMAND ----------

# DBTITLE 1,Import Umap Dependencies
import pandas
import matplotlib
import datashader
import bokeh
import holoviews
import skimage  # 'scikit-image' should be imported as 'skimage'
import colorcet

import umap

# COMMAND ----------

# DBTITLE 1,Set Umap Parameters
n_neighbors=int(dbutils.widgets.get('n_neighbors'))
min_dist=float(dbutils.widgets.get('min_dist'))

# COMMAND ----------

# DBTITLE 1,Define Umap get_embeddings function
def get_embeddings(features_mat, n_neighbors, min_dist, n_components=2, run_name=None):
    params = {
        'n_neighbors': n_neighbors,
        'min_dist': min_dist,
        'n_components': n_components,
    }
    mapper = umap.UMAP(**params).fit(features_mat)

    if run_name:
        with mlflow.start_run(run_name=run_name):
            for key, value in mapper.get_params().items():
                mlflow.log_param(key, value)
    else:
        for key, value in mapper.get_params().items():
            mlflow.log_param(key, value)
    
    return mapper

# COMMAND ----------

# DBTITLE 1,If cuda is used
import numba
import torch

# Check if GPU is available & set Threading Building Blocks (TBB) library to "workqueue" for GPU
print("GPU available?", torch.cuda.is_available())
if torch.cuda.is_available():
    numba.config.THREADING_LAYER = 'workqueue' 
    print('Set TBB library to "workqueue"')

# COMMAND ----------

# DBTITLE 1,Process 2D embeddings
mlflow.end_run()

mpper_2d = get_embeddings(features_mat, n_components=2, n_neighbors=n_neighbors, min_dist=min_dist, run_name="UMAP-2Dembedding")

# COMMAND ----------

# DBTITLE 1,Combine 2D embeddings + img_features pandasDF
embeddings2d_df=pd.concat([pd.DataFrame(mpper_2d.embedding_,columns=['c1','c2']),img_features_pdf[['id','x_center','y_center','label','slide_id']]],axis=1)

# COMMAND ----------

# DBTITLE 1,display
embeddings2d_df

# COMMAND ----------

# DBTITLE 1,Use Plotly for Viz
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

# DBTITLE 1,log the 2D plot in mlflow run + UC Volumes
from mlflow.tracking import MlflowClient

# Define the path to the Unity Catalog volume
uc_volume_path = f'/Volumes/{catalog_name}/{project_name}/files/umap2d.html'

# Save the plot to the UC volume
dbutils.fs.put(uc_volume_path, fig.to_html(), overwrite=True)

## Get the most recent run ID in with settings['experiment_name'] -- it should be for run_name: "UMAP-2Dembedding"
client = MlflowClient()
experiment_id = mlflow.get_experiment_by_name(settings['experiment_name']).experiment_id
runs = client.search_runs(experiment_id, order_by=["start_time desc"], max_results=1)
run_id = runs[0].info.run_id

## Log the artifact to the most recent run using MlflowClient
client.log_artifact(run_id, uc_volume_path, 'umap2d-plot')

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can also take a look at the clusters in 3d by using a 3d encoding.

# COMMAND ----------

# DBTITLE 1,Process 3D embeddings
mlflow.end_run()

mpper_3d = get_embeddings(features_mat, n_components=3, n_neighbors=n_neighbors, min_dist=min_dist, run_name="UMAP-3Dembedding")

# COMMAND ----------

# DBTITLE 1,Combine 3D embeddings + img_features pandasDF
embeddings3d_df=pd.concat([pd.DataFrame(mpper_3d.embedding_,columns=['c1','c2','c3']),img_features_pdf[['id','x_center','y_center','label','slide_id']]],axis=1)

# COMMAND ----------

# DBTITLE 1,Viz with Plotly
import plotly.express as px
fig = px.scatter_3d(embeddings3d_df,x='c1',y='c2',z='c3',color='label',hover_name='slide_id',width=1000,height=700)
fig.show()
