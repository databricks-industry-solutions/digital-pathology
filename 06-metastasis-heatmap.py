# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/digital-pathology. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/digital-pathology.

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a metastasis probability heatmap
# MAGIC In the previous step, we re-trained a resent model for our classification task and loged the model using ML flow. In this notebook, we load the classification model tarined in the previous step and use it to overlay a heatmap of metastasis probability over a new slide.
# MAGIC <br>
# MAGIC <img src="https://hls-eng-data-public.s3.amazonaws.com/img/slide_heatmap.png" alt="logo" width=60% /> 
# MAGIC </br>
# MAGIC To do so, we use the our distributed segmentation approach to create patches from a given slide to be scored, and then use the pre-trained model to infer the probability of metastasis on each segment. We then visualize the results as a heatmap. 
# MAGIC 
# MAGIC Note that, you need to have `openSlide` installed on the cluster to be able to generate pacthes (use the same cluster you used for patch generation).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initial Configuration

# COMMAND ----------

import json
import os
from pprint import pprint

os.chdir("/databricks/driver")
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

import io

import pandas as pd
import numpy as np
from PIL import Image

from pyspark.sql.functions import *
from pyspark.sql.types import *

import torch
from torch.utils.data import Dataset, DataLoader
from torchvision import models, transforms

import mlflow
import mlflow.pytorch

# COMMAND ----------

# DBTITLE 1,setup paths
IMG_PATH = settings['img_path']
WSI_PATH = settings['data_path']
mlflow.set_experiment(settings['experiment_name'])
TEMP_PATCH_PATH="/ml/tmp/hls"
PATCH_SIZE=settings['patch_size']
LEVEL=settings['level']
dbutils.fs.mkdirs(TEMP_PATCH_PATH)

# COMMAND ----------

cuda = False
use_cuda = cuda and torch.cuda.is_available()
device = torch.device("cuda" if use_cuda else "cpu")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Image segmentation
# MAGIC To visualize the heatmap of probability of metastasis on a segment of the slide, first we need to create a grid of patches and then for each patch we run prediction based on the model that we trained in the previous step. To do so we leverage patching and pre-processing functions that we used in the pre-procesing step for training the mode. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Grid generation
# MAGIC The following function, takes the `x`,`y` coordinates of the boundries of the segment of a given slide for scroing and outputs a dataframe containing coordinates of each segment (segments of size `299X299`) and `i,j` indices corresponding to the index of each segment within the grid.

# COMMAND ----------

def generate_patch_grid_df(*args):
  x_min,x_max,y_min,y_max, slide_name = args
  
  x = np.array(range(x_min,x_max,PATCH_SIZE))
  y = np.array(range(y_min,y_max,PATCH_SIZE))
  xv, yv = np.meshgrid(x, y, indexing='ij')
  
  Schema = StructType([
    StructField("x_center", IntegerType()),
    StructField("y_center", IntegerType()),
    StructField("i_j", StringType()),
  ])

  arr=[]
  for i in range(len(x)):
    for j in range(len(y)):
      x_center = int(xv[i,j].astype('int'))
      y_center = int(yv[i,j].astype('int'))
      arr+=[[x_center,y_center,"%d_%d"%(i,j)]]
  grid_size = xv.shape
  df = spark.createDataFrame(arr,schema=Schema) 
  return(df,grid_size)

# COMMAND ----------

# MAGIC %md
# MAGIC Next we generate patches based on a generated grid over a pre-selected segment of the slide.

# COMMAND ----------

name="tumor_058"
x_min,x_max,y_min,y_max = (23437,53337,135815,165715)

# COMMAND ----------

df_patch_info,grid_size = generate_patch_grid_df(x_min,x_max,y_min,y_max,name)
df_patch_info=df_patch_info.selectExpr(f"'{name}' as sid","x_center","y_center","i_j as label")
display(df_patch_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Patch generation
# MAGIC Now we apply patch pre-processing to the grid dataframe to generate a dataframe of features that will be fed to the pre-trained classifier for prediction.

# COMMAND ----------

# MAGIC %run ./definitions

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1024")

# COMMAND ----------

patch_generator=PatchGenerator(wsi_path=WSI_PATH,level=LEVEL,patch_size=PATCH_SIZE, img_path=IMG_PATH)

# COMMAND ----------

dataset_df = (
  df_patch_info
  .repartition(64)
  .withColumn('img_path',lit(TEMP_PATCH_PATH))
  .mapInPandas(patch_generator.dist_patch_save, schema='label:string, x_center: integer, y_center: integer, processed_img:string')
)

# COMMAND ----------

# MAGIC %md
# MAGIC Now by applying the `count` operation we evoke an action which results in patches being generated and written to the temporary location `TEMP_PATCH_PATH`

# COMMAND ----------

dataset_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inference
# MAGIC Now that we have the dataframe of segments, we simply load our classifer using the `uri` returned in the previous notebook and load the model. Next we use this model for prediction on the input spark dataframe in parallel.

# COMMAND ----------

cuda = False
use_cuda = cuda and torch.cuda.is_available()
device = torch.device("cuda" if use_cuda else "cpu")

# COMMAND ----------

# MAGIC %md
# MAGIC To estimate the probabilities, we first need to load the trained model in the previous step. We can use `mlflow.search_runs` function to lookup the best model (based on accuracy that is logged during the training step) and load the model for scoring:

# COMMAND ----------

best_run_id=mlflow.search_runs([settings['experiment_id']]).sort_values(by='metrics.best_accuracy',ascending=False)['run_id'].iloc[0]
model_name='resent-dp'

# COMMAND ----------

# DBTITLE 1,load the model
import mlflow
import mlflow.pytorch
MODEL_URI = f'runs:/{best_run_id}/{model_name}'
loaded_model = mlflow.pytorch.load_model(model_uri=MODEL_URI,map_location=torch.device('cpu'))

# COMMAND ----------

model_state = loaded_model.state_dict()
bc_model_state = sc.broadcast(model_state)

def get_model_for_eval():
  """Gets the broadcasted model."""
  # Load model as a Spark UDF.
  model = mlflow.pytorch.load_model(model_uri=MODEL_URI,map_location=torch.device('cpu'))
  model.load_state_dict(bc_model_state.value)
  model.eval()
  return model

@pandas_udf(ArrayType(FloatType()))
def predict_batch_udf(content_series: pd.Series) -> pd.Series:
  
  images = ImageNetDataset(list(content_series))
  loader = torch.utils.data.DataLoader(images, batch_size=500, num_workers=8)
  model = get_model_for_eval()
  model.to(device)
  all_predictions = []
  with torch.no_grad():
    for batch in loader:
      predictions = list(model(batch.to(device)).cpu().numpy())
      for prediction in predictions:
        all_predictions.append(prediction)
  return pd.Series(all_predictions)

# COMMAND ----------

# DBTITLE 1,load generated segments
images=spark.read.format('binaryFile').load(TEMP_PATCH_PATH).repartition(64)

# COMMAND ----------

images.count()

# COMMAND ----------

# DBTITLE 1,create predictions dataframe
predictions_df = images.select(col('path'), predict_batch_udf(col('content')).alias("prediction")).cache()

# COMMAND ----------

# DBTITLE 1,create pandas dataframe for scoring
predictions_pdf=(
  predictions_df
  .select('path',col('prediction')[1].alias('p'),regexp_extract('path','(\\w+)_(\\d+)-(\\d+)-(\\d+)', 0).alias('img_name'))
  .withColumn('sid',regexp_extract('img_name','(\\w+_\\d++)-(\\d+)-(\\d+)',1))
  .withColumn('x_center',regexp_extract('img_name','(\\w+_\\d++)-(\\d+)-(\\d+)',2).cast(IntegerType()))
  .withColumn('y_center',regexp_extract('img_name','(\\w+_\\d++)-(\\d+)-(\\d+)',3).cast(IntegerType()))
  .join(df_patch_info,on=['sid','x_center','y_center'])
  .toPandas()
)

# COMMAND ----------

predictions_pdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create metastasis heatmap
# MAGIC Now that we have the probability scores for each segment along with the indices of each segment on the grid, we can create a simple heatmap of probabiliy scores.

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC Here is how the original slide and the selected segment look like:

# COMMAND ----------

# DBTITLE 1,selected slide segment
pid="tumor_058"
slide = openslide.OpenSlide('/dbfs/%s/%s.tif' %(WSI_PATH,pid))
region= [x_min,y_min]

size=[2900,2900]
slide_segment= slide.read_region(region,3,size)

f, axarr = plt.subplots(1,2)
axarr[0].imshow(slide_segment)
axarr[0].set_xlim=3000
axarr[0].set_ylim=3000
axarr[1].imshow(slide.get_thumbnail(np.array(slide.dimensions)//50))
axarr[1].axis('off')
f.set_figheight(12)
f.set_figwidth(12)
display()

# COMMAND ----------

# MAGIC %md
# MAGIC And here is the heatmap of probability scores:

# COMMAND ----------

# DBTITLE 1,metastatic heatmap
x_min,x_max=predictions_pdf['x_center'].min(),predictions_pdf['x_center'].max()
y_min,y_max=predictions_pdf['y_center'].min(),predictions_pdf['y_center'].max()
pred_arr=predictions_pdf[['label','p']]
n_x,n_y=grid_size
width,height=299,299
scale_f=0.2

img_size = int(scale_f*width),int(scale_f*height)
total_width = img_size[0]*n_x
total_height = img_size[1]*n_y

y, x = np.meshgrid(np.linspace(x_min, x_max, n_x), np.linspace(y_min, y_max, n_y))
z=np.zeros(y.shape)

for ij,p in pred_arr.values:
      i = int(ij.split('_')[0])
      j = int(ij.split('_')[1])
      z[i][j]=p

z = z[:-1, :-1]
z_min, z_max = -np.abs(z).max(), np.abs(z).max()

fig, ax = plt.subplots()

ax.imshow(slide_segment)
c = ax.pcolormesh(x, y, z, cmap='magma', vmin=z_min, vmax=z_max)
ax.set_title('metastasis heatmap')

# set the limits of the plot to the limits of the data
ax.axis([x.min(), x.max(), y.min(), y.max()])
fig.colorbar(c, ax=ax)
fig.set_figheight(12)
fig.set_figwidth(12)
plt.show()

# COMMAND ----------


