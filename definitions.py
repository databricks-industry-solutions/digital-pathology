# Databricks notebook source
# MAGIC %md
# MAGIC This notebook contains definitions for functions that will be used multiple times in this solution accelerator.
# MAGIC The `PatchGenerator` class, contains methods to extract patches based on a given coordinate within the slide.
# MAGIC To extract patches and manipulate WSI images, we use the [OpenSlide library](https://openslide.org/), which is assumed to be installed during the clsuter configuration using [init script](https://docs.databricks.com/user-guide/clusters/init-scripts.html)

# COMMAND ----------

import io
from io import StringIO, BytesIO

from typing import Iterator
from PIL import Image
import openslide

import pandas as pd
import numpy as np

import mlflow
import mlflow.pytorch

from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.functions import pandas_udf

from tensorflow.keras.applications.inception_v3 import InceptionV3

import torch
from torch.utils.data import Dataset, DataLoader
from torchvision import models, transforms

# COMMAND ----------

class PatchGenerator:
  """
  class for distributed WSI patch generation with Apache Spark
  """
  def __init__(self,wsi_path,level,patch_size, img_path):
    self.wsi_path=f'/dbfs{wsi_path}'
    self.level=level
    self.patch_size=patch_size
    self.img_path = img_path

  @staticmethod
  def get_patch(sid,x_center,y_center,patch_size,level,img_path,wsi_path, save=False):
    '''
    function to extract a patch from slide by coordinates
    '''
    path=f'{wsi_path}{sid}.tif'
    slide = openslide.OpenSlide(path)
    x = int(x_center) - patch_size // 2
    y = int(y_center) - patch_size // 2
    
    patch_name=f'{sid}-{x_center}-{y_center}'
    img = slide.read_region((x, y), level,(patch_size, patch_size)).convert('RGB')
    
    if not save:
      output = BytesIO()
      img.save(output, format="png")
      image_as_string = output.getvalue()
      return (image_as_string)
    
    else:
      processed_patch_path=f'/dbfs{img_path}/{patch_name}.jpg'
      img.save(processed_patch_path)
      return(processed_patch_path)
    
  def dist_patch_save (self,pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in pdf_iter:
      processed_img_pdf=pd.DataFrame({
        'label':pdf['label'],
        'x_center' : pdf['x_center'],
        'y_center' : pdf['y_center'],
        'processed_img':pdf.apply(lambda x:self.get_patch(x['sid'],x['x_center'],x['y_center'],self.patch_size,self.level,x['img_path'],self.wsi_path,save=True),axis=1)
      })
      yield processed_img_pdf
      
  def dist_patch_extract (self,pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    
    for pdf in pdf_iter:
      processed_img_pdf=pd.DataFrame({
        'label':pdf['label'],
        'x_center' : pdf['x_center'],
        'y_center' : pdf['y_center'],
        'processed_img':pdf.apply(lambda x:self.get_patch(x['sid'],x['x_center'],x['y_center'],self.patch_size,self.level,self.img_path,self.wsi_path),axis=1)
      })
      yield processed_img_pdf

# COMMAND ----------

model = InceptionV3(include_top=False)
broadcaseted_model_weights = sc.broadcast(model.get_weights())

def model_fn(include_top=False):
  """
  Returns a InceptionV3 model with top layer removed and broadcasted pretrained weights.
  """
  model = InceptionV3(weights=None, include_top=include_top)
  model.set_weights(broadcaseted_model_weights.value)
  return model


def featurize_raw_img_series(model, content_series):
  """
  Featurize a pd.Series of raw images using the input model.
  :return: a pd.Series of image features
  """
  def convert_img(content):
    image = Image.open(BytesIO(content),formats=['JPEG'])
    return(np.asarray(image))
  
  _input = np.stack(content_series.map(lambda raw_img: convert_img(raw_img)))
  preds = model.predict(_input)
  output = [p.flatten() for p in preds]
  return pd.Series(output)

@pandas_udf('array<float>')
def featurize_raw_img_series_udf (content_series_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
  '''
  This method is a Scalar Iterator pandas UDF wrapping our featurization function.
  The decorator specifies that this returns a Spark DataFrame column of type ArrayType(FloatType).
  
  :param content_series_iter: This argument is an iterator over batches of data, where each batch
                              is a pandas Series of image data.
  '''
  # With Scalar Iterator pandas UDFs, we can load the model once and then re-use it
  # for multiple data batches.  This amortizes the overhead of loading big models.
  model = model_fn()
  for content_series in content_series_iter:
    yield featurize_raw_img_series(model, content_series)

# COMMAND ----------

class ImageNetDataset(Dataset):
  """
  Converts image contents into a PyTorch Dataset with standard ImageNet preprocessing.
  """
  def __init__(self, contents):
    self.contents = contents

  def __len__(self):
    return len(self.contents)

  def __getitem__(self, index):
    return self._preprocess(self.contents[index])

  def _preprocess(self, content):
    """
    Preprocesses the input image content using standard ImageNet normalization.
    
    See https://pytorch.org/docs/stable/torchvision/models.html.
    """
    image = Image.open(io.BytesIO(content))
    transform = transforms.Compose([
      transforms.Resize(224),
      transforms.CenterCrop(224),
      transforms.ToTensor(),
      transforms.Normalize(mean=[0.485, 0.456, 0.406],
                       std=[0.229, 0.224, 0.225])
    ])
    return transform(image)
