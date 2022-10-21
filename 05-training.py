# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/digital-pathology. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/digital-pathology.

# COMMAND ----------

# MAGIC %md
# MAGIC # Train a binary classifier with transfer learning
# MAGIC In this notebook, we use the labeled pacthes as a training set to train a classifier that predicts if a patch corresponds to a metastatic site or not. 
# MAGIC To do so, we use transfer learning with `resnet18` model using pytorch, and log the resulting model with mlflow. In the next notebook we use this model to overlay a metastatic heatmap on a new slide.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Initial Config

# COMMAND ----------

# MAGIC %pip install pytorch_lightning==1.6.5

# COMMAND ----------

from __future__ import print_function, division

import torch
import torch.nn as nn
import torch.optim as optim
from torch.optim import lr_scheduler
import numpy as np
import torchvision
from torchvision import datasets, models, transforms
import matplotlib.pyplot as plt
import time
import os
import copy

import pytorch_lightning

import mlflow
import mlflow.pytorch
from mlflow.tracking import MlflowClient

# import mlflow
# plt.ion()   # interactive mode

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

# DBTITLE 1,configuration
IMG_PATH = settings['img_path']
experiment_info=mlflow.set_experiment(settings['experiment_name'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Data
# MAGIC We will use torchvision and `torch.utils.data` packages for loading the data.
# MAGIC Our aim is to train a model to classify extrated patches into normal `(0)` and tumor `(1)` based on provided annotations. Usually, this is a very small dataset to generalize upon, if trained from scratch. Since we are using transfer learning, we should be able to generalize reasonably well.
# MAGIC 
# MAGIC This dataset is a very small subset of imagenet.

# COMMAND ----------

# Data augmentation and normalization for training
# Just normalization for validation
data_transforms = {
    'train': transforms.Compose([
        transforms.RandomResizedCrop(224),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ]),
    'test': transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ]),
}

data_dir = f'/dbfs{IMG_PATH}'
image_datasets = {x: datasets.ImageFolder(os.path.join(data_dir, x), data_transforms[x]) for x in ['train', 'test']}
dataloaders = {x: torch.utils.data.DataLoader(image_datasets[x], batch_size=4, shuffle=True, num_workers=4) for x in ['train', 'test']}
dataset_sizes = {x: len(image_datasets[x]) for x in ['train', 'test']}
class_names = image_datasets['train'].classes

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

# COMMAND ----------

print(f"train/test dataset: {dataset_sizes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Training the model
# MAGIC Now, let’s write a general function to train a model. Here, we will illustrate:
# MAGIC 
# MAGIC - Scheduling the learning rate
# MAGIC - Saving the best model
# MAGIC In the following, parameter scheduler is an [LR scheduler object](https://pytorch.org/docs/stable/optim.html#how-to-adjust-learning-rate) from `torch.optim.lr_scheduler`.

# COMMAND ----------

mlflow.pytorch.autolog()

# COMMAND ----------

# DBTITLE 1,generic training function
def train_model(model, criterion, optimizer, scheduler, num_epochs=5, log_model=False):
    
    with mlflow.start_run(run_name='resnet-training') as run:
      
      since = time.time()
      best_model_wts = copy.deepcopy(model.state_dict())
      best_acc = 0.0
  
      for epoch in range(num_epochs):
          print('Epoch {}/{}'.format(epoch, num_epochs - 1))
          print('-' * 10)
  
          # Each epoch has a training and validation phase
          for phase in ['train', 'test']:
              if phase == 'train':
                  model.train()  # Set model to training mode
              else:
                  model.eval()   # Set model to evaluate mode
  
              running_loss = 0.0
              running_corrects = 0
  
              # Iterate over data.
              for inputs, labels in dataloaders[phase]:
                  inputs = inputs.to(device)
                  labels = labels.to(device)
  
                  # zero the parameter gradients
                  optimizer.zero_grad()
  
                  # forward
                  # track history if only in train
                  with torch.set_grad_enabled(phase == 'train'):
                      outputs = model(inputs)
                      _, preds = torch.max(outputs, 1)
                      loss = criterion(outputs, labels)
  
                      # backward + optimize only if in training phase
                      if phase == 'train':
                          loss.backward()
                          optimizer.step()
  
                  # statistics
                  running_loss += loss.item() * inputs.size(0)
                  running_corrects += torch.sum(preds == labels.data)
              if phase == 'train':
                  scheduler.step()
  
              epoch_loss = running_loss / dataset_sizes[phase]
              epoch_acc = running_corrects.double() / dataset_sizes[phase]
  
              print('{} Loss: {:.4f} Acc: {:.4f}'.format(
                  phase, epoch_loss, epoch_acc))
  
              # deep copy the model
              if phase == 'test' and epoch_acc > best_acc:
                  best_acc = epoch_acc
                  best_model_wts = copy.deepcopy(model.state_dict())
  
          print()
  
      time_elapsed = time.time() - since
      print('Training complete in {:.0f}m {:.0f}s'.format(
          time_elapsed // 60, time_elapsed % 60))
      print('Best val Acc: {:4f}'.format(best_acc))
  
      # load best model weights
      model.load_state_dict(best_model_wts)
      
      mlflow.log_metric('best_accuracy',float(best_acc))
      
      if(log_model):
        mlflow.pytorch.log_model(model_ft,'resent-dp')
        
    return(run.info)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finetuning the convnet
# MAGIC Now we load a pretrained `resnet18` model and reset final fully connected layer. We then re-train the model based on our dataset.

# COMMAND ----------

model_ft = models.resnet18(pretrained=True)
num_ftrs = model_ft.fc.in_features
# Here the size of each output sample is set to 2.
# Alternatively, it can be generalized to nn.Linear(num_ftrs, len(class_names)).
model_ft.fc = nn.Linear(num_ftrs, 2)

model_ft = model_ft.to(device)

criterion = nn.CrossEntropyLoss()

# Observe that all parameters are being optimized
optimizer_ft = optim.SGD(model_ft.parameters(), lr=0.001, momentum=0.9)

# Decay LR by a factor of 0.1 every 7 epochs
exp_lr_scheduler = lr_scheduler.StepLR(optimizer_ft, step_size=7, gamma=0.1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train and evaluate
# MAGIC Depending on the number of images, this step can take several minitues. For example for 500 patches on a `cpu` machine it takes `5` min to train. If you use a single-node cluster with 1 `gpu` this step (with 4 epochs) will take `30s`.

# COMMAND ----------

run_info=train_model(model_ft, criterion, optimizer_ft, exp_lr_scheduler, num_epochs=4,log_model=True)

# COMMAND ----------

mlflow.end_run()

# COMMAND ----------

df = mlflow.search_runs([settings['experiment_id']])

# COMMAND ----------

df.sort_values(by='metrics.best_accuracy',ascending=False).display()
