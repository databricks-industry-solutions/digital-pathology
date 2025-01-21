# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/digital-pathology. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/digital-pathology.

# COMMAND ----------

# MAGIC %md
# MAGIC # Train a binary classifier with transfer learning
# MAGIC In this notebook, we use the labeled patches as a training set to train a classifier that predicts if a patch corresponds to a metastatic site or not. 
# MAGIC To do so, we use transfer learning with `resnet18` model using pytorch, and log the resulting model with mlflow. In the next notebook we use this model to overlay a metastatic heatmap on a new slide.

# COMMAND ----------

# DBTITLE 1,if cuda available: !nvidia-smi
import torch

# Check if CUDA is available
if torch.cuda.is_available():
    # Run nvidia-smi to check GPU status
    import os
    os.system('nvidia-smi') #!nvidia-smi
else:
    print("CUDA is not available -- Using CPU")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Set & Retrieve Configs

# COMMAND ----------

# DBTITLE 1,[RUNME clusters config specifies cluster lib]
## uncomment below to run this nb separately from RUNME nb if openslide-python hasn't been installed
# %pip install openslide-python
# dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,cluster init file: openslide-tools.sh would install this
## uncomment below to run this nb separately from RUNME nb if openslide-tools hasn't been installed
# !apt-get install -y openslide-tools

# COMMAND ----------

# DBTITLE 1,run Config without overwriting patches
# MAGIC %run ./config/0-config $project_name=digital_pathology $overwrite_old_patches=no $max_n_patches=2000

# COMMAND ----------

# DBTITLE 1,Install pytorch_lightning
# MAGIC %pip install pytorch_lightning==1.6.5
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import  Dependencies
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
from mlflow.models import infer_signature


# COMMAND ----------

# DBTITLE 1,Retrieve Configs
import json
import os
from pprint import pprint

catalog_name="dbdemos"
project_name='digital_pathology' 

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

# DBTITLE 1,Set paths and MLflow Expt
import mlflow
# Set the registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

IMG_PATH = settings['img_path']
experiment_info=mlflow.set_experiment(settings['experiment_name'])

# COMMAND ----------

# DBTITLE 1,Check expt_info
experiment_info

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Data
# MAGIC We will use torchvision and `torch.utils.data` packages for loading the data.
# MAGIC Our aim is to train a model to classify extracted patches into normal `(0)` and tumor `(1)` based on provided annotations. Usually, this is a very small dataset to generalize upon, if trained from scratch. Since we are using transfer learning, we should be able to generalize reasonably well.
# MAGIC
# MAGIC This dataset is a very small subset of imagenet.

# COMMAND ----------

# DBTITLE 1,Data Transforms specification
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

# data_dir = f'/dbfs{IMG_PATH}'
data_dir = f'{IMG_PATH}'
image_datasets = {x: datasets.ImageFolder(os.path.join(data_dir, x), data_transforms[x]) for x in ['train', 'test']}
dataloaders = {x: torch.utils.data.DataLoader(image_datasets[x], batch_size=4, shuffle=True, num_workers=4) for x in ['train', 'test']}
dataset_sizes = {x: len(image_datasets[x]) for x in ['train', 'test']}
class_names = image_datasets['train'].classes

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

# COMMAND ----------

# DBTITLE 1,Check
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

# DBTITLE 1,Start mlflow.pytorch.autolog
# Set the registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Start mlflow.pytorch.autolog
mlflow.pytorch.autolog()

# COMMAND ----------

# DBTITLE 1,generic training function
def train_model(model, criterion, optimizer, scheduler, num_epochs=5, log_model=False, artifact_pathName=None):
    
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
        
        mlflow.log_metric('best_accuracy', float(best_acc))
        mlflow.log_params({"optimizer": str(optimizer), "num_epochs": num_epochs})
        
        # LOG MODEL TO MLFLOW
        if log_model:
            # Create example input and infer signature
            example_input = torch.randn(1, 3, 224, 224).to(device)
            signature = infer_signature(example_input.cpu().numpy(), model(example_input).cpu().detach().numpy())
            
            mlflow.pytorch.log_model(pytorch_model=model,
                                     artifact_path=artifact_pathName,
                                     input_example=example_input.cpu().numpy(),
                                     signature=signature)
    
        # Print run ID
        print(f"run_id: {run.info.run_id}")
                
    return run.info
  

  #refs: 
  # https://mlflow.org/docs/latest/deep-learning/pytorch/guide/index.html 
  # https://mlflow.org/docs/latest/_modules/mlflow/pytorch.html

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finetuning the convnet
# MAGIC Now we load a pretrained `resnet18` model and reset final fully connected layer. We then re-train the model based on our dataset.

# COMMAND ----------

# DBTITLE 1,Defining the CNN model
model_ft = models.resnet18(weights=models.ResNet18_Weights.DEFAULT) ## update from prior models.resnet18(pretrained=True) which is deprecated.

num_ftrs = model_ft.fc.in_features
# Here the size of each output sample is set to 2.
# Alternatively, it can be generalized to nn.Linear(num_ftrs, len(class_names)).
model_ft.fc = nn.Linear(num_ftrs, 2)

model_ft = model_ft.to(device)

# ## UNCOMMENT TO USE ALL CORES 
# if torch.cuda.is_available():
#   print(f'using {torch. cuda. device_count() } available cuda cores .... ')
#   model_ft = model_ft.cuda()
#   model_ft = torch.nn.DataParallel(model_ft)  # This wraps the model for parallel GPU usage
# else:
#   print('using cpu .... ')
#   model_ft = model_ft.to(device)

criterion = nn.CrossEntropyLoss()

# Observe that all parameters are being optimized
optimizer_ft = optim.SGD(model_ft.parameters(), lr=0.001, momentum=0.95) 

# Decay LR by a factor of 0.1 every 7 epochs
# exp_lr_scheduler = lr_scheduler.StepLR(optimizer_ft, step_size=7, gamma=0.1)
exp_lr_scheduler = lr_scheduler.StepLR(optimizer_ft, step_size=1, gamma=0.025)

# COMMAND ----------

# DBTITLE 1,Check Optimizer
exp_lr_scheduler.optimizer  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train and evaluate
# MAGIC Depending on the number of images, this step can take several minutes. 
# MAGIC For example (with the initial default parameters for optimizer_ft), for 500 patches on a `cpu` machine it takes `5` min to train. If you use a single-node cluster with 1 `gpu` this step (with 4 epochs) will take `30s`.

# COMMAND ----------

## better on 1 compute (CPU/GPU) | When there's a lot of DATA --> DataParallel might make sense
run_info=train_model(model_ft, criterion, optimizer_ft, exp_lr_scheduler, num_epochs=10,log_model=True, artifact_pathName='resnet-dp')

# COMMAND ----------

# DBTITLE 1,End the MLflow run
mlflow.end_run()

# COMMAND ----------

# MAGIC %md
# MAGIC ---   

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rank training/experimental runs by best performance metric of choice
# MAGIC
# MAGIC While running the training, we **Logged the Model**: This involves saving the model artifacts (e.g., model weights, configuration) to a specified location (e.g., a run in MLflow). This step is typically done during or after training the model.
# MAGIC
# MAGIC We can either Register the same Logged Model Or another with a higher performance metric. 
# MAGIC
# MAGIC **Registering a Model** involves creating a new version of the model in the MLflow Model Registry, which can be part of Unity Catalog. This step allows you to manage the model lifecycle, including versioning, stage transitions (e.g., from staging to production), and access control.
# MAGIC
# MAGIC NB:logging a model and registering a model to Unity Catalog (UC) are two different steps in the MLflow workflow.
# MAGIC
# MAGIC **If you run the training a few times, you can review and select to register model with best metric of interest to Unity Catalog** 

# COMMAND ----------

# DBTITLE 1,Search for experimental runs by expt. id
df = mlflow.search_runs([settings['experiment_id']]) #pandasDF

# COMMAND ----------

# DBTITLE 1,Rank by best accuracy
# Rank by best accuracy
df_sorted = df.sort_values(by='metrics.best_accuracy', ascending=False)
display(df_sorted)

# COMMAND ----------

# DBTITLE 1,Get best metric run_id etc.
run_id, artifact_uri, metrics_best_acc = df_sorted.iloc[0][['run_id', 'artifact_uri', 'metrics.best_accuracy']]
run_id, artifact_uri, metrics_best_acc

# COMMAND ----------

# MAGIC %md
# MAGIC You can regester the model in an experimental run with the best performing metric for existing runs.

# COMMAND ----------

# DBTITLE 1,Register best metric run_id model to UC
import mlflow

# # Set the registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# run_id = run_id #run.info.run_id
artifact_pathName = 'resnet-dp'

# model_uri = 'runs:/<run_id>/resnet-dp'
model_uri = f"runs:/{run_id}/{artifact_pathName}"

print(f"Model URI: {model_uri}")

model_name = "resnet_bestmetric"
full_model_name = f"{catalog_name}.{project_name}.{model_name}"

# Register the model to Unity Catalog
mlflow.register_model(model_uri=model_uri, name=full_model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can use this registered model for inference 
