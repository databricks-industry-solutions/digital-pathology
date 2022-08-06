# Databricks notebook source
# MAGIC %md
# MAGIC This notebook is to setup your intial configuration for your project. You may only need to run this once and after that project configuration can be shared with other notebooks.
# MAGIC This notebook is executed from by `01-create-annotation-deltalake`.

# COMMAND ----------

dbutils.widgets.text('project_name','digital-pathology')
dbutils.widgets.dropdown('overwrite_old_patches','no',['yes','no'])
dbutils.widgets.text('max_n_patches','500')

# COMMAND ----------

# DBTITLE 0,add widgets
import mlflow
project_name=dbutils.widgets.get('project_name')
overwrite=dbutils.widgets.get('overwrite_old_patches')
max_n_patches=int(dbutils.widgets.get('max_n_patches'))

# COMMAND ----------

# DBTITLE 1,generate the init script
dbutils.fs.mkdirs('/tmp/openslide/')
dbutils.fs.put('/tmp/openslide/openslide-tools.sh',
               """
               #!/bin/bash
               apt-get install -y openslide-tools
               """, overwrite=True)

# COMMAND ----------

# DBTITLE 1,specify path to raw data for each project
project_data_paths = {'digital-pathology':"/databricks-datasets/med-images/camelyon16/",'omop-cdm-100K':"s3://hls-eng-data-public/data/rwe/all-states-90K/","omop-cdm-10K":"s3://hls-eng-data-public/data/synthea/",'psm':"s3://hls-eng-data-public/data/rwe/dbx-covid-sim/"}

# COMMAND ----------

# DBTITLE 1,class for project setup
import mlflow
import os
import hashlib
class SolAccUtil:
  def __init__(self,project_name,max_n_patches=max_n_patches,patch_size=299,level=0,data_path=None,base_path=None):
    user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
    project_name = project_name.strip().replace(' ','-')
    user_uid = abs(hash(user)) % (10 ** 5)
    if base_path!=None:
      base_path=base_path
    else:
      base_path = os.path.join('/home',user,project_name)
      
    if data_path != None:
      data_path=data_path
    else:
      data_path=project_data_paths[project_name]
     
    dbutils.fs.mkdirs(base_path)
    delta_path=os.path.join(base_path,project_name,'delta')
    experiment_name=os.path.join('/Users',user,project_name)
    
    if not mlflow.get_experiment_by_name(experiment_name):
      experiment_id = mlflow.create_experiment(experiment_name)
      experiment = mlflow.get_experiment(experiment_id)
    else:
      experiment = mlflow.get_experiment_by_name(experiment_name)
    self.settings = {}
    self.settings['max_n_patches']=max_n_patches
    self.settings['img_path']=f'/ml/{project_name}-{user_uid}'     
    self.settings['base_path']=base_path
    self.settings['delta_path']=delta_path
    self.settings['data_path']=data_path
    self.settings['experiment_name']=experiment.name
    self.settings['experiment_id']=experiment.experiment_id
    self.settings['artifact_location']=experiment.artifact_location
    self.settings['tags']=experiment.tags
    self.settings['patch_size']=patch_size
    self.settings['level']=level
    self.settings['user']=user
    self.settings['user_uid']=user_uid

  def load_remote_data(self,url,dest_path,unpack=False):
    import requests
    fname=url.split('/')[-1]
    r = requests.get(url)
    out_file=f'/dbfs{dest_path}/{fname}'
    print('-*-'*20)
    print(f'downloading file {fname} to {out_file}')
    print('-*-'*20)
    open(out_file,'wb').write(r.content)
    if unpack:
      print(f'unpacking file {fname} into {dest_path}')
      import tarfile
    # open file
      file = tarfile.open(os.path.join('/dbfs',dest_path,fname))
      file.extractall(os.path.join('/dbfs',dest_path))
      file.close()
    
  def print_info(self):
    _html='<p>'
    for key,val in self.settings.items():
      _html+=f'<b>{key}</b> = <i>{val}</i><br>'
    _html+='</p>'
    displayHTML(_html)
    
  def display_data(self):
    files=dbutils.fs.ls(f'{self.data_path}')
    if len(files)==0:
      print('no data available, please run load_remote_data(<url for the data>)')
    else:
      print('*'*100)
      print(f'data available in {self.data_path} are:')
      print('*'*100)
      display(files)

# COMMAND ----------

# DBTITLE 1,define project settings
project_utils = SolAccUtil(project_name=project_name)

# COMMAND ----------

# DBTITLE 1,write configurations for later access
import json 
with open(f"/dbfs/FileStore/{project_utils.settings['user_uid']}_{project_name}_configs.json",'w') as f:
  f.write(json.dumps(project_utils.settings,indent=4))
f.close()

# COMMAND ----------

# DBTITLE 1,display project settings
project_utils.print_info()
print('use project_utils for access to settings')

# COMMAND ----------

# DBTITLE 1,existing patches
if overwrite=='yes':
  try:
    dbutils.fs.rm(project_utils.settings['img_path'],recurse=True)
  except:
    pass
  
try:
  display(dbutils.fs.ls(project_utils.settings['img_path']))
except:
  print('no existing patches')
