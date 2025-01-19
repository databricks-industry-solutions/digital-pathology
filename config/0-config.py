# Databricks notebook source
# MAGIC %md
# MAGIC This notebook is to setup your intial configuration for your project. You may only need to run this once and after that project configuration can be shared with other notebooks.
# MAGIC This notebook is executed from by `01-create-annotation-deltalake`.

# COMMAND ----------

# DBTITLE 1,Interactive Cluster setup info.
## Interactive Cluster Info.

# 14.3.x-gpu-ml-scala2.12 | Unity Catalog | g4dn.4xlarge
# 2-8 Workers -- 128-512 GB Memory, 32-128 Cores 
# 1 Driver -- 64 GB Memory, 16 Cores 

# SINGLE NODE
# 14.3.x-gpu-ml-scala2.12 | Unity Catalog | g4dn.8xlarge
# 1 Driver -- 128 GB Memory, 32 Cores 

# 14.3.x-cpu-ml-scala2.12 | Unity Catalog | i3.2xlarge
# 1 Driver -- 61 GB Memory, 8 Cores 

#*# 14.3.x-cpu-ml-scala2.12 | Unity Catalog | i3.4xlarge
# 1 Driver -- 122 GB Memory, 16 Cores 

## include init scripts: /Workspace/Users/<user-email>/<repoORdirectory-location>/digital-pathology/openslide-tools.sh

# COMMAND ----------

# DBTITLE 1,Reset UC catalog-schema
 # %sql
# DROP SCHEMA IF EXISTS dbdemos.`digital-pathology` CASCADE;

# COMMAND ----------

# DBTITLE 1,Clear widgets if updates/reset required
## if required reset widgets
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Set nb parameters with widgets
dbutils.widgets.text('catalog_name','dbdemos') ## update to use specific catalog if needed
dbutils.widgets.text('project_name','digital_pathology') ## using underscore for UC
dbutils.widgets.dropdown('overwrite_old_patches','no',['yes','no'])
dbutils.widgets.text('max_n_patches','500')

# COMMAND ----------

# DBTITLE 1,Get parameter values from widgets
catalog_name=dbutils.widgets.get('catalog_name')
project_name=dbutils.widgets.get('project_name')
overwrite=dbutils.widgets.get('overwrite_old_patches')
max_n_patches=int(dbutils.widgets.get('max_n_patches'))

# COMMAND ----------

# DBTITLE 1,Check widget params
# print(f"Catalog Name: {catalog_name}")
# print(f"Project Name: {project_name}")
# print(f"Overwrite Old Patches: {overwrite}")
# print(f"Max Number of Patches: {max_n_patches}")

# COMMAND ----------

# DBTITLE 1,Create Catalog.Schema if NOT EXIST
# Create the catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{project_name}")

# Create a managed volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{project_name}.files")

# Optionally, create an external volume
# spark.sql("CREATE EXTERNAL VOLUME IF NOT EXISTS dbdemos.digital_pathology.files LOCATION 's3://your-bucket-path/digital_pathology/files'")

# COMMAND ----------

# DBTITLE 1,Specify path to raw data for each project
project_data_paths = {'digital_pathology':"/databricks-datasets/med-images/camelyon16/",
                      'omop-cdm-100K':"s3://hls-eng-data-public/data/rwe/all-states-90K/",
                      "omop-cdm-10K":"s3://hls-eng-data-public/data/synthea/",
                      'psm':"s3://hls-eng-data-public/data/rwe/dbx-covid-sim/"
                     }

# COMMAND ----------

# DBTITLE 1,Specify Config Class SolAccUtil for project setup
import mlflow
import os
import hashlib

class SolAccUtil:
  def __init__(self,project_name,max_n_patches=max_n_patches,
               patch_size=299,level=0,data_path=None,base_path=None):
    
    user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
    user_uid = abs(hash(user)) % (10 ** 5)
    
    catalog = catalog_name # specify & use specific catalog (above) if needed
    
    ## updated project_name with underscore instead
    # project_name2use = f"{project_name}".replace('-','_') ## better to use '_' for UC

    if base_path!=None:
      base_path=base_path
    else:      
      # base_path = f"/Volumes/{catalog}/{project_name2use}/files"
      base_path = f"/Volumes/{catalog}/{project_name}/files"
      
    if data_path != None:
      data_path=data_path
    else:
      data_path=project_data_paths[project_name] ## keep_same/original
     
    dbutils.fs.mkdirs(base_path)
    # delta_path= f"/Volumes/{catalog}/{project_name2use}/files/delta"
    delta_path= f"/Volumes/{catalog}/{project_name}/files/delta"

    # experiment_name=os.path.join('/Users',user,project_name2use) ## update if needed
    experiment_name=os.path.join('/Users',user,project_name) ## update if needed

    ## to-check wrt model registration to UC params to add? 
    if not mlflow.get_experiment_by_name(experiment_name):
      experiment_id = mlflow.create_experiment(experiment_name)
      experiment = mlflow.get_experiment(experiment_id)
    else:
      experiment = mlflow.get_experiment_by_name(experiment_name)
      
    self.settings = {}
    self.settings['max_n_patches']=max_n_patches
    
    # self.settings['img_path']=f'/Volumes/{catalog}/{project_name2use}/files/imgs' 
    self.settings['img_path']=f'/Volumes/{catalog}/{project_name}/files/imgs' 

    ## include these -- to update use in nbs 
    self.settings['catalog']=catalog
    # self.settings['project_name2use']=project_name2use
    self.settings['project_name']=project_name

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
    out_file=f'{dest_path}/{fname}'

    print('-*-'*20)
    print(f'downloading file {fname} to {out_file}')
    print('-*-'*20)
    open(out_file,'wb').write(r.content)
    if unpack:
      print(f'unpacking file {fname} into {dest_path}')
      import tarfile
    # open file
      file = tarfile.open(os.path.join('dbfs:',dest_path,fname))
      file.extractall(os.path.join('dbfs:',dest_path))
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

    return self

# COMMAND ----------

# DBTITLE 1,Check CLASS: project_utils
project_utils = SolAccUtil(
    project_name=project_name,
    max_n_patches=max_n_patches  # Replace with the appropriate value
)

# COMMAND ----------

# DBTITLE 1,Write configurations for later access
import json 

## update to UC Volumes 
# with open(f"/Volumes/mmt/{project_utils.settings['project_name2use']}/files/{project_utils.settings['user_uid']}_{project_utils.settings['project_name2use'] }_configs.json",'w') as f:

## update to UC Volumes 
with open(f"/Volumes/{catalog_name}/{project_utils.settings['project_name']}/files/{project_utils.settings['user_uid']}_{project_utils.settings['project_name'] }_configs.json",'w') as f:
  f.write(json.dumps(project_utils.settings,indent=4))
f.close()

# COMMAND ----------

# DBTITLE 1,Display project settings
## project_utils.print_info()
# print('use project_utils for access to settings')

## Check Settings in project_utils
# project_utils.settings

# COMMAND ----------

# DBTITLE 1,Existing patches
if overwrite=='yes':
  try:
    dbutils.fs.rm(project_utils.settings['img_path'],recurse=True)
  except:
    pass
  
try:
  display(dbutils.fs.ls(project_utils.settings['img_path']))
except:
  print('no existing patches')

# COMMAND ----------


