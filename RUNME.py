# Databricks notebook source
# MAGIC %md
# MAGIC ## RUNME
# MAGIC This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow: `[RUNNER]_digital_pathology_{user_initials}{YYYYMMDD}` to illustrate the order of execution. Happy exploring! 
# MAGIC 🎉

# COMMAND ----------

# MAGIC %md
# MAGIC ![RUNME_pipeline_setup](./imgs/RUNME_pipeline_setup.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Simply attach this notebook to a cluster and hit Run-All for this notebook. A multi-step job and the clusters used in the job will be created for you and hyperlinks are printed on the last block of the notebook. 
# MAGIC
# MAGIC 2. For clusters with **`*_w_init` suffixes**: Check cluster's Advance Options to make sure the the `openslide-tools.sh` workspace file path is added to the `Init scripts` -- **_Do this before you run the notebooks or workflow_** (see step 3.)
# MAGIC
# MAGIC 3. Run the accelerator notebooks: Feel free to explore the multi-step job page and **run the Workflow**, or **run the notebooks interactively** with the cluster to see how this solution accelerator executes. 
# MAGIC
# MAGIC     3a. **Run the Workflow**: Navigate to the Workflow link and hit the `Run Now` 💥. 
# MAGIC   
# MAGIC     3b. **Run the notebooks interactively**: Attach the notebook with the cluster(s) created and execute as described in the `job_json['tasks']` below.
# MAGIC
# MAGIC **Prerequisites** 
# MAGIC 1. You need to have cluster creation permissions in this workspace.
# MAGIC
# MAGIC 2. In case the environment has cluster-policies that interfere with automated deployment, you may need to manually create the cluster in accordance with the workspace cluster policy. The `job_json` definition below still provides valuable information about the configuration these series of notebooks should run with. 
# MAGIC
# MAGIC **Notes**
# MAGIC 1. The pipelines, workflows and clusters created in this script are not user-specific. Keep in mind that rerunning this script again after modification resets them for other users too.
# MAGIC
# MAGIC 2. If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators may require the user to set up additional cloud infra or secrets to manage credentials. 

# COMMAND ----------

# DBTITLE 1,original code
#### (original) -- this previous approach now has a recent code-change / PR induced Error

## Install util packages (separate cell)
# %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html pyspark>=3.1.2 --quiet --disable-pip-version-check
# dbutils.library.restartPython()


## Import solution accelerator companion modules/tools (separate cell)
# from solacc.companion import NotebookSolutionCompanion
# nsc = NotebookSolutionCompanion()

# COMMAND ----------

# MAGIC %md
# MAGIC **Jan2025_Update NB:** Recent updates to repository dependency `https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html` broke the existing code process. Hidden/Commented code reflects prior version.    
# MAGIC    
# MAGIC **Workaround Solution Provided:** ` solacc/companion/_init_.py` from a previous [`Pull Request`](https://github.com/databricks-industry-solutions/notebook-solution-companion/blob/f7e381d77675b29c2d3f9d377a528ceaf2255f23/solacc/companion/__init__.py) is copied to  `solacc_companion_init` in the workspace and `%run` to access `NotebookSolutionCompanion()` 
# MAGIC
# MAGIC **July2025_Update NB:** `<to document changes>`

# COMMAND ----------

# DBTITLE 1,install utils without solacc companion via git branch
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 pyspark>=3.1.2 databricks-sdk>=0.32.0 --quiet --disable-pip-version-check
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Widgets for run_job & use_existing_job
dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"

dbutils.widgets.dropdown("use_existing_job", "True", ["True", "False"])
use_existing_job = dbutils.widgets.get("use_existing_job") == "True"

# COMMAND ----------

# DBTITLE 1,run copied solacc companion module from workspace path
# MAGIC %run ./solacc_companion_init

# COMMAND ----------

# DBTITLE 1,access module NotebookSolutionCompanion()
nsc = NotebookSolutionCompanion()

# COMMAND ----------

# DBTITLE 1,Get User Info.
## This is so that each user can set up their own workflow pipeline
from datetime import datetime

# get user info.
user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

suffix = ''.join([s[0] for s in user.split('@')[0].split('.')]) + datetime.now().strftime("%Y%m%d")
suffix

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Databricks Instances + Token
databricks_instance = "https://e2-demo-field-eng.cloud.databricks.com/" #dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()    
databricks_token = dbutils.secrets.get("mmt", "databricks_token") #"<your-PATorSP-token>"

# Example to add a PAT/SP token to dbutils secrets using the CLI:
# 1. Open your terminal.
# 2. Use the Databricks CLI to create a secret scope (if not already created):
#    databricks secrets create-scope --scope <scope-name>
# 3. Add your token to the secret scope:
#    databricks secrets put --scope <scope-name> --key <key-name>
# 4. You will be prompted to enter the secret value (your PAT/SP token).

# Example to add a PAT/SP token to dbutils secrets within a notebook:
# Note: This step cannot be done directly within a notebook using dbutils.secrets API.
# You need to use the Databricks CLI or REST API to add secrets.

# For more information on setting up Personal Access Tokens (PAT) and Service Principals (SP) for authentication, refer to the Databricks documentation:
# https://docs.databricks.com/dev-tools/api/latest/authentication.html

# Databricks documentation for setting up PAT and SP:
# https://docs.databricks.com/dev-tools/api/latest/authentication.html#token-management

# Documentation on how to store secrets e.g. tokens: 
# https://docs.databricks.com/aws/en/security/secrets/

# COMMAND ----------

# DBTITLE 1,Example of adding PAT/SP to databricks api secrets
import getpass
import requests

databricks_token = getpass.getpass(prompt='Please enter your Databricks token: ')

# Function to add a secret to Databricks secrets store using REST API
def add_secret_to_databricks(scope, key, secret_value, databricks_instance, databricks_token):
    url = f"{databricks_instance}/api/2.0/secrets/put"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "scope": scope,
        "key": key,
        "string_value": secret_value
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print("Secret added successfully.")
    else:
        print(f"Failed to add secret: {response.status_code} - {response.text}")

# Prompt user for scope and key
scope = input("Enter the secret scope: ")
key = input("Enter the secret key: ")

# Use the token obtained from the first prompt as the secret value
secret_value = databricks_token

# Add the secret to Databricks secrets store
add_secret_to_databricks(scope, key, secret_value, databricks_instance, databricks_token)

# COMMAND ----------

# DBTITLE 1,Cluster Config -- Check Existing Else Create Clusters
def get_existing_clusters(headers, instance):
    import requests
    
    response = requests.get(
        f"{instance}/api/2.0/clusters/list",
        headers=headers
    )
    
    if response.status_code == 200:
        return response.json().get("clusters", [])
    else:
        print(f"❌ Failed to retrieve existing clusters: {response.text}")
        return []

def cluster_config_matches(existing_cluster, config):
    keys_to_check = ["spark_version", "num_workers", "node_type_id", "autotermination_minutes", "data_security_mode", "single_user_name"]
    for key in keys_to_check:
        if existing_cluster.get(key) != config.get(key):
            return False
    return True

def create_all_clusters_with_unity_catalog():
    import requests
    import json
        
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    
    # 1. CPU cluster WITHOUT init scripts (WITH Unity Catalog)
    cpu_cluster_config = {
        "cluster_name": "pathology_14-3-x_cpu_cluster",
        "spark_version": "14.3.x-cpu-ml-scala2.12",
        "num_workers": 2,
        "node_type_id": "i3.xlarge",
        "autotermination_minutes": 60,
        "data_security_mode": "SINGLE_USER",  # Required for Unity Catalog
        "single_user_name": dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    }
    
    # 2. CPU cluster WITH init scripts (WITH Unity Catalog)
    cpu_init_cluster_config = {
        "cluster_name": "pathology_14-3-x_cpu_cluster_w_init",
        "spark_version": "14.3.x-cpu-ml-scala2.12",
        "num_workers": 2,
        "node_type_id": "i3.xlarge",
        "autotermination_minutes": 60,
        "data_security_mode": "SINGLE_USER",  # Required for Unity Catalog
        "single_user_name": dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get(),
        "init_scripts": [
            {
                "workspace": {
                    "destination": f"{nsc.solacc_path}/openslide-tools.sh"
                }
            }
        ]
    }
    
    # 3. GPU cluster WITH init scripts (WITH Unity Catalog)
    gpu_init_cluster_config = {
        "cluster_name": "pathology_14-3-x_gpu_cluster_w_init",
        "spark_version": "14.3.x-gpu-ml-scala2.12",
        "num_workers": 2,
        "node_type_id": "g4dn.4xlarge",
        "autotermination_minutes": 60,
        "data_security_mode": "SINGLE_USER",  # Required for Unity Catalog
        "single_user_name": dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get(),
        "init_scripts": [
            {
                "workspace": {
                    "destination": f"{nsc.solacc_path}/openslide-tools.sh"
                }
            }
        ]
    }
    
    # Create all clusters
    cluster_configs = [cpu_cluster_config, cpu_init_cluster_config, gpu_init_cluster_config]
    clusters_created = {}
    
    existing_clusters = get_existing_clusters(headers, databricks_instance)
    
    for config in cluster_configs:
        existing_cluster = next((cluster for cluster in existing_clusters if cluster["cluster_name"] == config["cluster_name"] and cluster_config_matches(cluster, config)), None)
        
        if existing_cluster:
            cluster_id = existing_cluster["cluster_id"]
            clusters_created[config["cluster_name"]] = cluster_id
            print(f"✅ Using existing UC-enabled cluster {config['cluster_name']}: {cluster_id}")
        else:
            print(f"Creating Unity Catalog enabled cluster: {config['cluster_name']}")
            
            response = requests.post(
                f"{databricks_instance}/api/2.0/clusters/create",
                headers=headers,
                data=json.dumps(config)
            )
            
            if response.status_code == 200:
                cluster_id = response.json()["cluster_id"]
                clusters_created[config["cluster_name"]] = cluster_id
                print(f"✅ Created UC-enabled cluster {config['cluster_name']}: {cluster_id}")
            else:
                print(f"❌ Failed to create {config['cluster_name']}: {response.text}")
    
    return clusters_created

# Create Unity Catalog enabled clusters
print("🚀 Creating Unity Catalog enabled clusters...")
uc_clusters = create_all_clusters_with_unity_catalog()

# COMMAND ----------

# DBTITLE 1,List Created/Existing Clusters
uc_clusters

# COMMAND ----------

# DBTITLE 1,Define job_json with deployed clusters
# Get the cluster IDs from above
cpu_cluster_id = uc_clusters["pathology_14-3-x_cpu_cluster"]
cpu_init_cluster_id = uc_clusters["pathology_14-3-x_cpu_cluster_w_init"]
gpu_init_cluster_id = uc_clusters["pathology_14-3-x_gpu_cluster_w_init"]

job_json_with_deployed_clusters = {
    "name": f"[RUNNER]_digital_pathology_{suffix}",
    "timeout_seconds": 28800,
    "max_concurrent_runs": 1,
    "tags": {
        "usage": "solacc_testing",
        "group": "HLS",
        "do_not_delete": True,
        "removeAfter": "2026-01-31" ## update where appropriate
    },
    "tasks": [
        {
            "task_key": "Pathology_00",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "00-create-annotation-UC",
                "source": "WORKSPACE"
            },
            "existing_cluster_id": cpu_cluster_id,
            "timeout_seconds": 0,
            "email_notifications": {}
        },
        {
            "task_key": "Pathology_01",
            "depends_on": [{"task_key": "Pathology_00"}],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "01-README",
                "source": "WORKSPACE"
            },
            "existing_cluster_id": cpu_init_cluster_id,
            "libraries": [{"pypi": {"package": "openslide-python"}}],
            "timeout_seconds": 0,
            "email_notifications": {}
        },
        {
            "task_key": "Pathology_02",
            "depends_on": [{"task_key": "Pathology_01"}],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "02-patch-generation",
                "source": "WORKSPACE"
            },
            "existing_cluster_id": cpu_init_cluster_id,
            "libraries": [{"pypi": {"package": "openslide-python"}}],
            "timeout_seconds": 0,
            "email_notifications": {}
        },
        {
            "task_key": "Pathology_03",
            "depends_on": [{"task_key": "Pathology_02"}],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "03-feature-extraction",
                "source": "WORKSPACE"
            },
            "existing_cluster_id": cpu_init_cluster_id,
            "libraries": [{"pypi": {"package": "openslide-python"}}],
            "timeout_seconds": 0,
            "email_notifications": {}
        },
        {
            "task_key": "Pathology_04",
            "depends_on": [{"task_key": "Pathology_03"}],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "04-unsupervised-learning",
                "source": "WORKSPACE"
            },
            "existing_cluster_id": cpu_init_cluster_id,
            "libraries": [{"pypi": {"package": "openslide-python"}}],
            "timeout_seconds": 0,
            "email_notifications": {}
        },
        {
            "task_key": "Pathology_05",
            "depends_on": [{"task_key": "Pathology_02"}],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "05-training",
                "source": "WORKSPACE"
            },
            "existing_cluster_id": gpu_init_cluster_id,
            "libraries": [{"pypi": {"package": "openslide-python"}}],
            "timeout_seconds": 0,
            "email_notifications": {}
        },
        {
            "task_key": "Pathology_06",
            "depends_on": [{"task_key": "Pathology_05"}],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": "06-metastasis-heatmap",
                "source": "WORKSPACE"
            },
            "existing_cluster_id": gpu_init_cluster_id,
            "libraries": [{"pypi": {"package": "openslide-python"}}],
            "timeout_seconds": 0,
            "email_notifications": {}
        }
    ]
}

# COMMAND ----------

# DBTITLE 1,deploy (new) job pipeline
# dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
# run_job = dbutils.widgets.get("run_job") == "True"

# nsc.deploy_compute(job_json_with_deployed_clusters, run_job=run_job)

# COMMAND ----------

# DBTITLE 1,Search Existing Jobs with job_name_prefix
import requests
from requests.auth import HTTPBasicAuth
import datetime

# Extract the job prefix (name without suffix)
job_prefix = job_json_with_deployed_clusters['name'].replace(suffix, '')
print(f"job_prefix: {job_prefix}")

# Function to format job link properly
def format_job_link(databricks_instance, job_id):
    # Remove trailing slashes from the instance URL
    base_url = databricks_instance.rstrip('/')
    return f"{base_url}/#job/{job_id}"

# Function to get current user email using Databricks API
def get_current_user(databricks_instance, databricks_token):
    try:
        response = requests.get(
            f"{databricks_instance}/api/2.0/preview/scim/v2/Me",
            auth=HTTPBasicAuth("token", databricks_token)
        )
        if response.status_code == 200:
            user_data = response.json()
            return user_data.get('emails', [{}])[0].get('value')
    except Exception as e:
        print(f"Error getting current user: {e}")
    return None

# Function to get jobs with pagination using page_token
def get_jobs(databricks_instance, databricks_token, user_email=None):
    all_jobs = []
    limit = 100  # Number of jobs to fetch per request
    page_token = None
    
    while True:
        params = {"limit": limit}
        if page_token:
            params["page_token"] = page_token
        if user_email:
            params["owner_email"] = user_email
        
        try:
            response = requests.get(
                f"{databricks_instance}/api/2.1/jobs/list",
                params=params,
                auth=HTTPBasicAuth("token", databricks_token)
            )
            
            if response.status_code != 200:
                print(f"Failed to list jobs. Status code: {response.status_code}")
                print(f"Response: {response.text}")
                break
            
            result = response.json()
            jobs = result.get('jobs', [])
            all_jobs.extend(jobs)
            
            # Get next page token
            page_token = result.get('next_page_token')
            if not page_token:
                break
                
        except Exception as e:
            print(f"Error fetching jobs: {e}")
            break
    
    # print(f"Fetched {len(all_jobs)} jobs for {'user ' + user_email if user_email else 'all users'}")
    return all_jobs

# First try to get the current user
current_user = get_current_user(databricks_instance, databricks_token)
print(f"Current user: {current_user}")

# Try to get jobs by current user first
if current_user:
    print(f"Fetching jobs for user {current_user}...")
    user_jobs = get_jobs(databricks_instance, databricks_token, current_user)
else:
    user_jobs = []

# If no user jobs found or couldn't get current user, try without filter
if not user_jobs:
    print("No jobs found for the current user or failed to retrieve user information.")
    print("Fetching jobs without user filter...")
    
    # Try to get jobs with prefix filter directly to reduce results
    # This is a workaround since the API doesn't support name filtering directly
    # We'll fetch all jobs but implement early stopping when we find enough matches
    all_jobs = []
    matching_jobs = []
    limit = 100
    page_token = None
    max_pages = 50  # Limit to prevent excessive API calls
    pages_fetched = 0
    
    while pages_fetched < max_pages:
        params = {"limit": limit}
        if page_token:
            params["page_token"] = page_token
        
        try:
            response = requests.get(
                f"{databricks_instance}/api/2.1/jobs/list",
                params=params,
                auth=HTTPBasicAuth("token", databricks_token)
            )
            
            if response.status_code != 200:
                print(f"Failed to list jobs. Status code: {response.status_code}")
                print(f"Response: {response.text}")
                break
            
            result = response.json()
            jobs = result.get('jobs', [])
            all_jobs.extend(jobs)
            
            # Filter for matching jobs in this batch
            batch_matches = [job for job in jobs if job['settings']['name'].startswith(job_prefix)]
            matching_jobs.extend(batch_matches)
            
            # Early stopping if we found some matches
            if len(matching_jobs) >= 5:  # Found enough matches to work with
                print(f"Found {len(matching_jobs)} matching jobs, stopping search.")
                break
                
            # Get next page token
            page_token = result.get('next_page_token')
            if not page_token:
                break
                
            pages_fetched += 1
            # print(f"Fetched page {pages_fetched}, found {len(matching_jobs)} matching jobs so far")
            
        except Exception as e:
            print(f"Error fetching jobs: {e}")
            break
    
    # print(f"Fetched {len(all_jobs)} jobs across {pages_fetched + 1} pages")
    user_jobs = all_jobs
else:
    # Filter jobs that start with the same prefix from user's jobs
    matching_jobs = [job for job in user_jobs if job['settings']['name'].startswith(job_prefix)]

# Process matching jobs
if matching_jobs:
    print(f"Found {len(matching_jobs)} jobs matching prefix '{job_prefix}'")
    
    # Sort by creation time if available, otherwise by job_id
    if 'created_time' in matching_jobs[0]:
        latest_job = max(matching_jobs, key=lambda x: x.get('created_time', 0))
    else:
        latest_job = max(matching_jobs, key=lambda x: x['job_id'])
    
    print(f"Latest job: {latest_job['settings']['name']}")
    
    # Use the format_job_link function to create a properly formatted job link
    job_link = format_job_link(databricks_instance, latest_job['job_id'])
    print(f"Job link: {job_link}")
    
    if use_existing_job:
        print("Using the latest existing job as per user choice.")
        existing_job = latest_job
    else:
        print("Creating a new job as per user choice.")
        today_suffix = datetime.datetime.now().strftime("%Y%m%d")
        job_json_with_deployed_clusters['name'] = f"{job_prefix}_{today_suffix}"
        nsc.deploy_compute(job_json_with_deployed_clusters, run_job=run_job)
else:
    print(f"No existing jobs found with prefix: {job_prefix}")
    today_suffix = datetime.datetime.now().strftime("%Y%m%d")
    job_json_with_deployed_clusters['name'] = f"{job_prefix}_{today_suffix}"
    print(f"Creating new job: {job_json_with_deployed_clusters['name']}")
    nsc.deploy_compute(job_json_with_deployed_clusters, run_job=run_job)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Make sure that task compute includes the `openslide-tools.sh` where it is required 
# MAGIC
# MAGIC After running the deploy code and when the Workflow `[RUNNER]_digital_pathology_{user_initials}{YYYYMMDD}` and the 3 `pathology_14-3-x_{cpu/gpu}_cluster{_w_init}` clusters are set up:     
# MAGIC
# MAGIC <!-- <div style="text-align: center;"> -->
# MAGIC <img src="./imgs/CheckEachCompute_cpuORgpu_cluster_w_init_checkAdvOptions.png" alt="CheckEachCompute_cpuORgpu_cluster_w_init_checkAdvOptions" width="1600" height="600">
# MAGIC <!-- </div> -->
# MAGIC
# MAGIC **Check** within the created compute resource's Advance Options that `openslide-tools.sh` is added to `Init Scripts` for clusters that require them e.g. `pathology_14-3-x_{cpu/gpu}_cluster_w_init` and notebooks that would require their use run independently before running the full workflow.    
# MAGIC
# MAGIC <div style="display: flex; justify-content: center; gap: 20px;">
# MAGIC     <img src="./imgs/CheckEachTask_cluster_w_init_checkAdvOptions.png" alt="CheckEachTask_cluster_w_init_checkAdvOptions" width="800" height="600">      
# MAGIC     <img src="./imgs/gpu_cluster_w_init_checkAdvOptions.png" alt="gpu_cluster_w_init_checkAdvOptions" width="800" height="600">
# MAGIC </div>
# MAGIC
# MAGIC These checks with regards to adding the workspace/volumes `openslide-tools.sh` init script path help ensure that the tasks that require the `openslide` dependencies will complete successfully when workflow is run:    
# MAGIC
# MAGIC
# MAGIC <img src="./imgs/SuccessfulTaskRuns_CheckEachCompute_cpuORgpu_cluster_w_init_checkAdvOptions.png" alt="SuccessfulTaskRuns_CheckEachCompute_cpuORgpu_cluster_w_init_checkAdvOptions" width="1600" height="600">
