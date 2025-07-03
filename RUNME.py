# Databricks notebook source
# MAGIC %md
# MAGIC # RUNME
# MAGIC This notebook sets up the companion cluster(s) to run the solution accelerator.    
# MAGIC
# MAGIC It also creates the Workflow: `[RUNNER]_digital_pathology_{user_initials}{YYYYMMDD}` to illustrate the order of execution.   
# MAGIC
# MAGIC Happy exploring! 
# MAGIC 🎉

# COMMAND ----------

# MAGIC %md
# MAGIC ![RUNME_pipeline_setup](./imgs/RUNME_pipeline_setup.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Steps
# MAGIC 1. **Simply attach this notebook to a cluster** (e.g. ML DBR 14.3LTS is recommended) **and hit `Run-All` for this notebook** -- it will set up default parameters and cluster resources required for the multi-step workflow and start a run of the tasks as specified in the numbered notebooks. Hyperlinks to the clusters, job and run will be printed under the section **`Deploy Compute Resources + Job`**.       
# MAGIC
# MAGIC    a)  By default a run of the deployed job will start automatically. **You can choose to examine the resources first before initializing the job run manually from the workflow UI page.**    
# MAGIC    
# MAGIC    To do so, you update the **`deploy_digital_pathology_job`** function call specifying **`run_job=False`** before executing `Run-All` on this notebook:    
# MAGIC         
# MAGIC     ``` 
# MAGIC     deploy_digital_pathology_job(suffix=suffix, reuse=True, run_job=False, workspace_url=workspace_url)
# MAGIC     ```    
# MAGIC
# MAGIC    b) For clusters with **`*_w_init` suffixes**:    Check cluster's Advance Options to make sure the the `openslide-tools.sh` workspace file path is added to the `Init scripts` -- _This is best checked when you DO NOT `auto-run` i.e. the job was deployed with **`run_job=False`**; Check before you run the notebooks or workflow_ (see screenshots under `CHECKS`)
# MAGIC
# MAGIC 2. **Run the accelerator notebooks:**    
# MAGIC Feel free to explore the multi-step job page and **run the Workflow**, or **run the notebooks interactively** by attaching the associated cluster to see how this solution accelerator executes. 
# MAGIC
# MAGIC     2a. **Run the Workflow**: Navigate to the Workflow link and hit the `Run Now` 💥. 
# MAGIC   
# MAGIC     2b. **Run the notebooks interactively**: Attach the notebook with the cluster(s) created and execute as described in the **`job_json['tasks']`** below.
# MAGIC
# MAGIC 3. **Clean up resources:**    
# MAGIC When you are done exploring the solution accelerator you can run:    
# MAGIC     ``` nsc.cleanup_digital_pathology_resources(result, confirm=True) ```
# MAGIC
# MAGIC ---    
# MAGIC **Prerequisites:** 
# MAGIC 1. You need to have cluster creation permissions in this workspace.
# MAGIC
# MAGIC 2. In case the environment has cluster-policies that interfere with automated deployment, you may need to manually create the cluster in accordance with the workspace cluster policy. The `job_json` definition below still provides valuable information about the configuration these series of notebooks should run with. 

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:**
# MAGIC
# MAGIC <!-- 1. The workflow job and clusters created in this script are not user-specific if you use the default catalog, project name, and without adding the user-defined `suffix`, which you could omit. However, if you do omit using the `suffix`, keep in mind that rerunning this script again after modification will update these resources for other users also sharing the same workspace and default settings. -->
# MAGIC
# MAGIC <!-- 2.  -->
# MAGIC If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators may require the user to set up additional cloud infra or secrets to manage credentials. 
# MAGIC
# MAGIC **Jan2025_Update NB:** Recent updates to repository dependency `https://github.com/databricks-industry-solutions/notebook-solution-companion` broke the existing code process. Hidden/Commented code in Markdown reflects prior version.     
# MAGIC
# MAGIC <!-- ```
# MAGIC #### (original) -- this previous approach now has a recent code-change / PR induced Error
# MAGIC
# MAGIC ## Install util packages (separate cell)
# MAGIC # %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html pyspark>=3.1.2 --quiet --disable-pip-version-check
# MAGIC # dbutils.library.restartPython()
# MAGIC
# MAGIC
# MAGIC ## Import solution accelerator companion modules/tools (separate cell)
# MAGIC # from solacc.companion import NotebookSolutionCompanion
# MAGIC # nsc = NotebookSolutionCompanion()
# MAGIC ``` -->
# MAGIC    
# MAGIC **Workaround Solution Provided:** ` solacc/companion/_init_.py` from a previous [`Pull Request`](https://github.com/databricks-industry-solutions/notebook-solution-companion/blob/f7e381d77675b29c2d3f9d377a528ceaf2255f23/solacc/companion/__init__.py) is copied to  `solacc_companion_init` in the workspace and `%run` to access `NotebookSolutionCompanion()` 
# MAGIC
# MAGIC **July2025_Update NB:** Included digital pathology solacc specific modules to instantiated NotebookSolutionCompanion to help deploy the defined clusters first and then associate them with the workflow tasks and initialize a run of the workflow. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install Dependencies

# COMMAND ----------

# DBTITLE 1,install utils without solacc companion via git branch
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 pyspark>=3.1.2 databricks-sdk>=0.32.0 --quiet --disable-pip-version-check
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Instantiate `NotebookSolutionCompanion` + Digital Pathology Sol.Acc. Notebook Modules 

# COMMAND ----------

# MAGIC %run ./solacc_companion_digipath

# COMMAND ----------

# MAGIC %md
# MAGIC ### Specify Configs.
# MAGIC - UC Catalog | Project "Schema" 
# MAGIC - Derive User + Workspace Info.
# MAGIC - Job Workflow
# MAGIC - Job Config.

# COMMAND ----------

# DBTITLE 1,reset widgets  manually if needed
# dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Default Catalog and Schema
# DEFAULT config parameters:
dbutils.widgets.text("catalog_name", "dbdemos", "UC Catalog Name")
dbutils.widgets.text("schema_name", "digital_pathology", "UC Schema Name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Users can modify config. through widgets
# MAGIC **If you wish to use a preferred/designated `catalog` and `schema`:** PLEASE update the values in the above cell / widgets manually before running the following cells. 

# COMMAND ----------

# DBTITLE 1,Extract widget values
# Extract Default config
from config.default_config import UC_CONFIG

UC_CONFIG["catalog_name"] = dbutils.widgets.get("catalog_name")
UC_CONFIG["schema_name"] = dbutils.widgets.get("schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC **IF Config. modifications were made, following extracted widget values should update**

# COMMAND ----------

# DBTITLE 1,check -- default / updated config
print(f"Using UC catalog: {UC_CONFIG['catalog_name']}")
print(f"Using UC schema: {UC_CONFIG['schema_name']}")

# COMMAND ----------

# DBTITLE 1,Update runme_config
# Save updated config
with open("./config/runme_config.json", "w") as f:
  import json
  json.dump(UC_CONFIG, f)
  
# Confirm final configuration
print("Final UC Configuration:")
print(json.dumps(UC_CONFIG, indent=2))

# COMMAND ----------

# DBTITLE 1,Get User  Info.
## This is so that each user can set up their own workflow pipeline
from datetime import datetime

# get user info.
user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

suffix = ''.join([s[0] for s in user.split('@')[0].split('.')]) + '_' + datetime.now().strftime("%Y%m%d")
suffix

# COMMAND ----------

# DBTITLE 1,Retrieve Workspace URL
# Set your workspace URL
workspace_url = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
print(f"Workspace URL: {workspace_url} (e.g., https://<databricks-instance>)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Job Config.

# COMMAND ----------

# DBTITLE 1,config | job_json
# Function to create the digital pathology job configuration
def create_digital_pathology_job_config(suffix=""):
    """
    Creates a job configuration with properly configured job clusters including init scripts
    """
    # Get the username for the single user mode
    username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    
    # Define cluster keys that will be used consistently
    base_cpu_cluster_key = f"pathology_14-3-x_cpu_cluster_{suffix}"
    cpu_init_cluster_key = f"pathology_14-3-x_cpu_cluster_w_init_{suffix}"
    gpu_init_cluster_key = f"pathology_14-3-x_gpu_cluster_w_init_{suffix}"
    
    # Define the absolute path for the init script
    init_script_path = f"{nsc.solacc_path}/openslide-tools.sh"
    
    # Define the job configuration
    job_json = {
        "name": f"[RUNNER]_digital_pathology_{suffix}",
        "timeout_seconds": 28800,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "HLS",
            "do_not_delete": True,
            "removeAfter": "2026-01-31"
        },
        "job_clusters": [
            # Base CPU cluster without init scripts
            {
                "job_cluster_key": base_cpu_cluster_key,
                "new_cluster": {
                    "spark_version": "14.3.x-cpu-ml-scala2.12",
                    "num_workers": 2,
                    "node_type_id": {
                        "AWS": "i3.xlarge", 
                        "MSA": "Standard_DS3_v2", 
                        "GCP": "n1-highmem-4"
                    },
                    "data_security_mode": "SINGLE_USER",
                    "single_user_name": username
                }
            },
            # CPU cluster with init scripts
            {
                "job_cluster_key": cpu_init_cluster_key,
                "new_cluster": {
                    "spark_version": "14.3.x-cpu-ml-scala2.12",
                    "num_workers": 2,
                    "node_type_id": {
                        "AWS": "i3.xlarge", 
                        "MSA": "Standard_DS3_v2", 
                        "GCP": "n1-highmem-4"
                    },
                    "data_security_mode": "SINGLE_USER",
                    "single_user_name": username,
                    "init_scripts": [
                        {
                            "workspace": {
                                "destination": init_script_path
                            }
                        }
                    ]
                }
            },
            # GPU cluster with init scripts
            {
                "job_cluster_key": gpu_init_cluster_key,
                "new_cluster": {
                    "spark_version": "14.3.x-gpu-ml-scala2.12",
                    "num_workers": 2,
                    "node_type_id": {
                        "AWS": "g4dn.4xlarge",
                        "MSA": "Standard_NC4as_T4_v3",
                        "GCP": "n1-standard-8-nvidia-tesla-t4"
                    },
                    "data_security_mode": "SINGLE_USER",
                    "single_user_name": username,
                    "init_scripts": [
                        {
                            "workspace": {
                                "destination": init_script_path
                            }
                        }
                    ]
                }
            }
        ],
        "tasks": [
            {
                "task_key": "Pathology_00",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "00-create-annotation-UC",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": base_cpu_cluster_key,
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
                "job_cluster_key": cpu_init_cluster_key,
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
                "job_cluster_key": cpu_init_cluster_key,
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
                "job_cluster_key": cpu_init_cluster_key,
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
                "job_cluster_key": cpu_init_cluster_key,
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
                "job_cluster_key": gpu_init_cluster_key,
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
                "job_cluster_key": gpu_init_cluster_key,
                "libraries": [{"pypi": {"package": "openslide-python"}}],
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ]
    }
    
    return job_json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Compute Resources + Job (+ Initialize Task Run)
# MAGIC
# MAGIC - Click the links to explore Job, Tasks, Run, and Clusters

# COMMAND ----------

# DBTITLE 1,Deploy (ReUse) Resources +/- Run Task(s)
## Deploy, reuse existing resources, and RUN the job 
result = deploy_digital_pathology_job(suffix=suffix, reuse=True, run_job=True, workspace_url=workspace_url)
# print(f"Deployed with Resource ReUse and Running Job: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC <!-- ### There are different ways one can deploy the the workflow job. 
# MAGIC
# MAGIC By default we will setup 3 different compute clusters and initialize the workflow.
# MAGIC
# MAGIC However, the other options as as follows:
# MAGIC
# MAGIC   **[Required] Extract workspace url**
# MAGIC   ```
# MAGIC   # workspace_url = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
# MAGIC   # print(workspace_url)
# MAGIC   ```
# MAGIC
# MAGIC
# MAGIC   **Deploy with default options (reuse existing resources, don't run the job)**
# MAGIC   ```
# MAGIC   # result = deploy_digital_pathology_job(suffix=suffix, run_job=False, workspace_url=workspace_url)
# MAGIC   # print(f"Deployed resources: {result}")
# MAGIC   ```
# MAGIC
# MAGIC   **Deploy and create new resources even if they already exist**
# MAGIC   ```
# MAGIC   # result = deploy_digital_pathology_job(suffix=f"{new_suffix}", reuse=False, workspace_url=workspace_url)
# MAGIC   # print(f"Deployed new resources: {result}")
# MAGIC   ```
# MAGIC
# MAGIC   **Deploy, reuse existing resources, and DO NOT RUN the job**
# MAGIC   ```
# MAGIC   # result = deploy_digital_pathology_job(suffix=suffix, reuse=True, run_job=False, workspace_url=workspace_url)
# MAGIC   # print(f"Deployed with Resource ReUse and DO NOT RUN Job: {result}")
# MAGIC   ```
# MAGIC
# MAGIC   **Deploy, reuse existing resources, and RUN the job**
# MAGIC   ```
# MAGIC   result = deploy_digital_pathology_job(suffix=suffix, reuse=True, run_job=True, workspace_url=workspace_url)
# MAGIC   # print(f"Deployed with Resource ReUse and Running Job: {result}")
# MAGIC   ```
# MAGIC
# MAGIC   You can modify the Deployment code occordingly -->

# COMMAND ----------

# MAGIC %md
# MAGIC ### CHECKs 
# MAGIC
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

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Review Deployment Info.

# COMMAND ----------

# DBTITLE 1,deployment results
result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Up 
# MAGIC
# MAGIC **When you are ready to exit the Digital Pathology Solution Accelerator, we can clean up the compute resources and job deployment.**
# MAGIC
# MAGIC _Note: The files (Tables, Volumes, Models/Functions) generated in the Unity Catalog's Schema will remain unless you delete them manually as well._    
# MAGIC _You can Drop these via [SQL `DROP` commands](https://docs.databricks.com/aws/en/sql/language-manual/#ddl-statements)_.   

# COMMAND ----------

# DBTITLE 1,cleanup_digital_pathology_resources
## If you have the results object directly from the deployment

# Clean up without confirmation prompt
cleanup_status = nsc.cleanup_digital_pathology_resources(result, confirm=True)
print(f"\nCleanup status: {cleanup_status}")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
