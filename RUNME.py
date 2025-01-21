# Databricks notebook source
# MAGIC %md
# MAGIC ## RUNME
# MAGIC This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow: `[RUNNER]_digital_pathology_{user_initials}{YYYYMMDD}` to illustrate the order of execution. Happy exploring! 
# MAGIC 🎉

# COMMAND ----------

# MAGIC %md
# MAGIC ![RUNME_pipeline_setup](/Workspace/Users/may.merkletan@databricks.com/REPOs/digital-pathology/imgs/RUNME_pipeline_setup.png)

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

# COMMAND ----------

# DBTITLE 1,install utils without solacc companion via git branch
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 pyspark>=3.1.2 databricks-sdk>=0.32.0 --quiet --disable-pip-version-check
# MAGIC
# MAGIC dbutils.library.restartPython()

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

# nsc.solacc_path, nsc.username

# COMMAND ----------

# DBTITLE 1,Define Workflow job tasks and cluster resources
job_json = {
            "name": f"[RUNNER]_digital_pathology_{suffix}", ## update where appropriate
            "timeout_seconds": 28800,
            "max_concurrent_runs": 1,
            "tags": {
                "usage": "solacc_testing",
                "group": "HLS",
                "do_not_delete" : True,
                "removeAfter" : "2026-01-31" ## UPDATE | OR include as variable e.g. 30 days from today
            },
            "tasks": [
                {
                    "task_key": "Pathology_00",
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "00-create-annotation-deltalake",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "pathology_14-3-x_cpu_cluster",
                    "timeout_seconds": 0,
                    "email_notifications": {}
                },
                {
                    "task_key": "Pathology_01",
                    "depends_on": [
                        {
                            "task_key": "Pathology_00"
                        }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "01-README",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "pathology_14-3-x_cpu_cluster_w_init",
                    "libraries": [
                        {
                            "pypi": {
                                "package": "openslide-python"
                            }
                        }
                    ],
                    "timeout_seconds": 0,
                    "email_notifications": {}
                },
                {
                    "task_key": "Pathology_02",
                    "depends_on": [
                        {
                            "task_key": "Pathology_01"
                        }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "02-patch-generation",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "pathology_14-3-x_cpu_cluster_w_init",
                    "libraries": [
                        {
                            "pypi": {
                                "package": "openslide-python"
                            }
                        }
                    ],
                    "timeout_seconds": 0,
                    "email_notifications": {}
                },
                {
                    "task_key": "Pathology_03",
                    "depends_on": [
                        {
                            "task_key": "Pathology_02"
                        }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "03-feature-extraction",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "pathology_14-3-x_cpu_cluster_w_init",
                    "libraries": [
                        {
                            "pypi": {
                                "package": "openslide-python"
                            }
                        }
                    ],
                    "timeout_seconds": 0,
                    "email_notifications": {}
                },
                {
                    "task_key": "Pathology_04",
                    "depends_on": [
                        {
                            "task_key": "Pathology_03"
                        }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "04-unsupervised-learning",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "pathology_14-3-x_cpu_cluster_w_init",
                    "libraries": [
                        {
                            "pypi": {
                                "package": "openslide-python"
                            }
                        }
                    ],
                    "timeout_seconds": 0,
                    "email_notifications": {}
                },
                {
                    "task_key": "Pathology_05",
                    "depends_on": [
                        {
                            "task_key": "Pathology_02"
                        }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "05-training",
                        "source": "WORKSPACE"
                    },
                    "job_cluster_key": "pathology_14-3-x_gpu_cluster_w_init",
                    "libraries": [
                        {
                            "pypi": {
                                "package": "openslide-python"
                            }
                        }
                    ],
                    "timeout_seconds": 0,
                    "email_notifications": {}
                },
                {
                    "task_key": "Pathology_06",
                    "depends_on": [
                        {
                            "task_key": "Pathology_05"
                        }
                    ],
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": "06-metastasis-heatmap",
                        "source": "WORKSPACE"
                    },                
                    "job_cluster_key": "pathology_14-3-x_gpu_cluster_w_init",
                    "libraries": [
                        {
                            "pypi": {
                                "package": "openslide-python"
                            }
                        }
                    ],
                    "timeout_seconds": 0,
                    "email_notifications": {}
                }
            ],
            "job_clusters": [
                {
                    "job_cluster_key": "pathology_14-3-x_cpu_cluster",
                    "new_cluster": {
                        "spark_version": "14.3.x-cpu-ml-scala2.12", ## update where appropriate + corresponding job_clusters_key
                        "num_workers": 2,
                        "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"}
                    }
                },
                {
                    "job_cluster_key": "pathology_14-3-x_cpu_cluster_w_init",
                    "new_cluster": {
                        "spark_version": "14.3.x-cpu-ml-scala2.12", ## update where appropriate + corresponding job_clusters_key
                        "num_workers": 2,
                        "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"},
                        "init_scripts": [
                            {
                                "workspace": {
                                    "destination": f"{nsc.solacc_path}/openslide-tools.sh"
                                }#,
                                # "volumes":{
                                #     "destination": "/Volumes/dbdemos/digital_pathology/files/openslide-tools.sh" #optional to workspace init.sh file path
                                #            }
                            }
                        ]
                    }
                },
                {
                    "job_cluster_key": "pathology_14-3-x_gpu_cluster_w_init",
                    "new_cluster": {
                        "spark_version": "14.3.x-gpu-ml-scala2.12", ## update where appropriate + corresponding job_clusters_key
                        "num_workers": 2, #1,
                        "node_type_id": {"AWS": "g4dn.4xlarge", "MSA": "Standard_NC6s_v3", "GCP": "a2-highgpu-1g"},
                        "init_scripts": [
                            {
                                "workspace": {
                                    "destination": f"{nsc.solacc_path}/openslide-tools.sh"
                                }#,
                                # "volumes":{
                                #     "destination": "/Volumes/dbdemos/digital_pathology/files/openslide-tools.sh" #optional to workspace init.sh file path
                                #            }
                            }
                        ]
                    }
                }
            ]
        }


# COMMAND ----------

# DBTITLE 1,Deploy Workflow Pipeline
## Define run_job boolean Widget when deploy_compute() is executed; by default resources will be set up without running job pipeline:
dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"

nsc.deploy_compute(job_json, run_job=run_job)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Make sure that task compute includes the `openslide-tools.sh` where it is required 
# MAGIC
# MAGIC After running the deploy code and when the Workflow `[RUNNER]_digital_pathology_{user_initials}{YYYYMMDD}` and the 3 `pathology_13-4-x_{cpu/gpu}_cluster{_w_init}` clusters are set up:     
# MAGIC
# MAGIC <!-- <div style="text-align: center;"> -->
# MAGIC <img src="/Workspace/Users/may.merkletan@databricks.com/REPOs/digital-pathology/imgs/CheckEachCompute_cpuORgpu_cluster_w_init_checkAdvOptions.png" alt="CheckEachCompute_cpuORgpu_cluster_w_init_checkAdvOptions" width="1600" height="600">
# MAGIC <!-- </div> -->
# MAGIC
# MAGIC **Check** within the created compute resource's Advance Options that `openslide-tools.sh` is added to `Init Scripts` for clusters that require them e.g. `pathology_13-4-x_{cpu/gpu}_cluster_w_init` and notebooks that would require their use run independently before running the full workflow.    
# MAGIC
# MAGIC <div style="display: flex; justify-content: center; gap: 20px;">
# MAGIC     <img src="/Workspace/Users/may.merkletan@databricks.com/REPOs/digital-pathology/imgs/CheckEachTask_cluster_w_init_checkAdvOptions.png" alt="CheckEachTask_cluster_w_init_checkAdvOptions" width="800" height="600">      
# MAGIC     <img src="/Workspace/Users/may.merkletan@databricks.com/REPOs/digital-pathology/imgs/gpu_cluster_w_init_checkAdvOptions.png" alt="gpu_cluster_w_init_checkAdvOptions" width="800" height="600">
# MAGIC </div>
# MAGIC
# MAGIC <!-- <div style="display: flex; gap: 10px;">
# MAGIC     <img src="/Workspace/Users/may.merkletan@databricks.com/REPOs/digital-pathology/imgs/CheckEachTask_cluster_w_init_checkAdvOptions.png" alt="CheckEachTask_cluster_w_init_checkAdvOptions" width="800" height="600">   
# MAGIC     <img src="/Workspace/Users/may.merkletan@databricks.com/REPOs/digital-pathology/imgs/gpu_cluster_w_init_checkAdvOptions.png" alt="gpu_cluster_w_init_checkAdvOptions" width="800" height="600">
# MAGIC </div> -->
# MAGIC
# MAGIC These checks with regards to adding the workspace/volumes `openslide-tools.sh` init script path help ensure that the tasks that require the `openslide` dependencies will complete successfully when workflow is run:    
# MAGIC
# MAGIC <!-- <img src="/Workspace/Users/may.merkletan@databricks.com/REPOs/digital-pathology/imgs/CheckEachTaskNCompute_cpu_cluster_w_init_checkAdvOptions.png" alt="CheckEachTaskNCompute_cpu_cluster_w_init_checkAdvOptions" width="600" height="400"> -->
# MAGIC
# MAGIC <img src="/Workspace/Users/may.merkletan@databricks.com/REPOs/digital-pathology/imgs/SuccessfulTaskRuns_CheckEachCompute_cpuORgpu_cluster_w_init_checkAdvOptions.png" alt="SuccessfulTaskRuns_CheckEachCompute_cpuORgpu_cluster_w_init_checkAdvOptions" width="1600" height="600">
