# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow to illustrate the order of execution. Happy exploring! 
# MAGIC 🎉
# MAGIC 
# MAGIC **Steps**
# MAGIC 1. Simply attach this notebook to a cluster with DBR 11.0 and above, and hit Run-All for this notebook. A multi-step job and the clusters used in the job will be created for you and hyperlinks are printed on the last block of the notebook. 
# MAGIC 
# MAGIC 2. Run the accelerator notebooks: Feel free to explore the multi-step job page and **run the Workflow**, or **run the notebooks interactively** with the cluster to see how this solution accelerator executes. 
# MAGIC 
# MAGIC     2a. **Run the Workflow**: Navigate to the Workflow link and hit the `Run Now` 💥. 
# MAGIC   
# MAGIC     2b. **Run the notebooks interactively**: Attach the notebook with the cluster(s) created and execute as described in the `job_json['tasks']` below.
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

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

job_json = {
        "timeout_seconds": 14400,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "HLS"
        },
        "tasks": [
            {
                "job_cluster_key": "pathology_cluster",
                "notebook_task": {
                    "notebook_path": f"00-create-annotation-deltalake",
                    "base_parameters": {}
                },
                "task_key": "Pathology_00"
            },  
            {
                "job_cluster_key": "pathology_cluster_w_init",
                "notebook_task": {
                    "notebook_path": f"01-README",
                    "base_parameters": {}
                },
                "libraries": [
                    {
                        "pypi": {
                            "package": "openslide-python"
                        }
                    }
                ],
                "task_key": "Pathology_01",
                "depends_on": [
                    {
                        "task_key": "Pathology_00"
                    }
                ]
            },
            {
                "job_cluster_key": "pathology_cluster_w_init",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"02-patch-generation"
                },
                "libraries": [
                    {
                        "pypi": {
                            "package": "openslide-python"
                        }
                    }
                ],
                "task_key": "Pathology_02",
                "depends_on": [
                    {
                        "task_key": "Pathology_01"
                    }
                ]
            },
            {
                "job_cluster_key": "pathology_cluster_w_init",
                "notebook_task": {
                    "notebook_path": f"03-feature-extraction"
                },
                "libraries": [
                    {
                        "pypi": {
                            "package": "openslide-python"
                        }
                    }
                ],
                "task_key": "Pathology_03",
                "depends_on": [
                    {
                        "task_key": "Pathology_02"
                    }
                ]
            },
            {
                "job_cluster_key": "pathology_cluster_w_init",
                "notebook_task": {
                    "notebook_path": f"04-unsupervised-learning"
                },
                "libraries": [
                    {
                        "pypi": {
                            "package": "openslide-python"
                        },
                    }
                ],
                "task_key": "Pathology_04",
                "depends_on": [
                    {
                        "task_key": "Pathology_03"
                    }
                ]
            },
             {
                 "job_cluster_key": "pathology_gpu_cluster_w_init",
                 "notebook_task": {
                     "notebook_path": f"05-training"
                 },
                 "libraries": [
                    {
                        "pypi": {
                            "package": "openslide-python"
                        }
                    }
                ],
                 "task_key": "Pathology_05",
                 "depends_on": [
                     {
                         "task_key": "Pathology_02"
                     }
                 ]
             },
             {
                 "job_cluster_key": "pathology_cluster_w_init",
                 "notebook_task": {
                     "notebook_path": f"06-metastasis-heatmap"
                 },
                 "libraries": [
                    {
                        "pypi": {
                            "package": "openslide-python"
                        }
                    }
                ],
                 "task_key": "Pathology_06",
                 "depends_on": [
                     {
                         "task_key": "Pathology_05"
                     }
                 ]
             }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "pathology_cluster",
                "new_cluster": {
                    "spark_version": "10.4.x-cpu-ml-scala2.12",
                    "num_workers": 2,
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"}
                }
            },
            {
                "job_cluster_key": "pathology_cluster_w_init",
                "new_cluster": {
                    "spark_version": "10.4.x-cpu-ml-scala2.12",
                    "num_workers": 2,
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"},
                    "init_scripts": [
                        {
                            "dbfs": {
                                "destination": "dbfs:/tmp/openslide/openslide-tools.sh"
                            }
                        }
                    ]
                }
            },
            {
                "job_cluster_key": "pathology_gpu_cluster_w_init",
                "new_cluster": {
                    "spark_version": "10.4.x-gpu-ml-scala2.12",
                    "num_workers": 1,
                    "node_type_id": {"AWS": "g4dn.4xlarge", "MSA": "Standard_NC6s_v3", "GCP": "a2-highgpu-1g"},
                    "init_scripts": [
                        {
                            "dbfs": {
                                "destination": "dbfs:/tmp/openslide/openslide-tools.sh"
                            }
                        }
                    ]
                }
            }
        ]
    }

# COMMAND ----------

dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(job_json, run_job=run_job)

# COMMAND ----------



# COMMAND ----------


