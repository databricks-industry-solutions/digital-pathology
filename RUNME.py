# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow to create a Workflow DAG and illustrate the order of execution. Feel free to interactively run notebooks with the cluster or to run the Workflow to see how this solution accelerator executes. Happy exploring!
# MAGIC 
# MAGIC The pipelines, workflows and clusters created in this script are user-specific, so you can alter the workflow and cluster via UI without affecting other users. Running this script again after modification resets them.
# MAGIC 
# MAGIC **Note**: If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators sometimes require the user to set up additional cloud infra or data access, for instance. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-industry-solutions/notebook-solution-companion git+https://github.com/databricks-academy/dbacademy-rest git+https://github.com/databricks-academy/dbacademy-gems 

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
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_D3_v2", "GCP": "n1-highmem-4"}
                }
            },
            {
                "job_cluster_key": "pathology_cluster_w_init",
                "new_cluster": {
                    "spark_version": "10.4.x-cpu-ml-scala2.12",
                    "num_workers": 2,
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_D3_v2", "GCP": "n1-highmem-4"},
                    "init_scripts": [
                        {
                            "dbfs": {
                                "destination": "dbfs://tmp/openslide/openslide-tools.sh"
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
                                "destination": "dbfs://tmp/openslide/openslide-tools.sh"
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


