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
# MAGIC # TO UPDATE
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Simply attach this notebook to a cluster (e.g. ML DBR 14.3LTS is recommended) and hit `Run-All` for this notebook. A multi-step job and the clusters used in the job will be created for you and hyperlinks are printed on the last block of the notebook. By Default a run of the deployed job will start automatically. You can prevent this in the call of    
# MAGIC     ``` deploy_digital_pathology_job(suffix=suffix, reuse=True, run_job=False, workspace_url=workspace_url)```    
# MAGIC before executing `Run-All` on this notebook.
# MAGIC
# MAGIC 2. For clusters with **`*_w_init` suffixes**: Check cluster's Advance Options to make sure the the `openslide-tools.sh` workspace file path is added to the `Init scripts` -- _This is best checked when you DO NOT auto-run the deployed job ```run_job=False```; Check before you run the notebooks or workflow_ (see step 3.)
# MAGIC
# MAGIC 3. Run the accelerator notebooks: Feel free to explore the multi-step job page and **run the Workflow**, or **run the notebooks interactively** with the cluster to see how this solution accelerator executes. 
# MAGIC
# MAGIC     3a. **Run the Workflow**: Navigate to the Workflow link and hit the `Run Now` 💥. 
# MAGIC   
# MAGIC     3b. **Run the notebooks interactively**: Attach the notebook with the cluster(s) created and execute as described in the `job_json['tasks']` below.
# MAGIC
# MAGIC 4. Clean up resources: When you are done exploring the solution accelerator you can run:    
# MAGIC     ``` nsc.cleanup_digital_pathology_resources(result, confirm=True) ```
# MAGIC
# MAGIC **Prerequisites** 
# MAGIC 1. You need to have cluster creation permissions in this workspace.
# MAGIC
# MAGIC 2. In case the environment has cluster-policies that interfere with automated deployment, you may need to manually create the cluster in accordance with the workspace cluster policy. The `job_json` definition below still provides valuable information about the configuration these series of notebooks should run with. 
# MAGIC
# MAGIC **Notes**
# MAGIC 1. The pipelines, workflows and clusters created in this script are not user-derived if you use the default catalog, project name, and without adding the user-defined suffix. Keep in mind that rerunning this script again after modification resets them for other users too.
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

# MAGIC %md
# MAGIC ### Instantiate `NotebookSolutionCompanion` + Digital Pathology Sol.Acc. Notebook Modules 

# COMMAND ----------

# MAGIC %run ./solacc_companion_digipath

# COMMAND ----------

# DBTITLE 1,Run copied solacc companion module from workspace path
# %run ./solacc_companion_init

# COMMAND ----------

# DBTITLE 1,Access Modules within NotebookSolutionCompanion()
# nsc = NotebookSolutionCompanion()

# COMMAND ----------

# DBTITLE 1,Add DigiPath methods to the NotebookSolutionCompanion instance
# ## To consider being included within NotebookSolutionCompanion (nsc) in the future -- for now this is a workaround

# # Methods to add to the current instance of NotebookSolutionCompanion (nsc)
# def _nsc_deploy_job_clusters(self, job_clusters_json, reuse=True, wait=0):
#     """
#     Deploy only the job clusters defined in the job configuration.
#     Returns a dictionary mapping job_cluster_keys to their corresponding cluster IDs.
#     """
#     job_cluster_map = {}
    
#     if not job_clusters_json:
#         print("No job clusters to deploy")
#         return job_cluster_map
    
#     print(f"🚀 Deploying {len(job_clusters_json)} job clusters...")
    
#     for job_cluster_params in job_clusters_json:
#         jck = job_cluster_params["job_cluster_key"]
#         if "new_cluster" in job_cluster_params:
#             # Convert job cluster config to interactive cluster config
#             cluster_params = self.convert_job_cluster_to_cluster(job_cluster_params)
            
#             # Set auto-termination to 10 minutes
#             cluster_params["autotermination_minutes"] = 10
            
#             # Apply cloud-specific customization to the cluster parameters
#             cluster_params = self.customize_cluster_json(cluster_params)
            
#             # Check if cluster with this name already exists
#             cluster_name = cluster_params["cluster_name"]
#             clusters = self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/clusters/list")["clusters"]
#             clusters_matched = list(filter(lambda cluster: cluster_name == cluster["cluster_name"], clusters))
#             cluster_exists = len(clusters_matched) > 0
            
#             if cluster_exists and reuse:
#                 # Reuse the existing cluster
#                 cluster_id = clusters_matched[0]["cluster_id"]
#                 print(f"✅ Reusing existing cluster '{cluster_name}' with ID: {cluster_id}")
#             else:
#                 if cluster_exists and not reuse:
#                     # Delete the existing cluster first
#                     cluster_id = clusters_matched[0]["cluster_id"]
#                     print(f"🗑️ Deleting existing cluster '{cluster_name}' with ID: {cluster_id}")
#                     self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/clusters/permanent-delete", {"cluster_id": cluster_id})
#                     time.sleep(5)  # Wait a bit for the deletion to take effect
                
#                 # Create or update the cluster
#                 jcid = self.create_or_update_cluster_by_name(cluster_params)
                
#                 # Set ACL for the cluster
#                 self.set_acl_for_cluster(jcid)
                
#                 cluster_id = jcid
            
#             # Store the mapping
#             job_cluster_map[jck] = cluster_id
            
#             # Get libraries for this job cluster
#             if hasattr(self, 'job_input_json'):
#                 jcl = self.get_library_list_for_cluster(self.job_input_json, jck)
#                 if jcl:
#                     self.start_cluster(cluster_id)
#                     self.install_libraries(cluster_id, jcl)
    
#     time.sleep(wait)
#     print(f"✅ Successfully deployed {len(job_cluster_map)} job clusters")
#     return job_cluster_map


# def _nsc_deploy_job_with_existing_clusters(self, job_json, cluster_map, reuse=True, run_job=False):
#     """
#     Deploy a job using existing clusters instead of job clusters.
#     """
#     # Create a deep copy of the job configuration
#     job_params = copy.deepcopy(job_json)
    
#     # Customize the notebook paths in the job JSON
#     for i, task in enumerate(job_params.get("tasks", [])):
#         if "notebook_task" in task:
#             notebook_name = task["notebook_task"]["notebook_path"]
#             if not notebook_name.startswith(self.solacc_path):
#                 task["notebook_task"]["notebook_path"] = f"{self.solacc_path}/{notebook_name}"
    
#     # Set the job name if not already set
#     if "name" not in job_params:
#         job_params["name"] = self.job_name
    
#     # Add access control list
#     job_params["access_control_list"] = [
#         {
#             "group_name": "users",
#             "permission_level": "CAN_MANAGE_RUN"
#         }
#     ]
    
#     # Remove the job_clusters section
#     if "job_clusters" in job_params:
#         del job_params["job_clusters"]
    
#     # Replace job_cluster_key with existing_cluster_id in tasks
#     for task in job_params.get("tasks", []):
#         if "job_cluster_key" in task and task["job_cluster_key"] in cluster_map:
#             task["existing_cluster_id"] = cluster_map[task["job_cluster_key"]]
#             del task["job_cluster_key"]
    
#     # Check if job with this name already exists
#     job_name = job_params["name"]
#     job_found = list(self.w.jobs.list(name=job_name))
#     job_exists = len(job_found) > 0
    
#     if job_exists and reuse:
#         # Reuse the existing job by updating it
#         job_id = job_found[0].job_id
#         print(f"✅ Updating existing job '{job_name}' with ID: {job_id}")
#         reset_job_settings = JobSettings().from_dict(job_params)
#         self.w.jobs.reset(job_id, reset_job_settings)
#     else:
#         if job_exists and not reuse:
#             # Delete the existing job first
#             job_id = job_found[0].job_id
#             print(f"🗑️ Deleting existing job '{job_name}' with ID: {job_id}")
#             self.w.jobs.delete(job_id=job_id)
#             time.sleep(5)  # Wait a bit for the deletion to take effect
        
#         # Create a new job
#         create_job_request = CreateJob().from_dict(job_params)
#         job_id = self.w.jobs.create(request=create_job_request).job_id
#         print(f"✅ Created new job '{job_name}' with ID: {job_id}")
    
#     # Store the job ID for future reference
#     self.job_id = job_id
    
#     # Run the job if requested
#     run_id = None
#     if run_job:
#         print(f"🚀 Running job '{job_name}' with ID: {job_id}")
#         # Use the Databricks SDK directly instead of self.run_job()
#         run_response = self.w.jobs.run_now(job_id=job_id)
#         run_id = run_response.run_id
    
#     return job_id, run_id


# def _nsc_deploy_digital_pathology_job(self, job_json, suffix="", reuse=True, run_job=False):
#     """
#     Deploy a digital pathology job using the two-step approach:
#     1. Deploy the job clusters separately
#     2. Deploy the job with references to the deployed clusters
#     """
#     # Store the job JSON for use in other methods
#     self.job_input_json = copy.deepcopy(job_json)
    
#     # Step 1: Deploy the job clusters separately
#     job_clusters = job_json.get("job_clusters", [])
#     cluster_map = self._deploy_job_clusters(job_clusters, reuse=reuse)
    
#     # Step 2: Deploy the job with references to the deployed clusters
#     job_id, run_id = self._deploy_job_with_existing_clusters(job_json, cluster_map, reuse=reuse, run_job=run_job)
    
#     # Get the job name for reference
#     job_name = job_json.get("name", "digital-pathology-job")
    
#     # Collect cluster names and IDs
#     cluster_details = {}
#     for key, cluster_id in cluster_map.items():
#         # Get the cluster name from the job_clusters configuration
#         cluster_name = key
#         for jc in job_clusters:
#             if jc["job_cluster_key"] == key:
#                 # Use the cluster name from the job_cluster_key if available
#                 cluster_name = jc["job_cluster_key"]
#                 break
        
#         cluster_details[key] = {
#             "cluster_id": cluster_id,
#             "cluster_name": cluster_name
#         }
    
#     # Return comprehensive result
#     result = {
#         "job_id": job_id,
#         "job_name": job_name,
#         "clusters": cluster_map,
#         "cluster_details": cluster_details
#     }
    
#     if run_id:
#         result["run_id"] = run_id
    
#     return result


# # Add these methods to the existing instance of NotebookSolutionCompanion (nsc)
# import types
# nsc._deploy_job_clusters = types.MethodType(_nsc_deploy_job_clusters, nsc)
# nsc._deploy_job_with_existing_clusters = types.MethodType(_nsc_deploy_job_with_existing_clusters, nsc)
# nsc._deploy_digital_pathology_job = types.MethodType(_nsc_deploy_digital_pathology_job, nsc)

# print("✅ Added new methods to existing instance of NotebookSolutionCompanion (nsc)")

# COMMAND ----------

# DBTITLE 1,+ cleanup_digital_pathology_resources --> nsc
# # Define the cleanup method
# def _nsc_cleanup_digital_pathology_resources(self, results, confirm=True):
#     """
#     Clean up resources (job and clusters) created during digital pathology job deployment
    
#     Parameters:
#     -----------
#     results : dict
#         The results dictionary returned by deploy_digital_pathology_job
#     confirm : bool, optional
#         Whether to ask for confirmation before deleting resources (default: True)
    
#     Returns:
#     --------
#     dict
#         Dictionary with deletion status for each resource
#     """
#     if not isinstance(results, dict):
#         print("❌ Invalid results object. Please provide the dictionary returned by deploy_digital_pathology_job.")
#         return {"status": "failed", "reason": "Invalid results object"}
    
#     # Extract resource IDs
#     job_id = results.get('job_id')
#     clusters = results.get('clusters', {})
    
#     if not job_id and not clusters:
#         print("❌ No resources to clean up. The results dictionary doesn't contain job_id or clusters.")
#         return {"status": "failed", "reason": "No resources found"}
    
#     # Ask for confirmation if required
#     if confirm:
#         print(f"⚠️ You are about to delete the following resources:")
#         if job_id:
#             print(f"   - Job: {results.get('job_name', 'Unknown')} (ID: {job_id})")
        
#         if clusters:
#             print(f"   - Clusters ({len(clusters)}):")
#             for key, cluster_id in clusters.items():
#                 print(f"     - {key} (ID: {cluster_id})")
        
#         confirmation = input("\nAre you sure you want to delete these resources? (y/n): ").strip().lower()
#         if confirmation != 'y':
#             print("❌ Cleanup cancelled.")
#             return {"status": "cancelled"}
    
#     deletion_status = {"job": None, "clusters": {}}
    
#     # Delete the job
#     if job_id:
#         try:
#             print(f"🗑️ Deleting job {results.get('job_name', 'Unknown')} (ID: {job_id})...")
#             self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/jobs/delete", {"job_id": job_id})
#             print(f"✅ Successfully deleted job with ID: {job_id}")
#             deletion_status["job"] = "success"
#         except Exception as e:
#             print(f"❌ Failed to delete job {job_id}: {e}")
#             deletion_status["job"] = f"failed: {str(e)}"
    
#     # Delete the clusters
#     for key, cluster_id in clusters.items():
#         try:
#             print(f"🗑️ Deleting cluster {key} (ID: {cluster_id})...")
#             self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/clusters/permanent-delete", {"cluster_id": cluster_id})
#             print(f"✅ Successfully deleted cluster with ID: {cluster_id}")
#             deletion_status["clusters"][cluster_id] = "success"
#         except Exception as e:
#             print(f"❌ Failed to delete cluster {cluster_id}: {e}")
#             deletion_status["clusters"][cluster_id] = f"failed: {str(e)}"
    
#     # Overall status
#     if deletion_status["job"] == "success" and all(status == "success" for status in deletion_status["clusters"].values()):
#         deletion_status["status"] = "success"
#         print("\n✅ All resources have been successfully deleted.")
#     else:
#         deletion_status["status"] = "partial"
#         print("\n⚠️ Some resources could not be deleted. See details above.")
    
#     return deletion_status

# # Add the method to the existing instance of NotebookSolutionCompanion (nsc)
# nsc.cleanup_digital_pathology_resources = types.MethodType(_nsc_cleanup_digital_pathology_resources, nsc)

# print("✅ Added cleanup method to NotebookSolutionCompanion (nsc) instance")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Specify Configs.
# MAGIC - UC Catalog | Project "Schema" 
# MAGIC - Derive User + Workspace Info.
# MAGIC - Job Workflow

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

suffix = ''.join([s[0] for s in user.split('@')[0].split('.')]) + datetime.now().strftime("%Y%m%d")
suffix

# COMMAND ----------

# DBTITLE 1,Retrieve Workspace URL
# Set your workspace URL
workspace_url = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
print(f"Workspace URL: {workspace_url} (e.g., https://<databricks-instance>)")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Config.

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
    
    # Create the job configuration
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

# DBTITLE 1,deploy_digital_pathology_job
# ## Function to deploy resources and job
# def deploy_digital_pathology_job(suffix="", reuse=True, run_job=False, workspace_url=None):
#     """
#     Creates and deploys the digital pathology job using the enhanced NSC methods
#     """
#     # Create job configuration
#     job_json = create_digital_pathology_job_config(suffix)
    
#     # Deploy the job using the new method
#     print("🚀 Creating digital pathology job with job clusters...")
#     result = nsc._deploy_digital_pathology_job(job_json, suffix, reuse, run_job)
    
#     print("\n✅ Job deployment complete!")
    
#     # Add URLs if workspace_url is provided
#     if workspace_url:
#         # Ensure URL is properly formatted
#         if workspace_url.endswith("/"):
#             workspace_url = workspace_url[:-1]  # Remove trailing slash if present
        
#         # Add job URL
#         job_id = result['job_id']
#         job_name = result['job_name']
#         job_url = f"{workspace_url}/#job/{job_id}"
#         result['job_url'] = job_url
        
#         # Add cluster URLs
#         cluster_urls = {}
#         for key, cluster_id in result['clusters'].items():
#             cluster_url = f"{workspace_url}/#setting/clusters/{cluster_id}/configuration"
#             cluster_urls[key] = cluster_url
        
#         result["cluster_urls"] = cluster_urls
        
#         # Add run URL if a run was started
#         if "run_id" in result:
#             run_id = result['run_id']
#             job_run_url = f"{workspace_url}/#job/{job_id}/run/{run_id}"
#             result['job_run_url'] = job_run_url
    
#     # # Display a summary of the deployment with links
#     # print("\n📋 Deployment Summary:")
    
#     # # Display job information
#     # if workspace_url and 'job_url' in result:
#     #     job_link = f"<a href='{result['job_url']}' target='_blank'>{result['job_name']}</a>"
#     #     print(f"   - Job: {job_link if nsc.print_html else result['job_name']} (ID: {result['job_id']})")
#     #     if not nsc.print_html:
#     #         print(f"   - Job URL: {result['job_url']}")
#     # else:
#     #     print(f"   - Job ID: {result['job_id']}")
#     #     print(f"   - Job Name: {result['job_name']}")
    
#     # # Display run information if available
#     # if "run_id" in result and workspace_url and 'job_run_url' in result:
#     #     run_link = f"<a href='{result['job_run_url']}' target='_blank'>Run #{result['run_id']}</a>"
#     #     print(f"   - Run: {run_link if nsc.print_html else 'Run #' + str(result['run_id'])}")
#     #     if not nsc.print_html:
#     #         print(f"   - Run URL: {result['job_run_url']}")
#     # elif "run_id" in result:
#     #     print(f"   - Run ID: {result['run_id']}")
    
#     # # Display cluster information
#     # print(f"   - Clusters: {len(result['clusters'])} deployed with 10-minute auto-termination")
    
#     # if workspace_url and "cluster_urls" in result:
#     #     print("   - Cluster details:")
#     #     for key, cluster_id in result['clusters'].items():
#     #         cluster_url = result['cluster_urls'][key]
#     #         cluster_link = f"<a href='{cluster_url}' target='_blank'>{key}</a>"
#     #         print(f"     - {cluster_link if nsc.print_html else key} (ID: {cluster_id})")
#     #         if not nsc.print_html:
#     #             print(f"       {cluster_url}")
    
#     # Use displayHTML for a more interactive display if supported
#     if nsc.print_html:
#         try:
#             from IPython.display import display, HTML
            
#             # Create HTML for job information
#             job_html = f"""
#             <div style="margin-top: 20px; padding: 10px; border: 1px solid #ccc; border-radius: 5px; background-color: #f8f8f8;">
#                 <h3 style="margin-top: 0;">Digital Pathology Job Deployment</h3>
#                 <p><strong>Job:</strong> <a href="{result['job_url']}" target="_blank">{result['job_name']}</a> (ID: {result['job_id']})</p>
#             """
            
#             # Add run information if available
#             if "run_id" in result and 'job_run_url' in result:
#                 job_html += f"""<p><strong>Run:</strong> <a href="{result['job_run_url']}" target="_blank">Run #{result['run_id']}</a></p>"""
            
#             # Add cluster information
#             job_html += """<p><strong>Clusters:</strong></p><ul>"""
#             for key, cluster_id in result['clusters'].items():
#                 cluster_url = result['cluster_urls'][key]
#                 job_html += f"""<li><a href="{cluster_url}" target="_blank">{key}</a> (ID: {cluster_id})</li>"""
#             job_html += """</ul></div>"""
            
#             display(HTML(job_html))
#         except:
#             pass
    
#     return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy Compute Resources + Job + Initialize Task Run

# COMMAND ----------

# DBTITLE 1,Deploy (ReUse) Resources +/- Run Task(s)
## Deploy, reuse existing resources, and RUN the job 
result = deploy_digital_pathology_job(suffix=suffix, reuse=True, run_job=True, workspace_url=workspace_url)
# print(f"Deployed with Resource ReUse and Running Job: {result}")

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
# MAGIC ### There are different ways one can deploy the the workflow job. 
# MAGIC
# MAGIC By default we will setup 3 different compute clusters and initialize the workflow.
# MAGIC
# MAGIC However, the other options as as followes:
# MAGIC
# MAGIC   **Extract workspace url**
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
# MAGIC   **Deploy, reuse existing resources, and DO NOT run the job**
# MAGIC   ```
# MAGIC   # result = deploy_digital_pathology_job(suffix=suffix, reuse=True, run_job=False, workspace_url=workspace_url)
# MAGIC   # print(f"Deployed with Resource ReUse and Running Job: {result}")
# MAGIC   ```
# MAGIC
# MAGIC   **Deploy, reuse existing resources, and RUN the job**
# MAGIC   ```
# MAGIC   result = deploy_digital_pathology_job(suffix=suffix, reuse=True, run_job=True, workspace_url=workspace_url)
# MAGIC   # print(f"Deployed with Resource ReUse and Running Job: {result}")
# MAGIC   ```

# COMMAND ----------

# DBTITLE 1,deployment results
result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Up 
# MAGIC
# MAGIC When you are ready to exit the Digital Pathology Solution Accelerator, we can clean up the compute resources and job deployment. 
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
