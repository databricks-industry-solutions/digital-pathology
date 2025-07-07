# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <!-- This is added to Workspace path to circumvent encountered issues (_when tested Jan2025_).  -->
# MAGIC
# MAGIC <!-- #### (original) -- this previous approach now has a recent code-change / PR induced Error
# MAGIC
# MAGIC
# MAGIC **Jan2025_Update NB:** Recent updates to repository dependency `https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html` broke the existing code process. Hidden/Commented code reflects prior version.    
# MAGIC    
# MAGIC **Workaround Solution Provided:** ` solacc/companion/_init_.py` from a previous [`Pull Request`](https://github.com/databricks-industry-solutions/notebook-solution-companion/blob/f7e381d77675b29c2d3f9d377a528ceaf2255f23/solacc/companion/__init__.py) is copied to  `solacc_companion_init` in the workspace and `%run` to access `NotebookSolutionCompanion()` 
# MAGIC
# MAGIC
# MAGIC ## Install util packages (separate cell)
# MAGIC # %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html pyspark>=3.1.2 --quiet --disable-pip-version-check
# MAGIC # dbutils.library.restartPython()
# MAGIC
# MAGIC
# MAGIC ## Import solution accelerator companion modules/tools (separate cell)
# MAGIC # from solacc.companion import NotebookSolutionCompanion
# MAGIC # nsc = NotebookSolutionCompanion() -->
# MAGIC
# MAGIC <!-- `TypeError: JobsAPI.create() got an unexpected keyword argument 'request'` . -->
# MAGIC     
# MAGIC - The `__init__.py` code used here is taken from a prior PR e.g. [solacc/companion/\__init\__.py](https://github.com/databricks-industry-solutions/notebook-solution-companion/blob/f7e381d77675b29c2d3f9d377a528ceaf2255f23/solacc/companion/__init__.py) <!-- link wrt the PR update --> that works without throwing the observed `request` error related to a recent PR

# COMMAND ----------

# DBTITLE 1,__init__.py
# Databricks notebook source
from dbacademy.dbrest import DBAcademyRestClient
from dbruntime.display import displayHTML
from databricks.sdk import WorkspaceClient
import hashlib
import json
import re
import time
import copy
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from databricks.sdk.service.jobs import JobSettings, CreateJob
from databricks.sdk.service.pipelines import EditPipeline, CreatePipeline
from databricks.sdk.service.compute import CreateCluster

def init_locals():

    # noinspection PyGlobalUndefined
    global spark, sc, dbutils

    try: spark
    except NameError:spark = SparkSession.builder.getOrCreate()

    try: sc
    except NameError: sc = spark.sparkContext

    try: dbutils
    except NameError:
        if spark.conf.get("spark.databricks.service.client.enabled") == "true":
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        else:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]

    return sc, spark, dbutils


sc, spark, dbutils = init_locals()

class NotebookSolutionCompanion():
  """
  A class to provision companion assets for a notebook-based solution, includingn job, cluster(s), DLT pipeline(s) and DBSQL dashboard(s)
  """
  
  def __init__(self):
    self.w = self.get_workspace_client()
    self.solution_code_name = self.get_notebook_dir().split('/')[-1]
    self.solacc_path = self.get_notebook_dir()
    hash_code = hashlib.sha256(self.solacc_path.encode()).hexdigest()
    self.job_name = f"[RUNNER] {self.solution_code_name} | {hash_code}" # use hash to differentiate solutions deployed to different paths
    self.client = DBAcademyRestClient() # part of this code uses dbacademy rest client as the SDK migration work is ongoing
    self.workspace_url = self.get_workspace_url()
    self.print_html = int(spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion").split(".")[0].split("__")[-1]) >= 11 # below DBR 11, html print is not supported
    self.username = self.get_username()
    self.cloud = self.get_cloud()
  
  def get_cloud(self) -> str:
    if self.w.config.is_azure:
      return "MSA"
    elif self.w.config.is_aws:
      return "AWS"
    elif self.w.config.is_gcp:
      return "GCP"
    else: 
      raise NotImplementedError

  @staticmethod
  def get_workspace_client() -> WorkspaceClient: 
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    DATABRICKS_TOKEN = ctx.apiToken().getOrElse(None)
    DATABRICKS_URL = ctx.apiUrl().getOrElse(None)
    return WorkspaceClient(host=DATABRICKS_URL, token=DATABRICKS_TOKEN)

  def get_username(self) -> str:
    return self.w.current_user.me().user_name
      
  @staticmethod
  def get_workspace_url() -> str:
    try:
        url = spark.conf.get('spark.databricks.workspaceUrl') # wrap this in try because this config went missing in GCP in July 2023
    except:
        url = ""
    return url

  @staticmethod
  def get_notebook_dir() -> str:
    notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    return "/".join(notebook_path.split("/")[:-1])
  
  @staticmethod
  def convert_job_cluster_to_cluster(job_cluster_params):
    params = job_cluster_params["new_cluster"]
    params["cluster_name"] = f"""{job_cluster_params["job_cluster_key"]}"""
    params["autotermination_minutes"] = 15 # adding a default autotermination as best practice
    return params

  def create_or_update_job_by_name(self, params):
    """Look up the companion job by name and resets it with the given param and return job id; create a new job if a job with that name does not exist"""
    # job_found = self.client.jobs().get_by_name(params["name"])
    job_found = list(self.w.jobs.list(name=params["name"]))
    if job_found: 
      job_id = job_found[0].job_id
      # reset_params = {"job_id": job_id,
      #                "new_settings": params}
      reset_job_settings = JobSettings().from_dict(params)
      # json_response = self.client.execute_post_json(f"{self.client.endpoint}/api/2.1/jobs/reset", reset_params) # returns {} if status is 200
      self.w.jobs.reset(job_id, reset_job_settings)
      # assert json_response == {}, "Job reset returned non-200 status"
      
      if self.print_html:
          displayHTML(f"""Reset the <a href="/#job/{job_id}/tasks" target="_blank">{params["name"]}</a> job to original definition""")
      else:
          print(f"""Reset the {params["name"]} job at: {self.workspace_url}/#job/{job_id}/tasks""")
          
    else:
      # json_response = self.client.execute_post_json(f"{self.client.endpoint}/api/2.1/jobs/create", params)
      create_job_request = CreateJob().from_dict(params)
      job_id = self.w.jobs.create(request=create_job_request).job_id
      if self.print_html:
          displayHTML(f"""Created <a href="/#job/{job_id}/tasks" target="_blank">{params["name"]}</a> job""")
      else:
          print(f"""Created {params["name"]} job at: {self.workspace_url}/#job/{job_id}/tasks""")
          
    return job_id
  
  # Note these functions assume that names for solacc jobs/cluster/pipelines are unique, which is guaranteed if solacc jobs/cluster/pipelines are created from this class only
  def create_or_update_pipeline_by_name(self, dlt_config_table, pipeline_name, dlt_definition_dict, spark):
    """Look up a companion pipeline by name and edit with the given param and return pipeline id; create a new pipeline if a pipeline with that name does not exist"""
    # pipeline_found = self.client.pipelines.get_by_name(pipeline_name)
    pipeline_found = list(self.w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'"))
      
    if pipeline_found:
        pipeline_id = pipeline_found[0].pipeline_id
        dlt_definition_dict['pipeline_id'] = pipeline_id
        # self.client.execute_put_json(f"{self.client.endpoint}/api/2.0/pipelines/{pipeline_id}", dlt_definition_dict)
        request = EditPipeline(pipeline_id = pipeline_id).from_dict(dlt_definition_dict)
        self.w.pipelines.update(request=request, pipeline_id=pipeline_id)
    else:
        # response = self.client.pipelines().create_from_dict(dlt_definition_dict)
        request = CreatePipeline().from_dict(dlt_definition_dict)
        pipeline_id = self.w.pipelines.create(request=request).pipeline_id
        
    return pipeline_id
  
  def create_or_update_cluster_by_name(self, params):
      """Look up a companion cluster by name and edit with the given param and return cluster id; create a new cluster if a cluster with that name does not exist"""
      
      def edit_cluster(client, cluster_id, params):
        """Wait for a cluster to be in editable states and edit it to the specified params"""
        cluster_state = client.execute_get_json(f"{client.endpoint}/api/2.0/clusters/get?cluster_id={cluster_id}")["state"]
        while cluster_state not in ("RUNNING", "TERMINATED"): # cluster edit only works in these states; all other states will eventually turn into those two, so we wait and try later
          time.sleep(30) 
          cluster_state = client.execute_get_json(f"{client.endpoint}/api/2.0/clusters/get?cluster_id={cluster_id}")["state"]
        json_response = client.execute_post_json(f"{client.endpoint}/api/2.0/clusters/edit", params) # returns {} if status is 200
        assert json_response == {}, "Cluster edit returned non-200 status"
      
      clusters = self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/clusters/list")["clusters"]
      clusters_matched = list(filter(lambda cluster: params["cluster_name"] == cluster["cluster_name"], clusters))
      cluster_id = clusters_matched[0]["cluster_id"] if len(clusters_matched) == 1 else None
      if cluster_id: 
        params["cluster_id"] = cluster_id
        edit_cluster(self.client, cluster_id, params)
        if self.print_html:
          displayHTML(f"""Reset the <a href="/#setting/clusters/{cluster_id}/configuration" target="_blank">{params["cluster_name"]}</a> cluster to original definition""")
        else:
          print(f"""Reset the {params["cluster_name"]} cluster at: {self.workspace_url}/#setting/clusters/{cluster_id}/configuration""")
          
        
        
      else:
        json_response = self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/clusters/create", params)
        cluster_id = json_response["cluster_id"]
        if self.print_html:
          displayHTML(f"""Created <a href="/#setting/clusters/{cluster_id}/configuration" target="_blank">{params["cluster_name"]}</a> cluster""")
        else:
          print(f"""Created {params["cluster_name"]} cluster at: {self.workspace_url}/#setting/clusters/{cluster_id}/configuration""")
        
      return cluster_id

  def customize_cluster_json(self, input_json):
    node_type_id_dict = copy.deepcopy(input_json["node_type_id"]) 
    input_json["node_type_id"] = node_type_id_dict[self.cloud]
    if self.cloud == "AWS": 
      input_json["aws_attributes"] = {
                        "availability": "ON_DEMAND",
                        "zone_id": "auto"
                    }
    if self.cloud == "MSA": 
      input_json["azure_attributes"] = {
                        "availability": "ON_DEMAND_AZURE",
                        "zone_id": "auto"
                    }
    if self.cloud == "GCP": 
      input_json["gcp_attributes"] = {
                        "use_preemptible_executors": False
                    }
    return input_json
    
  @staticmethod
  def customize_job_json(input_json, job_name, solacc_path, cloud):
    if "name" not in input_json:
      input_json["name"] = job_name

    for i, _ in enumerate(input_json["tasks"]):
      if "notebook_task" in input_json["tasks"][i]:
        notebook_name = input_json["tasks"][i]["notebook_task"]['notebook_path']
        input_json["tasks"][i]["notebook_task"]['notebook_path'] = solacc_path + "/" + notebook_name
        
    if "job_clusters" in input_json:
      for j, _ in enumerate(input_json["job_clusters"]):
        if "new_cluster" in input_json["job_clusters"][j]:
          node_type_id_dict = input_json["job_clusters"][j]["new_cluster"]["node_type_id"]
          input_json["job_clusters"][j]["new_cluster"]["node_type_id"] = node_type_id_dict[cloud]
          if cloud == "AWS": 
            input_json["job_clusters"][j]["new_cluster"]["aws_attributes"] = {
                              "availability": "ON_DEMAND",
                              "zone_id": "auto"
                          }
          if cloud == "MSA": 
            input_json["job_clusters"][j]["new_cluster"]["azure_attributes"] = {
                              "availability": "ON_DEMAND_AZURE",
                              "zone_id": "auto"
                          }
          if cloud == "GCP": 
            input_json["job_clusters"][j]["new_cluster"]["gcp_attributes"] = {
                              "use_preemptible_executors": False
                          }
      input_json["access_control_list"] = [
          {
          "group_name": "users",
          "permission_level": "CAN_MANAGE_RUN"
          }
      ]
    return input_json
  
  @staticmethod
  def customize_pipeline_json(input_json, solacc_path):
    for i, _ in enumerate(input_json["libraries"]):
      notebook_name = input_json["libraries"][i]["notebook"]['path']
      input_json["libraries"][i]["notebook"]['path'] = solacc_path + "/" + notebook_name
    return input_json
  
  def start_cluster(self, cluster_id):
    "starts cluster if terminated; no op otherwise"
    cluster_state = self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/clusters/get?cluster_id={cluster_id}")["state"]
    if cluster_state in ("TERMINATED"):
      response = self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/clusters/start", {"cluster_id": cluster_id})
      assert response == {}, "" # returns {} if 200
      return
     
  
  def install_libraries(self, jcid, jcl):
    """install_libraries is not synchronous: does not block until installs complete""" 
    self.client.execute_post_json(f"{self.client.endpoint}/api/2.0/libraries/install", {"cluster_id": jcid, "libraries":jcl} )
    
  @staticmethod
  def get_library_list_for_cluster(job_input_json, jck):
    jcl = []
    for t in job_input_json["tasks"]:
      if "job_cluster_key" in t: # task such as DLT pipelines may not include a job cluster key
        if t["job_cluster_key"] == jck and "libraries" in t:
          if t["libraries"]:
            jcl += t["libraries"]
    return jcl
  
  def set_acl_for_cluster(self, jcid):
    response = self.client.execute_patch_json(f"{self.client.endpoint}/api/2.0/preview/permissions/clusters/{jcid}", 
                          {
                            "access_control_list": [
                              {
                                "group_name": "users",
                                "permission_level": "CAN_RESTART"
                              }
                            ]
                          })

  
  def deploy_compute(self, input_json, run_job=False, wait=0):
    self.job_input_json = copy.deepcopy(input_json)
    self.job_params = self.customize_job_json(self.job_input_json, self.job_name, self.solacc_path, self.cloud)
    self.job_id = self.create_or_update_job_by_name(self.job_params)
    time.sleep(wait) # adding wait (seconds) to allow time for JSL cluster configuration using Partner Connect to complete
    if not run_job: # if we don't run job, create interactive cluster
      if "job_clusters" in self.job_params:
        for job_cluster_params in self.job_params["job_clusters"]:
          jck = job_cluster_params["job_cluster_key"]
          if "new_cluster" in job_cluster_params:
            jcid = self.create_or_update_cluster_by_name(self.convert_job_cluster_to_cluster(job_cluster_params)) # returns cluster id
            self.set_acl_for_cluster(jcid)
            jcl = self.get_library_list_for_cluster(self.job_input_json, jck)
            if jcl:
              self.start_cluster(jcid)
              self.install_libraries(jcid, jcl)
    else:
      self.run_job()
      
  def deploy_pipeline(self, input_json, dlt_config_table, spark):
    self.pipeline_input_json = copy.deepcopy(input_json)
    self.pipeline_params = self.customize_pipeline_json(self.pipeline_input_json, self.solacc_path)
    pipeline_name = self.pipeline_params["name"] 
    return self.create_or_update_pipeline_by_name(dlt_config_table, pipeline_name, self.pipeline_params, spark) 

  def get_wsfs_folder_id(self, target_wsfs_directory): # Try creating a wsfs folder, return object id 
    trial = 1
    client = self.client
    try: 
      client.execute_post_json(f"{client.endpoint}/api/2.0/workspace/mkdirs", {"path": target_wsfs_directory})
    except:
      pass
    wsfs_status = client.execute_get_json(f"{client.endpoint}/api/2.0/workspace/get-status?path={target_wsfs_directory}")
    if wsfs_status["object_type"] == "DIRECTORY":
      return wsfs_status["object_id"]
    while wsfs_status["object_type"] != "DIRECTORY":
      trial += 1
      try:
        client.execute_post_json(f"{client.endpoint}/api/2.0/workspace/mkdirs", {"path": f"{target_wsfs_directory}_{trial}"})
      except:
        pass
      wsfs_status = client.execute_get_json(f"{client.endpoint}/api/2.0/workspace/get-status?path={target_wsfs_directory}_{trial}")
    return wsfs_status["object_id"]

  def check_if_dashboard_exists(self, id):
    try:
      self.client.execute_get_json(f"{self.client.endpoint}/api/2.0/preview/sql/permissions/dashboards/{id}")
      return True
    except Exception:
      return False

  def deploy_dbsql(self, input_path, dbsql_config_table, spark, reuse=True):
    error_string = "Cannot import dashboard; please enable dashboard import feature first"
    db, tb =  dbsql_config_table.split(".")
    dbsql_config_table_exists = tb in [t.name for t in spark.catalog.listTables(db)]
    dbsql_file_name = input_path.split("/")[-1].split(".")[0]
    target_wsfs_directory = f"""/Users/{self.username}/{dbsql_file_name}"""
    
    # Try retrieve dashboard id if exists
    if not reuse:
      print(f"Not reusing exisitng dashboards; a new dashboard will be created and the {dbsql_config_table} will include the new dashboard id")
      id = None 
    elif not dbsql_config_table_exists:
      print(f"{dbsql_config_table} does not exist")
      id = None 
    else:
      dbsql_id_pdf = spark.table(dbsql_config_table).filter(f"path = '{input_path}' and solacc = '{self.solacc_path}'").toPandas()
      assert len(dbsql_id_pdf) <= 1, f"Two or more dashboards created from the same in-repo-path {input_path} exist in the {dbsql_config_table} table for the same accelerator {self.solacc_path}; this is unexpected; please remove the duplicative record(s) in {dbsql_config_table} and try again"
      id = dbsql_id_pdf['id'][0] if len(dbsql_id_pdf) > 0 else None
      

    # If we found the dashboard record in our table, and the dashboard was successfully created, then display the dashboard link and return id
    if id and id != error_string and self.check_if_dashboard_exists(id):
      if self.print_html:
            displayHTML(f"""Found <a href="/sql/dashboards/{id}" target="_blank">DBSQL dashboard</a> created from {input_path} of this accelerator""")
      else:
            print(f"""Found dashboard for this accelerator at: {self.workspace_url}/sql/dashboards/{id}""")
      return id
    else:
      # If the dashboard does not exist in record, or does not exist in the workspace, or we do not want to reuse it, create the dashboard first and log it to the dbsql table
      # TODO: Remove try except once the API is in public preview
      try:
        # get the folder id for the folder we will save queries to
        folder_object_id = self.get_wsfs_folder_id(target_wsfs_directory)

        # create dashboard
        with open(input_path) as f:
          input_json = json.load(f)
        client = self.client
        result = client.execute_post_json(f"{client.endpoint}/api/2.0/preview/sql/dashboards/import", {'parent': f'folders/{folder_object_id}', "import_file_contents": input_json})
        id = result['id']
        
        # create record in dbsql table to enable reuse
        if not dbsql_config_table_exists:
          # initialize table
          spark.createDataFrame([{"path": input_path, "id": id, "solacc": self.solacc_path}]).write.mode("append").option("mergeSchema", "True").saveAsTable(dbsql_config_table)
        else:
          # upsert table record
          spark.createDataFrame([{"path": input_path, "id": id, "solacc": self.solacc_path}]).createOrReplaceTempView("new_record")
          spark.sql(f"""MERGE INTO {dbsql_config_table} t USING new_record n
          ON t.path = n.path and t.solacc = n.solacc
          WHEN MATCHED THEN UPDATE SET t.id = n.id
          WHEN NOT MATCHED THEN INSERT *
          """)
        
        # display result
        if self.print_html:
            displayHTML(f"""Created <a href="/sql/dashboards/{id}" target="_blank">{result['name']} dashboard</a> """)
        else:
            print(f"""Created {result['name']} dashboard at: {self.workspace_url}/sql/dashboards/{id}-{result['slug']}""")
        
        return id
      
      except:
        pass
    
  def submit_run(self, task_json):
    json_response = self.client.execute_post_json(f"/2.1/jobs/runs/submit", task_json)
    assert "run_id" in json_response, "task_json submission errored"
    run_id = json_response["run_id"]
    response = self.client.runs().wait_for(run_id)
    result_state= response['state'].get('result_state', None)
    assert result_state == "SUCCESS", f"Run failed; please investigate at: {self.workspace_url}#job/<job_id>/run/{run_id} where the <job_id> is the part before `-` on the printed output above" 

  def run_job(self):
    self.run_id = self.client.jobs().run_now(self.job_id)["run_id"]
    response = self.client.runs().wait_for(self.run_id)
    
    # print info about result state
    self.test_result_state= response['state'].get('result_state', None)
    self.life_cycle_state = response['state'].get('life_cycle_state', None)
    
    print("-" * 80)
    print(f"#job/{self.job_id}/run/{self.run_id} is {self.life_cycle_state} - {self.test_result_state}")
    assert self.test_result_state == "SUCCESS", f"Job Run failed: please investigate at: {self.workspace_url}#job/{self.job_id}/run/{self.run_id}"

