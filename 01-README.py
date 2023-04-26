# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/digital-pathology. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/digital-pathology.

# COMMAND ----------

# MAGIC %md
# MAGIC # Streamlining Tumor Growth Assessment with <img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="250"/>
# MAGIC 
# MAGIC 
# MAGIC The rate of tumor growth serves as a crucial biomarker for forecasting patient outcomes and influences the formulation of treatment plans. In clinical settings, pathologists typically count mitotic figures under a microscope, but this manual and subjective method presents reproducibility challenges. Consequently, researchers have been motivated to automate this process and employ advanced machine learning (ML) techniques.
# MAGIC 
# MAGIC <br>
# MAGIC <img src="https://hls-eng-data-public.s3.amazonaws.com/img/slide_heatmap.png" alt="logo" width=60% /> 
# MAGIC </br>
# MAGIC 
# MAGIC A primary obstacle in automating tumor growth assessment is the size of whole slide images (WSIs), which can range from 0.5 to 3.5 GB. These large files can hinder the image preprocessing step required for downstream ML applications.
# MAGIC 
# MAGIC This solution accelerator provides a step-by-step guide to harness Databricks' capabilities for image segmentation and pre-processing on WSIs. Users will learn how to train a binary classifier that generates a metastasis probability map over a WSI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow
# MAGIC We assume that a histopathologist's annotations are already converted into a tabular format (one can use any XML parser to do this conversion). These annotations are subsequently stored in a database in the lakehouse. Utilizing Spark distribution, we create patches from whole slide images (WSI)(already available in the cloud storage) and associate each patch with its corresponding annotation. We then store these information as a table in the database. We then apply a pre-trained image model (InceptionV3) to extract features from these patches and save the generated embeddings. To facilitate exploratory analysis, we use UMAP to visualize these embeddings.
# MAGIC 
# MAGIC In parallel, we fine-tune another pre-trained image model (resnet18) and register the best performing model. All these steps are tracked and logged using MLFlow. Ultimately, we employ this model to predict metastasis probabilities and display the results as a heatmap overlaid on a new whole slide image.
# MAGIC 
# MAGIC [![](https://mermaid.ink/img/pako:eNp1kz1v2zAQhv_KgUMnZyi6FBoCxBUCG4gKw6rTRctJPFuEKZIlqbhpkP_eY2jVbtQaHgTe8957H-SL6KwkUYhAP0YyHZUKDx6HxgD_HPqoOuXQRFgBBlipEK3D2FttD_ydsdb-hI0n520IFJQ5zNXlMslL0hE1HmkO1Cle88kxx8jIS_LqYS5YV0mx5pJdVNY8fgIaWpKS7cOc3iV4V91t5qFtCm0pGIofP0PF49Bz6M2serjX9vSuvmtqkyiehFRdqqkxGVnd3N6WywLujLERI0HQStK5yq-WD7w69BHsHhJWR-sJMMOcJoAykKbW2zGcR1cuOWfNKbuOQsgJryWZqjP0xVNy5b11PQXYezvA93p9YS6uE_PhL3_uKmKr6aJYV-ztnH7-zwYg2ilXFq2ryQefCA5kyHNNcra0t8Z2fxp7H97l6KMKI2r1i64tTyr25x1fjdY-kYd6AduCS03bQq2nDadO-PxeGbqJo6F_XoMtQ9ztltKF51wthQiO_N76gX1huKBVRr957I48QAn8SoBF7lzc9QVKaTfFdFtgoIiB_yoAv6MWW6VVVNP0NhkuVXAan6HnfQ7owBowdMq7FAsxkB9QSX7NL0nWiNjTQI0o-FPSHkcdG9GYV0ZxjLZ-Np0ooh9pIUYneR3nxy-KPepAr78BMtZdPA?type=png)](https://mermaid-js.github.io/mermaid-live-editor/edit#pako:eNp1kz1v2zAQhv_KgUMnZyi6FBoCxBUCG4gKw6rTRctJPFuEKZIlqbhpkP_eY2jVbtQaHgTe8957H-SL6KwkUYhAP0YyHZUKDx6HxgD_HPqoOuXQRFgBBlipEK3D2FttD_ydsdb-hI0n520IFJQ5zNXlMslL0hE1HmkO1Cle88kxx8jIS_LqYS5YV0mx5pJdVNY8fgIaWpKS7cOc3iV4V91t5qFtCm0pGIofP0PF49Bz6M2serjX9vSuvmtqkyiehFRdqqkxGVnd3N6WywLujLERI0HQStK5yq-WD7w69BHsHhJWR-sJMMOcJoAykKbW2zGcR1cuOWfNKbuOQsgJryWZqjP0xVNy5b11PQXYezvA93p9YS6uE_PhL3_uKmKr6aJYV-ztnH7-zwYg2ilXFq2ryQefCA5kyHNNcra0t8Z2fxp7H97l6KMKI2r1i64tTyr25x1fjdY-kYd6AduCS03bQq2nDadO-PxeGbqJo6F_XoMtQ9ztltKF51wthQiO_N76gX1huKBVRr957I48QAn8SoBF7lzc9QVKaTfFdFtgoIiB_yoAv6MWW6VVVNP0NhkuVXAan6HnfQ7owBowdMq7FAsxkB9QSX7NL0nWiNjTQI0o-FPSHkcdG9GYV0ZxjLZ-Np0ooh9pIUYneR3nxy-KPepAr78BMtZdPA)
# MAGIC 
# MAGIC 
# MAGIC By leveraging Databricks for image segmentation and pre-processing, as well as training a binary classifier to produce metastasis probability maps, the automation of tumor growth assessment becomes more achievable. This innovative approach addresses the challenges posed by manual counting and large WSI sizes, ultimately improving the overall efficiency and reliability of the process.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Dataset
# MAGIC The data used in this solution accelerator is from the [Camelyon16 Grand Challenge](http://gigadb.org/dataset/100439), along with annotations based on hand-drawn metastasis outlines. We use curated annotations for this dataset obtained from Baidu Research [github repository](https://github.com/baidu-research/NCRF).
# MAGIC 
# MAGIC 
# MAGIC ## Notebooks
# MAGIC We use Apache Spark's parallelization capabilities, using `pandas_udf`, to generate tumor/normal patches based on annotation data as well as feature extraction, using a pre-trained [InceptionV3](https://keras.io/api/applications/inceptionv3/). We use the embeddings obtained this way to explore clusters of patches by visualizing 2d and 3d embeddings, using UMAP. We then use transfer learning with pytorch to train a convnet to classify tumor vs normal patches and later use the resulting model to overlay a metastasis heatmap on a new slide.
# MAGIC 
# MAGIC 
# MAGIC This solution accelerator contains the following notebooks:
# MAGIC 
# MAGIC - [config]($./config/0-config): configuring paths and other settings. Also for the first time setting up a cluster for patch generation, use the `initscript` generated by the config notebook to install `openSlide` on your cluster.
# MAGIC 
# MAGIC - [create-annotation-deltalake]($./00-create-annotation-deltalake): to download annotations and write to delta.
# MAGIC 
# MAGIC - [patch-generation]($./02-patch-generation): This notebook generates patches from WSI based on annotations. 
# MAGIC 
# MAGIC - [feature-extraction]($./03-feature-extraction): To extract image embeddings using `InceptionV3` in a distributed manner
# MAGIC 
# MAGIC - [unsupervised-learning]($./04-unsupervised-learning): dimensionality reduction and cluster inspection with UMAP
# MAGIC 
# MAGIC - [training]($./05-training): In this notebook we tune and train a binary classifier to classify tumor/normal patches with pytorch and log the model with mlflow.
# MAGIC 
# MAGIC - [metastasis-heatmap]($./06-metastasis-heatmap): This notebook we use the model trained in the previous step to generate a metastasis probability heatmap for a given slide.
# MAGIC 
# MAGIC - [definitions]($./definitions): This notebook contains definitions for some of the functions that are used in multiple places (for example patch generation and pre processing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Orchestration
# MAGIC We have included a [RUNME]($./RUNME) notebook within this package. To run the end-to-end workflow, simply run this notebook (by attaching it to any running cluster). By running the notebook you create a [databricks workflow runner](https://docs.databricks.com/workflows/index.html) (`digital-pathology`) that runs all the steps in this package, inclduing setting up clusters for each part of your experiment. 
# MAGIC 
# MAGIC ## Cluster Setup
# MAGIC This workflow depends on the [openSlide](https://openslide.org/)package which is a C library. Fortunatley databricks open standards makes it seemless to install third party pckages in your cluster. To do so, we first create an `Init Script` to install `openslide-tools` from [OpenSlide library](https://openslide.org/) on your cluster.
# MAGIC Note that all this work is automatically done within the RUNME notebook and you do not need to manually do install anything. After running the notebook, you'll notice that there are three clsuters available to you:
# MAGIC 1. 

# COMMAND ----------

slides_html="""
<iframe src="https://docs.google.com/presentation/d/e/2PACX-1vQkRfYrJsS_2s73lc9Z5Pz15E6r8FhnRVaeWc49VjW1NfHmoGzxoE1GfyJDY4b7dfu7BMQ99X6nlLzp/embed?start=true&loop=true&delayms=3000" frameborder="0" width="960" height="569" allowfullscreen="true" mozallowfullscreen="true" webkitallowfullscreen="true"></iframe>
"""
displayHTML(slides_html)

# COMMAND ----------

# DBTITLE 1,WSI dataset
WSI_PATH='/databricks-datasets/med-images/camelyon16/'
display(dbutils.fs.ls(WSI_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC after creating the cluster, attach this notebook and run this command for a quick look at the slides and test if the libraries are installed.

# COMMAND ----------

# DBTITLE 1,Quick view of some of the slides
import numpy as np
import openslide
import matplotlib.pyplot as plt

f, axarr = plt.subplots(1,4,sharey=True)
i=0
for pid in ["normal_034","normal_036","tumor_044", "tumor_045"]:
  path = f'/dbfs/{WSI_PATH}/{pid}.tif'
  slide = openslide.OpenSlide(path)
  axarr[i].imshow(slide.get_thumbnail([m//50 for m in slide.dimensions]))
  axarr[i].set_title(pid)
  i+=1

# COMMAND ----------

# DBTITLE 1,Viewing slides at different zoom levels
sid='tumor_049'
slide = openslide.OpenSlide(f'/dbfs/{WSI_PATH}/{sid}.tif')
image_datas=[]
region=[14793,127384]
size=[1000,1000]
f, axarr = plt.subplots(1,3,sharex=True,sharey=True)
for level,ind in zip([0,4,5],[0,1,2]):
  img = slide.read_region(region,level,size)
  axarr[ind].imshow(img)
  axarr[ind].set_title(f"level:{level}")
display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## License
# MAGIC Copyright / License info of the notebook. Copyright [2021] the Notebook Authors.  The source in this notebook is provided subject to the [Apache 2.0 License](https://spdx.org/licenses/Apache-2.0.html).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Pandas |BSD 3-Clause License| https://github.com/pandas-dev/pandas/blob/master/LICENSE | https://github.com/pandas-dev/pandas|
# MAGIC |Numpy |BSD 3-Clause License| https://github.com/numpy/numpy/blob/main/LICENSE.txt | https://github.com/numpy/numpy|
# MAGIC |Apache Spark |Apache License 2.0| https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark/tree/master/python/pyspark|
# MAGIC |Pillow (PIL) | HPND License| https://github.com/python-pillow/Pillow/blob/master/LICENSE | https://github.com/python-pillow/Pillow/|
# MAGIC |OpenSlide | GNU LGPL version 2.1 |https://github.com/openslide/openslide/blob/main/LICENSE.txt| https://github.com/openslide| 
# MAGIC |Open Slide Python| GNU LGPL version 2.1 |https://github.com/openslide/openslide-python/blob/main/LICENSE.txt| https://github.com/openslide/openslide-python|
# MAGIC |pytorch lightning|Apache License 2.0| https://github.com/PyTorchLightning/pytorch-lightning/blob/master/LICENSE | https://github.com/PyTorchLightning/pytorch-lightning|
# MAGIC |NCRF|Apache License 2.0|https://github.com/baidu-research/NCRF/blob/master/LICENSE|https://github.com/baidu-research/NCRF|
# MAGIC 
# MAGIC |Author|
# MAGIC |-|
# MAGIC |Databricks Inc.|

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disclaimers
# MAGIC Databricks Inc. (“Databricks”) does not dispense medical, diagnosis, or treatment advice. This Solution Accelerator (“tool”) is for informational purposes only and may not be used as a substitute for professional medical advice, treatment, or diagnosis. This tool may not be used within Databricks to process Protected Health Information (“PHI”) as defined in the Health Insurance Portability and Accountability Act of 1996, unless you have executed with Databricks a contract that allows for processing PHI, an accompanying Business Associate Agreement (BAA), and are running this notebook within a HIPAA Account.  Please note that if you run this notebook within Azure Databricks, your contract with Microsoft applies.
