# Databricks notebook source
# MAGIC %md
# MAGIC | Version | Date       | Developer | Remark             |
# MAGIC |---------|------------|-----------|--------------------|
# MAGIC | 1.0     | Feb-1-2025 | Johnson | Initial version:developed pipeline & function for data engineering    |

# COMMAND ----------

# pip install synapseml

# COMMAND ----------

# !pip install shap


# COMMAND ----------

import numpy as np; np.__version__ = '1.24.0'
import shap
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import matplotlib.pyplot as plt # for data visualization purposes
import seaborn as sns # for statistical data visualization
%matplotlib inline
import warnings
warnings.filterwarnings('ignore')

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F


# Fit and transform the data

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("example").getOrCreate()


# def display(df):
#     """
#     Mimics Databricks' display() function by converting a Spark DataFrame to Pandas 
#     and displaying it nicely in Jupyter notebooks.
    
#     Args:
#         df (pyspark.sql.DataFrame): The Spark DataFrame to display.
#     """
#     try:
#         from IPython.display import display as ipy_display
#         ipy_display(df.toPandas())  # Convert to Pandas and display
#     except:
#         print(df.show())  # Fallback to show() if Pandas conversion fails


# COMMAND ----------

# DBTITLE 1,Pipeline_Data_ Engineering
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, MinMaxScaler, VectorAssembler

# Step 1: StringIndexer for categorical columns
sex_indexer = StringIndexer(inputCol="sex", outputCol="sex_index")
smoker_indexer = StringIndexer(inputCol="smoker", outputCol="smoker_index")
region_indexer = StringIndexer(inputCol="region", outputCol="region_index")

# Step 2: OneHotEncoder for region
region_encoder = OneHotEncoder(inputCol="region_index", outputCol="region_encoded")

# Step 3: VectorAssembler to combine "bmi" for MinMaxScaler
bmi_assembler = VectorAssembler(inputCols=["bmi"], outputCol="bmi_assembled")
age_assembler = VectorAssembler(inputCols=["age"], outputCol="age_assembled")

# Step 4: MinMaxScaler for normalization
bmi_scaler = MinMaxScaler(inputCol="bmi_assembled", outputCol="bmi_normalized")
age_scaler = MinMaxScaler(inputCol="age_assembled", outputCol="age_normalized")

# Step 5: Create Pipeline
pipeline = Pipeline(stages=[
    sex_indexer, smoker_indexer, region_indexer, region_encoder,  # Categorical encoding
    bmi_assembler, bmi_scaler,  # bmi normalization
    age_assembler, age_scaler   # age normalization
])

# COMMAND ----------

# DBTITLE 1,VectorAssembler
from pyspark.ml.feature import VectorAssembler

def vectorize_features(df_transformed):
    # Step 1: Select the features to vectorize
    input_columns = ["sex_index", "smoker_index", "region_encoded", "bmi_normalized", "age_normalized", "children"]

    # Step 2: Create a new VectorAssembler to combine features
    assembler = VectorAssembler(inputCols=input_columns, outputCol="features")

    # Step 3: Rename expenses to label
    df_transformed = df_transformed.withColumnRenamed("expenses", "label")

    # Step 4: Vectorize the features using VectorAssembler
    df_vector = assembler.transform(df_transformed)
    df_final = df_vector.select("label", "features")

    return df_final
