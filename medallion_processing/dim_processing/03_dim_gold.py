# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension & Master Data — Gold Layer
# MAGIC
# MAGIC Reads the Silver dimension/master tables, removes ingestion metadata,
# MAGIC and writes the Gold dimension tables.

# COMMAND ----------

# Importing Libraries
from utils.utils import getRowsCols

# Define Catalog
catalog = "ipl_data_analysis"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team — Gold Layer Processing

# COMMAND ----------

# Read Silver Layer Table
dfs = spark.read.table(f"{catalog}.silver.slv_team")

print("Table: Team")
getRowsCols(dfs)

# COMMAND ----------

def gold_processing_team(dataframe):
    """Prepare the Team dimension for the Gold layer."""

    # Transformation 01: Drop ingestion metadata
    columns_to_drop = ["source_file", "ingested_at"]
    dfg = dataframe.drop(*columns_to_drop)

    return dfg

# COMMAND ----------

# Generate Gold DataFrame
dfg = gold_processing_team(dfs)

display(dfg)

# COMMAND ----------

# Write Gold Delta Table
(
    dfg.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.gold.dim_team")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Player — Gold Layer Processing

# COMMAND ----------

# Read Silver Layer Table
dfs = spark.read.table(f"{catalog}.silver.slv_player")

print("Table: Player")
getRowsCols(dfs)

# COMMAND ----------

def gold_processing_player(dataframe):
    """Prepare the Player dimension for the Gold layer."""

    # Transformation 01: Drop ingestion metadata
    columns_to_drop = ["source_file", "ingested_at"]
    dfg = dataframe.drop(*columns_to_drop)

    return dfg

# COMMAND ----------

# Generate Gold DataFrame
dfg = gold_processing_player(dfs)

display(dfg)

# COMMAND ----------

# Write Gold Delta Table
(
    dfg.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.gold.dim_player")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match — Gold Layer Processing

# COMMAND ----------

# Read Silver Layer Table
dfs = spark.read.table(f"{catalog}.silver.slv_match")

print("Table: Match")
getRowsCols(dfs)

# COMMAND ----------

def gold_processing_match(dataframe):
    """Prepare the Match dimension for the Gold layer."""

    # Transformation 01: Drop unwanted columns and ingestion metadata
    columns_to_drop = ["country_id", "source_file", "ingested_at"]
    dfg = dataframe.drop(*columns_to_drop)

    return dfg

# COMMAND ----------

# Generate Gold DataFrame
dfg = gold_processing_match(dfs)

display(dfg)

# COMMAND ----------

# Write Gold Delta Table
(
    dfg.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.dim_match")
)
