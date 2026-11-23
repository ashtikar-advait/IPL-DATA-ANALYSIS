# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension & Master Data — Silver Layer
# MAGIC
# MAGIC Reads the Bronze dimension/master tables, applies the cleaning and
# MAGIC standardisation steps defined in the project, and writes Silver Delta tables.

# COMMAND ----------

# Importing Libraries
from pyspark.sql import functions as F

# Import reusable utility functions
from utils.utils import (
    getRowsCols,
    getNullReport,
    getProfile,
    stdColName,
    getStringTypeCols,
    stdTrimCols,
    stdCatCols,
    handleGarbage,
    handleNull,
)

# Define Catalog
catalog = "ipl_data_analysis"

# Define common placeholder values
null_placeholders = ["NULL", "NA", "N/A", "UNKNOWN", " ", "", "-", "--"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match — Silver Layer Processing

# COMMAND ----------

# Read Bronze table
dfb = spark.read.table(f"{catalog}.bronze.brz_match")

print("Table: Match")
getRowsCols(dfb)

# Data profiling
getProfile(dfb)

# Schema inspection
dfb.printSchema()

# Null report
null_report = getNullReport(dfb, null_placeholders)
display(null_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Match Data Quality Findings
# MAGIC
# MAGIC - Nulls/placeholders are standardised.
# MAGIC - String columns are trimmed and title-cased.
# MAGIC - Non-printable characters are removed.
# MAGIC - match_date is converted to Date.
# MAGIC - season_year is converted to Integer.

# COMMAND ----------

def silver_transform_match(dataframe, null_placeholders):
    """Clean and standardise the Match Bronze DataFrame."""

    # Transformation 01: Standardise column names
    dfs = stdColName(dataframe)

    # Transformation 02: Trim and standardise string columns
    string_type_cols = getStringTypeCols(dfs)
    dfs = stdTrimCols(dfs, string_type_cols)
    dfs = stdCatCols(dfs, string_type_cols)

    # Transformation 03: Remove non-printable characters
    dfs = handleGarbage(dfs, string_type_cols)

    # Transformation 04: Standardise nulls/placeholders
    null_report = getNullReport(dfs, null_placeholders)
    null_cols = [
        row["column_name"]
        for row in null_report
            .filter(F.col("total_bad_records") > 0)
            .select("column_name")
            .collect()
    ]
    dfs = handleNull(dfs, columns=null_cols, null_placeholders=null_placeholders)

    # Transformation 05: Correct data types
    dfs = (
        dfs.withColumn("match_date", F.to_date(F.col("match_date"), "M/d/yyyy"))
           .withColumn("season_year", F.col("season_year").cast("integer"))
    )

    return dfs

# Generate Silver DataFrame
dfs = silver_transform_match(dfb, null_placeholders)

# Inspect result
dfs.printSchema()
display(dfs.limit(5))

# Save Silver table
(
    dfs.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.slv_match")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Player_Match — Silver Layer Processing

# COMMAND ----------

# Read Bronze table
dfb = spark.read.table(f"{catalog}.bronze.brz_player_match")

print("Table: Player Match")
getRowsCols(dfb)

# Data profiling
getProfile(dfb)

# Schema inspection
dfb.printSchema()

# Null report
null_report = getNullReport(dfb, null_placeholders)
display(null_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Player_Match Data Quality Findings
# MAGIC
# MAGIC - Player_match_SK = -1 is a dummy record and is removed.
# MAGIC - PlayerMatch_key is dropped because its source values were affected by
# MAGIC   scientific notation and the column is not required downstream.
# MAGIC - Batting_Status and Bowling_Status are dropped because they are 100% NULL.
# MAGIC - DOB is converted from String to Date.
# MAGIC - Player_Name is excluded from title-case standardisation to preserve
# MAGIC   names such as SC Ganguly.

# COMMAND ----------

def silver_transform_player_match(dataframe, null_placeholders):
    """Clean and standardise the Player_Match Bronze DataFrame."""

    # Transformation 01: Standardise column names
    dfs = stdColName(dataframe)

    # Transformation 02: Trim string columns
    string_type_cols = getStringTypeCols(dfs)
    dfs = stdTrimCols(dfs, string_type_cols)

    # Preserve original player-name casing
    if "player_name" in string_type_cols:
        string_type_cols.remove("player_name")

    # Transformation 03: Standardise categorical string columns
    dfs = stdCatCols(dfs, string_type_cols)

    # Transformation 04: Standardise nulls/placeholders
    null_report = getNullReport(dfs, null_placeholders)
    null_cols = [
        row["column_name"]
        for row in null_report
            .filter(F.col("total_bad_records") > 0)
            .select("column_name")
            .collect()
    ]
    dfs = handleNull(dfs, columns=null_cols, null_placeholders=null_placeholders)

    return dfs

# Generate Silver DataFrame
dfs = silver_transform_player_match(dfb, null_placeholders)

# Additional transformations based on the source notebook's findings
dfs = dfs.drop("batting_status", "bowling_status", "playermatch_key")
dfs = dfs.filter(F.col("player_match_sk") != -1)
dfs = dfs.withColumn("dob", F.to_date(F.col("dob"), "M/d/yyyy"))

# Inspect result
dfs.printSchema()
display(dfs.limit(5))

# Save Silver table
(
    dfs.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.slv_player_match")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Player — Silver Layer Processing

# COMMAND ----------

# Read Bronze table
dfb = spark.read.table(f"{catalog}.bronze.brz_player")

print("Table: Player")
getRowsCols(dfb)

# Data profiling
getProfile(dfb)

# Schema inspection
dfb.printSchema()

# Null report
null_report = getNullReport(dfb, null_placeholders)
display(null_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Player Data Quality Findings
# MAGIC
# MAGIC - String values are trimmed.
# MAGIC - Non-printable characters are removed.
# MAGIC - Categorical values are standardised.
# MAGIC - DOB is converted from String to Date.
# MAGIC - Placeholder handling follows the configured null-placeholder list.

# COMMAND ----------

def silver_transform_player(dataframe, null_placeholders):
    """Clean and standardise the Player Bronze DataFrame."""

    # Transformation 01: Standardise column names
    dfs = stdColName(dataframe)

    # Transformation 02: Trim string columns
    string_type_cols = getStringTypeCols(dfs)
    dfs = stdTrimCols(dfs, string_type_cols)

    # Transformation 03: Remove non-printable characters
    dfs = handleGarbage(dfs, string_type_cols)

    # Transformation 04: Standardise categorical values
    dfs = stdCatCols(dfs, string_type_cols)

    # Transformation 05: Standardise nulls/placeholders
    null_report = getNullReport(dfs, null_placeholders)
    null_cols = [
        row["column_name"]
        for row in null_report
            .filter(F.col("total_bad_records") > 0)
            .select("column_name")
            .collect()
    ]
    dfs = handleNull(dfs, columns=null_cols, null_placeholders=null_placeholders)

    # Transformation 06: Correct DOB data type
    dfs = dfs.withColumn("dob", F.to_date(F.col("dob"), "M/d/yyyy"))

    return dfs

# Generate Silver DataFrame
dfs = silver_transform_player(dfb, null_placeholders)

# Inspect result
dfs.printSchema()
display(dfs.limit(5))

# Save Silver table
(
    dfs.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.slv_player")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team — Silver Layer Processing

# COMMAND ----------

# Read Bronze table
dfb = spark.read.table(f"{catalog}.bronze.brz_team")

print("Table: Team")
getRowsCols(dfb)

# Data profiling
getProfile(dfb)

# Schema inspection
dfb.printSchema()

# Null report
null_report = getNullReport(dfb, null_placeholders)
display(null_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Team Data Quality Findings
# MAGIC
# MAGIC - Column names are standardised.
# MAGIC - String values are trimmed and title-cased.
# MAGIC - Placeholder handling follows the configured null-placeholder list.

# COMMAND ----------

def silver_transform_team(dataframe, null_placeholders):
    """Clean and standardise the Team Bronze DataFrame."""

    # Transformation 01: Standardise column names
    dfs = stdColName(dataframe)

    # Transformation 02: Trim and standardise string columns
    string_type_cols = getStringTypeCols(dfs)
    dfs = stdTrimCols(dfs, string_type_cols)
    dfs = stdCatCols(dfs, string_type_cols)

    # Transformation 03: Standardise nulls/placeholders
    null_report = getNullReport(dfs, null_placeholders)
    null_cols = [
        row["column_name"]
        for row in null_report
            .filter(F.col("total_bad_records") > 0)
            .select("column_name")
            .collect()
    ]
    dfs = handleNull(dfs, columns=null_cols, null_placeholders=null_placeholders)

    return dfs

# Generate Silver DataFrame
dfs = silver_transform_team(dfb, null_placeholders)

# Inspect result
dfs.printSchema()
display(dfs.limit(5))

# Save Silver table
(
    dfs.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.slv_team")
)
