# Databricks notebook source
# MAGIC %md
# MAGIC # Facts — Silver Layer
# MAGIC
# MAGIC Reads the Bronze ball-by-ball table, applies standardisation and
# MAGIC data-quality handling, and writes the Silver Delta table.

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
    handleNull,
)

# Define Catalog and null placeholders
catalog = "ipl_data_analysis"
null_placeholders = ["NULL", "NA", "N/A", "UNKNOWN", " ", "", "-", "--"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ball by Ball — Silver Layer Processing

# COMMAND ----------

# Read Bronze Layer Data
dfb = spark.read.table(f"{catalog}.bronze.brz_ball_by_ball")

print("Table: Ball by Ball")
getRowsCols(dfb)

# Data profiling
getProfile(dfb)

# Inspect schema
dfb.printSchema()

# Null report
null_report = getNullReport(dfb, null_placeholders)
display(null_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Findings
# MAGIC
# MAGIC - match_date requires conversion from String to Date.
# MAGIC - Column names and string values require standardisation.
# MAGIC - Extra_Type contains casing inconsistencies (for example, wides/Wides).
# MAGIC - Actual nulls and placeholder nulls are identified.
# MAGIC - match_id must not contain nulls.
# MAGIC - Duplicate match_id values are expected because the table is at delivery grain.

# COMMAND ----------

def silver_transform_ball_by_ball(dataframe, null_placeholders):
    """Clean and standardise the ball-by-ball Bronze DataFrame."""

    # Transformation 01: Standardise column names
    dfs = stdColName(dataframe)

    # Transformation 02: Trim and standardise string columns
    string_type_cols = getStringTypeCols(dfs)
    dfs = stdTrimCols(dfs, string_type_cols)
    dfs = stdCatCols(dfs, string_type_cols)

    # Transformation 03: Identify columns containing nulls/placeholders
    null_report = getNullReport(dfs, null_placeholders)
    null_cols = [
        row["column_name"]
        for row in (
            null_report
            .filter(F.col("total_bad_records") > 0)
            .select("column_name")
            .collect()
        )
    ]

    # Transformation 04: Standardise nulls/placeholders
    dfs = handleNull(
        dfs,
        columns=null_cols,
        null_placeholders=null_placeholders,
    )

    return dfs

# COMMAND ----------

# Generate Silver DataFrame
dfs = silver_transform_ball_by_ball(dfb, null_placeholders)

# Additional Transformation 05: Convert match_date from String to Date
dfs = dfs.withColumn(
    "match_date",
    F.to_date(F.col("match_date"), "M/d/yyyy"),
)

# Additional Transformation 06: Standardise LBW dismissal label
dfs = dfs.withColumn(
    "out_type",
    F.when(F.lower(F.col("out_type")) == "lbw", "LBW")
     .otherwise(F.col("out_type")),
)

# Inspect transformed DataFrame
dfs.printSchema()
display(dfs.limit(5))

# COMMAND ----------

# Save Silver Delta table
(
    dfs.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.slv_ball_by_ball")
)
