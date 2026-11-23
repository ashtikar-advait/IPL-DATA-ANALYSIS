# Databricks notebook source
# MAGIC %md
# MAGIC # Facts — Bronze Layer
# MAGIC
# MAGIC Ingests Ball_By_Ball.csv into the Bronze Delta table.

# COMMAND ----------

# Importing Libraries
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
)

# Define Catalog
catalog = "ipl_data_analysis"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ball_By_Ball.csv Processing

# COMMAND ----------

# Schema Definition
ball_by_ball_schema = StructType([
    StructField("MatcH_id", IntegerType(), True),
    StructField("Over_id", IntegerType(), True),
    StructField("Ball_id", IntegerType(), True),
    StructField("Innings_No", IntegerType(), True),
    StructField("Team_Batting", IntegerType(), True),
    StructField("Team_Bowling", IntegerType(), True),
    StructField("Striker_Batting_Position", IntegerType(), True),
    StructField("Extra_Type", StringType(), True),
    StructField("Runs_Scored", IntegerType(), True),
    StructField("Extra_runs", IntegerType(), True),
    StructField("Wides", IntegerType(), True),
    StructField("Legbyes", IntegerType(), True),
    StructField("Byes", IntegerType(), True),
    StructField("Noballs", IntegerType(), True),
    StructField("Penalty", IntegerType(), True),
    StructField("Bowler_Extras", IntegerType(), True),
    StructField("Out_type", StringType(), True),
    StructField("Caught", IntegerType(), True),
    StructField("Bowled", IntegerType(), True),
    StructField("Run_out", IntegerType(), True),
    StructField("LBW", IntegerType(), True),
    StructField("Retired_hurt", IntegerType(), True),
    StructField("Stumped", IntegerType(), True),
    StructField("caught_and_bowled", IntegerType(), True),
    StructField("hit_wicket", IntegerType(), True),
    StructField("ObstructingFeild", IntegerType(), True),
    StructField("Bowler_Wicket", IntegerType(), True),
    StructField("Match_Date", StringType(), True),
    StructField("Season", IntegerType(), True),
    StructField("Striker", IntegerType(), True),
    StructField("Non_Striker", IntegerType(), True),
    StructField("Bowler", IntegerType(), True),
    StructField("Player_Out", IntegerType(), True),
    StructField("Fielders", IntegerType(), True),
    StructField("Striker_match_SK", IntegerType(), True),
    StructField("StrikerSK", IntegerType(), True),
    StructField("NonStriker_match_SK", IntegerType(), True),
    StructField("NONStriker_SK", IntegerType(), True),
    StructField("Fielder_match_SK", IntegerType(), True),
    StructField("Fielder_SK", IntegerType(), True),
    StructField("Bowler_match_SK", IntegerType(), True),
    StructField("BOWLER_SK", IntegerType(), True),
    StructField("PlayerOut_match_SK", IntegerType(), True),
    StructField("BattingTeam_SK", IntegerType(), True),
    StructField("BowlingTeam_SK", IntegerType(), True),
    StructField("Keeper_Catch", IntegerType(), True),
    StructField("Player_out_sk", IntegerType(), True),
    StructField("MatchDateSK", IntegerType(), True),
])

# Raw Data Path
raw_path = "/Volumes/ipl_data_analysis/source_data/raw/Ball_By_Ball.csv*"

# Read source CSV
dfb = (
    spark.read.format("csv")
    .options(header="true", delimiter=",")
    .schema(ball_by_ball_schema)
    .load(raw_path)
)

# Add ingestion metadata
dfb = (
    dfb.withColumn("source_file", F.col("_metadata.file_path"))
       .withColumn("ingested_at", F.current_timestamp())
)

# Write Bronze Delta table
(
    dfb.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.bronze.brz_ball_by_ball")
)

display(dfb.limit(5))
