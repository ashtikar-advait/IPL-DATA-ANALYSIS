# Databricks notebook source
# Creating Spark Session

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IPL Data Analysis").getOrCreate()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating Catalog and Schemas
# MAGIC CREATE CATALOG IF NOT EXISTS ipl_data_analysis;
# MAGIC
# MAGIC USE CATALOG ipl_data_analysis;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ipl_data_analysis.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ipl_data_analysis.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ipl_data_analysis.gold;
# MAGIC
# MAGIC -- Source Data
# MAGIC CREATE SCHEMA IF NOT EXISTS ipl_data_analysis.source_data;
# MAGIC
# MAGIC -- VOLUME creation
# MAGIC CREATE VOLUME iF NOT EXISTS ipl_data_analysis.source_data.raw;
# MAGIC
# MAGIC -- Next Step is to upload the file in to the raw volume it is better to upload using the GUI as of now
# MAGIC

# COMMAND ----------

# # Getting Files

# # AWS s3 Path
# path = "s3://ipl-data-analysis/"

# # Loading Data
# ball_by_ball = spark.read.format("csv").option("header","true").load(f"{path}Ball_By_Ball.csv")
# match = spark.read.format("csv").option("header","true").load(f"{path}Match.csv")
# player = spark.read.format("csv").option("header","true").load(f"{path}Player.csv")
# player_match = spark.read.format("csv").option("header","true").load(f"{path}Player_match.csv")
# team = spark.read.format("csv").option("header","true").load(f"{path}Team.csv")

# COMMAND ----------

## Define Catalog
catalog = "ipl_data_analysis"

# COMMAND ----------

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

    StructField("Player_Out", DoubleType(), True),
    StructField("Fielders", DoubleType(), True),

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
    StructField("MatchDateSK", IntegerType(), True)
])

# COMMAND ----------

## Defining Data Frame Data Types
raw_data_path = "/Volumes/ipl_data_analysis/source_data/raw/Ball_By_Ball.csv*"

bbb_df = spark.read.option("header","true").option("delimiter",",").schema(ball_by_ball_schema).csv(raw_data_path)
bbb_df = bbb_df.toDF(*[c.lower() for c in bbb_df.columns])
bbb_df.show(5)