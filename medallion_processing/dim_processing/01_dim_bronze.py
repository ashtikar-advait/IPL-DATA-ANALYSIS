# Databricks notebook source
# MAGIC %md
# MAGIC # Dimension & Master Data — Bronze Layer
# MAGIC
# MAGIC Ingests the Match, Player, Player_Match and Team source CSV files
# MAGIC into Bronze Delta tables.

# COMMAND ----------

# Importing Libraries
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    LongType,
)

# Define Catalog
catalog = "ipl_data_analysis"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Match.csv Processing

# COMMAND ----------

# Schema Definition
match_schema = StructType([
    StructField("Match_SK", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("Team1", StringType(), True),
    StructField("Team2", StringType(), True),
    StructField("match_date", StringType(), True),
    StructField("Season_Year", StringType(), True),
    StructField("Venue_Name", StringType(), True),
    StructField("City_Name", StringType(), True),
    StructField("Country_Name", StringType(), True),
    StructField("Toss_Winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("Toss_Name", StringType(), True),
    StructField("Win_Type", StringType(), True),
    StructField("Outcome_Type", StringType(), True),
    StructField("ManOfMach", StringType(), True),
    StructField("Win_Margin", IntegerType(), True),
    StructField("Country_id", IntegerType(), True),
])

# Raw Data Path
raw_path = "/Volumes/ipl_data_analysis/source_data/raw/Match.csv*"

# Read source CSV
dfb = (
    spark.read.format("csv")
    .options(header="true", delimiter=",")
    .schema(match_schema)
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
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.bronze.brz_match")
)

display(dfb.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Player.csv Processing

# COMMAND ----------

# Schema Definition
player_schema = StructType([
    StructField("PLAYER_SK", IntegerType(), True),
    StructField("Player_Id", IntegerType(), True),
    StructField("Player_Name", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("Batting_hand", StringType(), True),
    StructField("Bowling_skill", StringType(), True),
    StructField("Country_Name", StringType(), True),
])

# Raw Data Path
raw_path = "/Volumes/ipl_data_analysis/source_data/raw/Player.csv"

# Read source CSV
dfb = (
    spark.read.format("csv")
    .schema(player_schema)
    .options(header="true", delimiter=",")
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
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.bronze.brz_player")
)

display(dfb.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Player_Match.csv Processing

# COMMAND ----------

# Schema Definition
player_match_schema = StructType([
    StructField("Player_match_SK", IntegerType(), True),
    StructField("PlayerMatch_key", LongType(), True),
    StructField("Match_Id", IntegerType(), True),
    StructField("Player_Id", IntegerType(), True),
    StructField("Player_Name", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("Batting_hand", StringType(), True),
    StructField("Bowling_skill", StringType(), True),
    StructField("Country_Name", StringType(), True),
    StructField("Role_Desc", StringType(), True),
    StructField("Player_team", StringType(), True),
    StructField("Opposit_Team", StringType(), True),
    StructField("Season_year", IntegerType(), True),
    StructField("is_manofThematch", IntegerType(), True),
    StructField("Age_As_on_match", IntegerType(), True),
    StructField("IsPlayers_Team_won", IntegerType(), True),
    StructField("Batting_Status", StringType(), True),
    StructField("Bowling_Status", StringType(), True),
    StructField("Player_Captain", StringType(), True),
    StructField("Opposit_captain", StringType(), True),
    StructField("Player_keeper", StringType(), True),
    StructField("Opposit_keeper", StringType(), True),
])

# Raw Data Path
raw_path = "/Volumes/ipl_data_analysis/source_data/raw/Player_match.csv*"

# Read source CSV
dfb = (
    spark.read.format("csv")
    .schema(player_match_schema)
    .options(header="true", delimiter=",")
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
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.bronze.brz_player_match")
)

display(dfb.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Team.csv Processing

# COMMAND ----------

# Schema Definition
team_schema = StructType([
    StructField("Team_SK", IntegerType(), True),
    StructField("Team_Id", IntegerType(), True),
    StructField("Team_Name", StringType(), True),
])

# Raw Data Path
raw_path = "/Volumes/ipl_data_analysis/source_data/raw/Team.csv*"

# Read source CSV
dfb = (
    spark.read.format("csv")
    .schema(team_schema)
    .options(header="true", delimiter=",")
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
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.bronze.brz_team")
)

display(dfb.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ipl_data_analysis.bronze

# COMMAND ----------

tables = [
    "brz_ball_by_ball",
    "brz_match",
    "brz_player",
    "brz_player_match",
    "brz_team",
]

for table in tables:
    count = spark.table(f"{catalog}.bronze.{table}").count()
    print(f"{table}: {count:,} records")
