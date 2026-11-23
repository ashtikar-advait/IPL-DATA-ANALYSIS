# Databricks notebook source
# MAGIC %md
# MAGIC # Facts — Gold Layer
# MAGIC
# MAGIC Builds the ball-by-ball and player-match fact tables by applying
# MAGIC the project's existing surrogate-key mapping and column-selection logic.

# COMMAND ----------

# Importing Libraries
from pyspark.sql import functions as F

# Import reusable utility functions
from utils.utils import getRowsCols, getNullReport, handleNull

# Define Catalog
catalog = "ipl_data_analysis"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Dimension Tables

# COMMAND ----------

dim_match = spark.read.table(f"{catalog}.gold.dim_match")
dim_player = spark.read.table(f"{catalog}.gold.dim_player")
dim_team = spark.read.table(f"{catalog}.gold.dim_team")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ball by Ball — Gold Fact Processing

# COMMAND ----------

# Read Silver Layer
dfs = spark.read.table(f"{catalog}.silver.slv_ball_by_ball")

print("Table: Ball by Ball")
getRowsCols(dfs)

# COMMAND ----------

# Gold transformation
def gold_layer_ball_by_ball(dataframe):
    """Remove unused columns and standardise the fielder placeholder."""

    # Drop columns not required in the Gold fact table.
    columns_to_drop = [
        "team_batting",
        "team_bowling",
        "player_out_sk",
        "fielder_match_sk",
        "source_file",
        "ingested_at",
    ]
    dfg = dataframe.drop(*columns_to_drop)

    # Replace the -1 placeholder in fielder_sk with NULL.
    dfg = handleNull(
        dfg,
        null_placeholders=[-1],
        columns=["fielder_sk"],
    )

    return dfg

# Generate Gold DataFrame
dfg = gold_layer_ball_by_ball(dfs)

# COMMAND ----------

# Map match surrogate key
dfg = dfg.join(
    dim_match.select("match_id", "match_sk"),
    on="match_id",
    how="left",
)

# COMMAND ----------

# Map player-out surrogate key
dfg = (
    dfg.join(
        dim_player.select("player_id", "player_sk"),
        F.col("player_out") == F.col("player_id"),
        "left",
    )
    .withColumnRenamed("player_sk", "player_out_sk")
    .drop("player_id")
)

# COMMAND ----------

# Map fielder match surrogate key
slv_player_match = spark.read.table(f"{catalog}.silver.slv_player_match")

dfg = dfg.join(
    slv_player_match.select(
        F.col("match_id").alias("fielder_match_id"),
        F.col("player_id").alias("fielder_player_id"),
        F.col("player_match_sk").alias("fielder_match_sk"),
    ),
    (dfg["match_id"] == F.col("fielder_match_id"))
    & (dfg["fielders"] == F.col("fielder_player_id")),
    "left",
).drop("fielder_match_id", "fielder_player_id")

# COMMAND ----------

# Select final fact columns
final_columns = [
    # Match reference
    "match_sk",

    # Delivery grain
    "innings_no",
    "over_id",
    "ball_id",

    # Player references
    "strikersk",
    "nonstriker_sk",
    "bowler_sk",
    "player_out_sk",
    "fielder_sk",

    # Match-specific fielder reference
    "fielder_match_sk",

    # Team references
    "battingteam_sk",
    "bowlingteam_sk",

    # Delivery details
    "striker_batting_position",
    "extra_type",
    "out_type",

    # Run measures
    "runs_scored",
    "extra_runs",
    "wides",
    "legbyes",
    "byes",
    "noballs",
    "penalty",
    "bowler_extras",

    # Dismissal indicators
    "caught",
    "bowled",
    "run_out",
    "lbw",
    "retired_hurt",
    "stumped",
    "caught_and_bowled",
    "hit_wicket",
    "obstructingfeild",
    "bowler_wicket",
    "keeper_catch",
]

dfg = dfg.select(*final_columns)

# COMMAND ----------

# Write Gold fact table
(
    dfg.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.fact_ball_by_ball")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Player Match — Gold Fact Processing

# COMMAND ----------

# Read Silver Layer
dfs = spark.read.table(f"{catalog}.silver.slv_player_match")

print("Table: Player Match")
getRowsCols(dfs)

# COMMAND ----------

def gold_layer_player_match(dataframe):
    """Remove ingestion metadata from the Player_Match DataFrame."""

    columns_to_drop = ["source_file", "ingested_at"]
    return dataframe.drop(*columns_to_drop)

# Generate Gold DataFrame
dfg = gold_layer_player_match(dfs)

# COMMAND ----------

# Map match surrogate key
dfg = dfg.join(
    dim_match.select("match_id", "match_sk"),
    on="match_id",
    how="left",
)

# Map player surrogate key
dfg = dfg.join(
    dim_player.select("player_id", "player_sk"),
    on="player_id",
    how="left",
)

# Map player's team surrogate key
dfg = dfg.join(
    dim_team.select(
        F.col("team_name").alias("player_team"),
        F.col("team_sk").alias("player_team_sk"),
    ),
    on="player_team",
    how="left",
)

# Map opposition team's surrogate key
dfg = dfg.join(
    dim_team.select(
        F.col("team_name").alias("opposit_team"),
        F.col("team_sk").alias("opposit_team_sk"),
    ),
    on="opposit_team",
    how="left",
)

# COMMAND ----------

# Select final fact columns
final_columns = [
    "player_match_sk",
    "match_sk",
    "player_sk",
    "player_team_sk",
    "opposit_team_sk",
    "role_desc",
    "is_manofthematch",
    "age_as_on_match",
    "isplayers_team_won",
    "player_captain",
    "opposit_captain",
    "player_keeper",
    "opposit_keeper",
]

dfg = dfg.select(*final_columns)

# COMMAND ----------

# Write Gold fact table
(
    dfg.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.fact_player_match")
)
