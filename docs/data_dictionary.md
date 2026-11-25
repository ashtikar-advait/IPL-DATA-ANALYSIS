# IPL Data Engineering — Data Dictionary

## 1. Overview

This document describes the datasets, tables, attributes and relationships used in the IPL Data Engineering project.

The project processes historical IPL data (2008–2017) using Databricks and PySpark, following the Medallion Architecture.

**Catalog:** `ipl_data_analysis`

**Schemas:**
- `source_data`
- `bronze`
- `silver`
- `gold`

---

## 2. Source Datasets

Five CSV datasets are used as the source for the pipeline.

| Source File | Description | Data Domain |
|---|---|---|
| `Match.csv` | Match-level details, including date, teams, venue, toss and match result | Match |
| `Player.csv` | Player master information and attributes | Player |
| `Player_match.csv` | Player participation and match-specific information | Player-Match |
| `Team.csv` | Team identifiers and team names | Team |
| `Ball_By_Ball.csv` | Delivery-level information, runs, extras, dismissals and player references | Ball-by-Ball |

The source CSV files are stored in the Unity Catalog Volume:

`ipl_data_analysis.source_data.raw`

---

## 3. Bronze Layer

The Bronze layer stores the ingested source datasets as Delta tables.

The ingestion scripts use explicitly defined PySpark schemas and add ingestion metadata.

### Bronze Tables

| Table | Source File | Description |
|---|---|---|
| `brz_match` | Match.csv | Raw match information |
| `brz_player` | Player.csv | Raw player master data |
| `brz_player_match` | Player_match.csv | Raw player participation data |
| `brz_team` | Team.csv | Raw team information |
| `brz_ball_by_ball` | Ball_By_Ball.csv | Raw delivery-level information |

### Ingestion Metadata

The ingestion process adds the following columns:

| Column | Description |
|---|---|
| `source_file` | Source file path obtained from Spark file metadata |
| `ingested_at` | Timestamp when the data was ingested |

The Bronze layer is intended to retain source information with minimal transformation.

---

## 4. Silver Layer

The Silver layer contains cleaned and standardised versions of the Bronze datasets.

### Silver Tables

| Table | Source | Description |
|---|---|---|
| `slv_match` | `brz_match` | Standardised match information |
| `slv_player` | `brz_player` | Standardised player information |
| `slv_player_match` | `brz_player_match` | Cleaned player participation and match attributes |
| `slv_team` | `brz_team` | Standardised team information |
| `slv_ball_by_ball` | `brz_ball_by_ball` | Standardised delivery-level information |

### Transformations

| Dataset | Transformations |
|---|---|
| Match | Column-name standardisation, string trimming, categorical standardisation, garbage-value removal, null handling, date conversion and season-year conversion |
| Player | Column-name standardisation, string trimming, non-printable character removal, categorical standardisation and DOB conversion |
| Player Match | Column-name standardisation, string trimming, null handling, removal of unusable columns and dummy records, DOB conversion |
| Team | Column-name standardisation, string trimming, categorical standardisation and null handling |
| Ball by Ball | Column-name standardisation, string trimming, categorical standardisation, placeholder handling, match-date conversion and LBW label standardisation |

### Important Data Quality Decisions

**Player Match**
- `player_match_sk = -1` is treated as a dummy record and removed.
- `playermatch_key` is dropped because scientific notation in the source resulted in unrecoverable values.
- `batting_status` and `bowling_status` are dropped because they contain entirely null values.
- The original analysis identified `bowling_skill` placeholders and an actual null, with the intended standardisation to `N/A`.

**Ball by Ball**
- Duplicate `match_id` values are expected because the dataset contains multiple delivery records per match.
- `match_date` is converted from String to Date.
- `out_type` values representing LBW are standardised to `LBW`.

---

## 5. Gold Layer

The Gold layer contains the dimensional model used for analytical consumption.

The model follows a **Fact Constellation (Galaxy) Schema**, with three dimensions and two fact tables.

### 5.1 Dimension Tables

#### `dim_match`

**Purpose:** Stores match-level descriptive information.

| Attribute | Description |
|---|---|
| `match_sk` | Match surrogate/reference key |
| `match_id` | Source match identifier |
| `match_date` | Match date |
| `season_year` | IPL season |
| `venue_name` | Match venue |
| `city_name` | Match city |
| `country_name` | Match country |
| `toss_winner` | Team that won the toss |
| `toss_name` | Toss decision |
| `match_winner` | Match-winning team |
| `win_type` | Type of match victory |
| `win_margin` | Winning margin |

The table also contains other source attributes retained by the Gold transformation.

#### `dim_player`

**Purpose:** Stores player master and descriptive information.

| Attribute | Description |
|---|---|
| `player_sk` | Player surrogate/reference key |
| `player_id` | Source player identifier |
| `player_name` | Player name |
| `dob` | Date of birth |
| `batting_hand` | Batting hand |
| `bowling_skill` | Bowling style or skill |
| `country_name` | Player's country |

#### `dim_team`

**Purpose:** Stores team master information.

| Attribute | Description |
|---|---|
| `team_sk` | Team surrogate/reference key |
| `team_id` | Source team identifier |
| `team_name` | Team name |

---

### 5.2 Fact Tables

#### `fact_ball_by_ball`

**Purpose:** Stores delivery-level match information, player references, team references, runs, extras and dismissal indicators.

**Grain:** One record per delivery represented in the source ball-by-ball dataset.

| Attribute Group | Columns |
|---|---|
| Match reference | `match_sk` |
| Delivery details | `innings_no`, `over_id`, `ball_id`, `striker_batting_position` |
| Player references | `strikersk`, `nonstriker_sk`, `bowler_sk`, `player_out_sk`, `fielder_sk` |
| Match-specific player reference | `fielder_match_sk` |
| Team references | `battingteam_sk`, `bowlingteam_sk` |
| Run measures | `runs_scored`, `extra_runs`, `wides`, `legbyes`, `byes`, `noballs`, `penalty`, `bowler_extras` |
| Delivery classification | `extra_type`, `out_type` |
| Dismissal indicators | `caught`, `bowled`, `run_out`, `lbw`, `retired_hurt`, `stumped`, `caught_and_bowled`, `hit_wicket`, `obstructingfeild`, `bowler_wicket`, `keeper_catch` |

**Important:** Player and team identifiers represent different roles within the same delivery. The player and team dimensions are therefore referenced multiple times.

#### `fact_player_match`

**Purpose:** Stores player participation and match-specific attributes.

**Grain:** One record per player-match participation record represented in the Silver player-match dataset.

| Attribute | Description |
|---|---|
| `player_match_sk` | Player-match identifier |
| `match_sk` | Match reference |
| `player_sk` | Player reference |
| `player_team_sk` | Player's team reference |
| `opposit_team_sk` | Opposition team reference |
| `role_desc` | Player's role |
| `is_manofthematch` | Man-of-the-match indicator |
| `age_as_on_match` | Player age at the time of the match |
| `isplayers_team_won` | Player's team victory indicator |
| `player_captain` | Player captain information |
| `opposit_captain` | Opposition captain information |
| `player_keeper` | Player wicketkeeper information |
| `opposit_keeper` | Opposition wicketkeeper information |

---

## 6. Key Relationships

The following relationships are used in the Gold transformations.

| Fact Table | Foreign Key / Reference | Dimension |
|---|---|---|
| `fact_ball_by_ball` | `match_sk` | `dim_match` |
| `fact_ball_by_ball` | `strikersk`, `nonstriker_sk`, `bowler_sk`, `player_out_sk`, `fielder_sk` | `dim_player` |
| `fact_ball_by_ball` | `battingteam_sk`, `bowlingteam_sk` | `dim_team` |
| `fact_player_match` | `match_sk` | `dim_match` |
| `fact_player_match` | `player_sk` | `dim_player` |
| `fact_player_match` | `player_team_sk`, `opposit_team_sk` | `dim_team` |

The `fielder_match_sk` field in `fact_ball_by_ball` is derived through a match-specific lookup against the Silver player-match dataset.

---

## 7. Serving Layer Views

The Serving Layer provides enriched SQL views built on the Gold tables.

| View | Purpose |
|---|---|
| `vw_ball_by_ball_enriched` | Combines delivery information with match, player and team details |
| `vw_batting_deliveries` | Provides a batting-focused delivery-level dataset |
| `vw_bowling_deliveries` | Provides a bowling-focused delivery-level dataset |
| `vw_fact_player_match_enriched` | Combines player-match facts with player, team and match details |

All four views are created in:

`ipl_data_analysis.gold`

---

## 8. Naming Conventions

| Prefix | Meaning |
|---|---|
| `brz_` | Bronze layer table |
| `slv_` | Silver layer table |
| `dim_` | Gold dimension table |
| `fact_` | Gold fact table |
| `vw_` | Serving layer view |

**Note:** This dictionary documents the implemented processing and selected analytical attributes. For the complete physical schema and data types, refer to the table definitions and schemas in the Databricks notebooks.