# IPL Data Engineering — Project Workflow

## 1. Overview

This document describes the execution workflow of the IPL Data Engineering pipeline, from initial Databricks setup to the creation of analytics-ready Serving Layer views.

The project uses Databricks, PySpark, Delta Lake and Unity Catalog to process historical IPL datasets.

The pipeline follows the Medallion Architecture:

**Raw Data → Bronze → Silver → Gold → Serving Layer**

---

## 2. Technology and Environment

| Component | Technology |
|---|---|
| Processing platform | Databricks |
| Processing language | Python |
| Distributed processing | PySpark |
| Storage format | Delta Lake |
| Data organisation and governance | Unity Catalog |
| Analytical transformations | Databricks SQL |
| Local development | VS Code and Python virtual environment |
| Version control | Git and GitHub |

---

## 3. Initial Setup

**File:** `setup/setup.py`

The setup notebook prepares the Databricks environment.

### Activities

1. Creates the project catalog: `ipl_data_analysis`.
2. Creates the Bronze, Silver and Gold schemas.
3. Creates the `source_data` schema.
4. Creates the `raw` Volume inside `source_data`.
5. Prepares the location for uploading the source CSV files.

### Unity Catalog Structure

```text
ipl_data_analysis
│
├── source_data
│   └── raw (Volume)
│
├── bronze
├── silver
└── gold
```

The five historical IPL CSV files are uploaded to the raw Volume before ingestion.

---

## 4. Bronze Layer — Raw Data Ingestion

The Bronze layer ingests the source CSV files into Delta tables.

Processing scripts:

| Script | Responsibility |
|---|---|
| `dim_processing/01_dim_bronze.py` | Ingests Match, Player, Player_match and Team datasets |
| `facts_processing/01_facts_bronze.py` | Ingests Ball_By_Ball dataset |

### Processing Activities

- Reads source CSV files from the Unity Catalog Volume.
- Applies explicitly defined PySpark schemas.
- Reads files using CSV headers and comma delimiters.
- Adds `source_file` and `ingested_at` metadata.
- Writes the ingested datasets as Delta tables.

### Output Tables

**Dimensions**

- `bronze.brz_match`
- `bronze.brz_player`
- `bronze.brz_player_match`
- `bronze.brz_team`

**Facts**

- `bronze.brz_ball_by_ball`

The Bronze layer provides the ingested datasets for subsequent transformations.

---

## 5. Silver Layer — Cleaning and Standardisation

The Silver layer processes Bronze tables to improve consistency and data quality.

Processing scripts:

| Script | Responsibility |
|---|---|
| `dim_processing/02_dim_silver.py` | Cleans Match, Player, Player_match and Team |
| `facts_processing/02_facts_silver.py` | Cleans Ball_By_Ball |

### Common Transformations

- Standardisation of column names.
- Trimming whitespace from string columns.
- Standardisation of categorical values.
- Identification and handling of actual nulls and placeholder values.
- Removal of non-printable characters where applicable.
- Data type corrections.

### Dataset-Specific Transformations

**Match**
- Converts `match_date` from String to Date.
- Converts `season_year` from String to Integer.
- Standardises categorical values and handles missing values.

**Player**
- Removes non-printable characters.
- Standardises string values.
- Converts `dob` to Date.

**Player Match**
- Removes the dummy record where `player_match_sk = -1`.
- Drops `playermatch_key` because its original values were affected by scientific notation and were not recoverable.
- Drops `batting_status` and `bowling_status`, which were entirely null.
- Converts `dob` to Date.

**Team**
- Standardises column names and string values.
- Handles missing values.

**Ball by Ball**
- Converts `match_date` to Date.
- Standardises the `out_type` value `LBW`.
- Handles placeholder values and categorical inconsistencies.

### Output Tables

- `silver.slv_match`
- `silver.slv_player`
- `silver.slv_player_match`
- `silver.slv_team`
- `silver.slv_ball_by_ball`

---

## 6. Reusable Utility Functions

**File:** `utils/utils.py`

Reusable PySpark functions are maintained separately to reduce code duplication and support common data-processing operations.

The utility module includes functions for:

- Row and column information.
- Null and placeholder reporting.
- Data profiling.
- Column-name standardisation.
- Identification of string-type columns.
- String trimming.
- Categorical value standardisation.
- Non-printable character removal.
- Null and placeholder handling.

The utilities are imported into the Silver and Gold processing scripts wherever required.

---

## 7. Gold Layer — Dimensional Modelling

The Gold layer converts the cleaned Silver datasets into analytical fact and dimension tables.

Processing scripts:

| Script | Responsibility |
|---|---|
| `dim_processing/03_dim_gold.py` | Creates the three dimension tables |
| `facts_processing/03_facts_gold.py` | Creates the two fact tables |

### 7.1 Dimension Processing

The dimension processing script creates:

- `gold.dim_match`
- `gold.dim_player`
- `gold.dim_team`

The transformations remove ingestion metadata and prepare the dimension datasets for analytical joins.

### 7.2 Fact Processing

The fact processing script creates:

**`gold.fact_ball_by_ball`**

- Reads the Silver ball-by-ball dataset.
- Removes selected unwanted columns.
- Handles the `-1` placeholder in `fielder_sk`.
- Joins the match dimension to obtain `match_sk`.
- Joins the player dimension to obtain `player_out_sk`.
- Looks up `fielder_match_sk` using match and player references from Silver player-match data.
- Selects the final delivery-level analytical columns.

**`gold.fact_player_match`**

- Reads the Silver player-match dataset.
- Removes ingestion metadata.
- Joins the match dimension to obtain `match_sk`.
- Joins the player dimension to obtain `player_sk`.
- Joins the team dimension to obtain `player_team_sk` and `opposit_team_sk`.
- Selects the final player-match attributes.

### Final Gold Model

| Table Type | Tables |
|---|---|
| Dimensions | `dim_match`, `dim_player`, `dim_team` |
| Facts | `fact_ball_by_ball`, `fact_player_match` |

The model follows a Fact Constellation (Galaxy) Schema.

---

## 8. Serving Layer — Analytical Views

**File:** `serving_layer/serving_layer.sql`

The Serving Layer creates enriched SQL views using the Gold fact and dimension tables.

### Views Created

| View | Description |
|---|---|
| `vw_ball_by_ball_enriched` | Enriches delivery records with match, player and team information |
| `vw_batting_deliveries` | Provides a batting-focused delivery dataset |
| `vw_bowling_deliveries` | Provides a bowling-focused delivery dataset |
| `vw_fact_player_match_enriched` | Enriches player-match records with player, team and match details |

All views are created in the `ipl_data_analysis.gold` schema.

These views provide a simplified analytical interface over the dimensional model.

---

## 9. Data Quality and Validation

SQL-based validation queries are maintained separately from the PySpark processing scripts.

| File | Purpose |
|---|---|
| `sql/data_quality_checks.sql` | Checks data quality across the datasets |
| `sql/validation_queries.sql` | Validates data relationships, keys and expected records |

Validation activities include:

- Null and placeholder checks.
- Duplicate identification.
- Record-count checks.
- Key consistency checks.
- Relationship checks between matches, players and teams.
- Surrogate-key/reference validation.

The validation scripts are used to inspect the processed data and verify the expected relationships.

---

## 10. Execution Sequence

The recommended execution order is:

| Sequence | Component | Script / File |
|---:|---|---|
| 1 | Environment setup | `setup/setup.py` |
| 2 | Dimension Bronze | `01_dim_bronze.py` |
| 3 | Facts Bronze | `01_facts_bronze.py` |
| 4 | Dimension Silver | `02_dim_silver.py` |
| 5 | Facts Silver | `02_facts_silver.py` |
| 6 | Dimension Gold | `03_dim_gold.py` |
| 7 | Facts Gold | `03_facts_gold.py` |
| 8 | Serving Layer | `serving_layer.sql` |
| 9 | Validation | SQL validation scripts |

**Dependency:** Gold fact processing requires the Gold dimension tables to be available.

---

## 11. Downstream Analytics

The enriched Serving Layer views are intended to provide datasets for downstream analytical consumption.

Power BI is maintained as a separate project component for reporting and visualisation.

The reporting layer is downstream of the data engineering pipeline.

---

## 12. Project Scope and Limitations

The current implementation focuses on historical batch processing of IPL datasets.

The project does not currently claim:

- Real-time streaming ingestion.
- Automated pipeline orchestration.
- Scheduled production execution.
- Production deployment.
- A fully automated CI/CD workflow.

These may be considered as future enhancements.