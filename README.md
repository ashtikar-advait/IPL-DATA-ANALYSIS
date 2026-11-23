# IPL Data Engineering Project

An end-to-end batch data engineering project built using **Databricks, PySpark, Delta Lake, Unity Catalog and Databricks SQL**, using historical IPL data from 2008–2017.

## Project Overview

This project transforms raw IPL datasets into structured, analytics-ready data using the Medallion Architecture.

The pipeline follows three processing layers:

- **Bronze:** Ingests raw CSV datasets into Delta tables.
- **Silver:** Cleans, standardises and prepares the ingested data.
- **Gold:** Builds dimensional and fact tables for analytical workloads.
- **Serving Layer:** Creates enriched SQL views on top of Gold tables for downstream analysis.

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Data engineering platform |
| PySpark | Distributed data processing |
| Delta Lake | Storage and table format |
| Unity Catalog | Data governance and organisation |
| Databricks SQL | Analytical views and SQL queries |
| Python | Processing logic and reusable utilities |
| Git & GitHub | Version control |

## Architecture

```text
          IPL Historical CSV Data
                    |
                    v
             Raw Data Volume
                    |
                    v
             BRONZE LAYER
          Raw Delta Tables
                    |
                    v
             SILVER LAYER
       Cleaning & Standardisation
                    |
                    v
              GOLD LAYER
        Dimensional Data Model
                    |
             +------+------+
             |             |
          Dimensions      Facts
             |             |
             +------+------+
                    |
                    v
             SERVING LAYER
           Enriched SQL Views
                    |
                    v
          Downstream Analytics
```

## Data Model

The Gold layer follows a fact constellation (galaxy) schema.

### Dimension Tables

- `dim_match`
- `dim_player`
- `dim_team`

### Fact Tables

- `fact_ball_by_ball`
- `fact_player_match`

### Serving Views

- `vw_ball_by_ball_enriched`
- `vw_batting_deliveries`
- `vw_bowling_deliveries`
- `vw_fact_player_match_enriched`

## Project Structure

```text
IPL_DATA_ANALYSIS/
│
├── architecture/
├── docs/
├── medallion_processing/
│   ├── dim_processing/
│   │   ├── dim_bronze.py
│   │   ├── dim_silver.py
│   │   └── dim_gold.py
│   │
│   └── facts_processing/
│       ├── facts_bronze.py
│       ├── facts_silver.py
│       └── facts_gold.py
│
├── serving_layer/
│
├── setup/
│
├── sql/
│   ├── data_quality_checks.sql
│   └── validation_queries.sql
│
├── utils/
│   ├── __init__.py
│   └── utils.py
│
├── powerbi/
├── requirements.txt
├── README.md
└── .gitignore
```

## Databricks Catalog Structure

```text
ipl_data_analysis
│
├── source_data
│   └── raw (Volume)
│
├── bronze
│   ├── brz_match
│   ├── brz_player
│   ├── brz_player_match
│   ├── brz_team
│   └── brz_ball_by_ball
│
├── silver
│   ├── slv_match
│   ├── slv_player
│   ├── slv_player_match
│   ├── slv_team
│   └── slv_ball_by_ball
│
└── gold
    ├── dim_match
    ├── dim_player
    ├── dim_team
    ├── fact_ball_by_ball
    └── fact_player_match
```

## Execution Sequence

1. Run the initial Databricks setup to create the catalog, schemas and raw Volume.
2. Upload the historical IPL CSV datasets into the raw Volume.
3. Execute Bronze ingestion scripts.
4. Execute Silver transformation scripts.
5. Execute Gold dimension and fact processing scripts.
6. Execute the Serving Layer SQL views.
7. Run SQL data quality checks and validation queries.

## Project Objectives

- Implement a Medallion Architecture for historical IPL data.
- Develop reusable PySpark utility functions.
- Apply data cleaning and standardisation.
- Build dimensional and fact tables.
- Create enriched SQL views for analytical consumption.
- Maintain a structured and version-controlled project repository.

## Future Scope

- Develop interactive Power BI dashboards.
- Improve pipeline execution and orchestration.
- Extend analytical capabilities using additional IPL datasets.

---

**Note:** This project uses historical IPL data from 2008–2017 and is designed as a batch data processing pipeline.