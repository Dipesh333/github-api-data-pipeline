**GitHub Data Engineering Pipeline (API → Raw → Silver → Gold)**
**Overview**

This project demonstrates an end-to-end Data Engineering pipeline built using Python.
It ingests data from the GitHub REST API, stores it in a layered architecture (Raw → Silver → Gold), and models the data using Kimball dimensional modeling principles.

The focus of this project is correct data engineering design, not tool overuse.

**Key Objectives**

Build a reliable API ingestion pipeline

Apply proper data layering (Raw / Silver / Gold)

Design fact and dimension tables (Kimball)

Implement logging and error handling correctly

Demonstrate production-ready thinking without overengineering

**Architecture**
GitHub API
   ↓
Extract Layer (Python, requests)
   ↓
Raw Layer (JSON, immutable, partitioned)
   ↓
Silver Layer (clean, flattened, business-ready)
   ↓
Gold Layer (Fact & Dimensions - Kimball)

Data Layers Explained
1️⃣**Extract Layer**

Fetches repository metadata and commits from GitHub API

Handles:

Authentication

Pagination

Incremental fetching using a lookback window

Logs progress and failures

2️⃣**Raw Layer (JSON)**

Stores API responses as-is

Partitioned by run_date

Immutable

Includes _extracted_at timestamp for lineage

Example:

data/raw/commits/run_date=2026-02-03/commits.json


Why JSON?
Preserves nested structure and avoids data loss.

3️⃣ **Silver Layer (CSV)**

Reads raw JSON

Flattens nested structures

Applies a clean, stable schema

Adds explicit business context (e.g., repo_name)

Example:

data/silver/commits/commits.csv


Silver represents analyzable, trusted data.

4️⃣ **Gold Layer (Kimball Model)**

The Gold layer follows a star schema.

**Dimensions**

dim_author – commit authors

dim_repo – repositories

dim_date – calendar attributes

**Fact**

fact_commits – one row per commit

Each fact row references dimensions via surrogate keys.

Example:

data/gold/
├── dim_author.csv
├── dim_repo.csv
├── dim_date.csv
└── fact_commits.csv

Kimball Design Decisions

Grain: one row per commit

Fact: commit events

Dimensions: author, repository, date

Surrogate keys: used in Gold layer

Degenerate dimension: commit message remains in fact

This design supports questions like:

Commits per author per day

Activity trends over time

Repository contribution analysis

Logging & Error Handling

Logging is implemented at:

Pipeline level (main.py)

Extract layer (API calls)

Errors are logged with full stack traces

Pipeline follows a fail-fast approach

Transform layers intentionally do not suppress errors

Logs are written to:

logs/pipeline.log

Storage Choices

CSV is used intentionally for Silver and Gold layers for simplicity and portability.

The schema and modeling are database-ready.

In production, Silver and Gold layers would be stored in a data warehouse (e.g., Postgres, Snowflake, BigQuery).

**How to Run
Prerequisites**

Python 3.10+

GitHub personal access token

**Setup**
pip install -r requirements.txt
export GITHUB_TOKEN=your_token_here

**Run Pipeline**
python -m src.main

**Project Structure**
project_root/
├── src/
│   ├── extract/
│   ├── load/
│   ├── transform/
│   ├── gold/
│   └── main.py
├── data/
│   ├── raw/
│   ├── silver/
│   └── gold/
├── logs/
│   └── pipeline.log
└── README.md

**Key Learnings**

Importance of data layering

Why Raw storage must preserve structure

How Silver enables consistent modeling

How Gold enables analytics

Proper placement of logging and error handling

Engineering trade-offs over tool obsession

Future Improvements

Replace CSV with a database or warehouse

Add orchestration with Airflow

Add data quality checks

Support multiple repositories

Add incremental Gold builds

**Final Note**

This project emphasizes engineering judgment and correct design decisions over unnecessary complexity.
It reflects how real-world data pipelines are planned, built, and evolved.
