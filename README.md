# 🚲 Root Data Engineering Take-Home — Citi Bike Insurance Pipeline

### Author  
**Varaidzo Churu**

---

## 📦 Project Overview

This project builds a **reproducible data pipeline** that models Citi Bike’s daily insurance exposure.  
The pipeline ingests trip and weather data, applies pricing logic, and outputs validated datasets for analysis.

It performs:  
1. **Ingestion** — Citi Bike trip data and hourly weather data (via the Open-Meteo API)  
2. **Transformation** — joins trips with weather and calculates premiums  
3. **Aggregation** — computes daily earned premiums and exposures  
4. **Validation** — outputs data-quality metrics in JSON format  

**Data sources:**  
- Trips: [Citi Bike System Data](https://citibikenyc.com/system-data)  
- Weather: [Open-Meteo API](https://open-meteo.com/)  

---

## ⚙️ Tech Stack

- **DuckDB** — lightweight analytical database  
- **dbt** — SQL transformation and reproducibility framework  
- **Python** — used for data ingestion and validation  
- **Make / bash** — for setup automation  

---

## 🚀 Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/varaidzochuru/root--assessment.git

# 2. Navigate to project
cd root--assessment

# 3. Open in VS Code (recommended)
code .

# 4. Run automated setup
make setup

🧰 Prerequisites
System Requirements

For Windows users:
Install WSL 2 with Ubuntu — follow Microsoft’s official guide
.
All commands below assume you are inside an Ubuntu terminal.

Required Software:

Python ≥ 3.11

git

make (optional but recommended)

💻 Installation
Manual Installation
sudo apt update && sudo apt install python3 python3-pip -y
git clone https://github.com/varaidzochuru/root--assessment.git
cd root--assessment


Create and activate a virtual environment:

python3 -m venv venv
source venv/bin/activate


Install dependencies:

pip install duckdb dbt-core dbt-duckdb pandas requests

⚙️ Configuration
1. Virtual Environment

Always activate your environment before running any command:

source venv/bin/activate

2. dbt Profile Setup

Create or edit ~/.dbt/profiles.yml to include:

bike_insurance_duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /home/varaidzochuru/root--assessment/duckdb/bike_insurance.duckdb
      threads: 1
      extensions: []


Adjust the path to match your local DuckDB file location.

✅ Verification

Test your setup:

# Check Python
which python  # → should point to venv/bin/python

# Verify dbt
dbt --version
dbt debug     # should return "All checks passed!"

🧱 How the Pipeline Works
Data Ingestion

Citi Bike trip CSVs are downloaded manually from their open data portal.

Hourly weather data is fetched via the get_weather.py script and stored in /data/.

Transformation (dbt models)

fact_trip.sql

Joins trips to hourly weather data.

Flags rainy/windy hours (is_rainy_or_windy).

Calculates per-trip premium:

R15/classic bike, R20/electric bike, R5 base non-usage

1.2× multiplier if rainy/windy.

agg_exposure_daily.sql

data_quality_report.json — validation summary

🐍 Querying DuckDB in Python

After running dbt, you can query the DuckDB database directly:

import duckdb
conn = duckdb.connect('/home/varaidzochuru/root--assessment/duckdb/bike_insurance.duckdb')

rows = conn.execute("SELECT * FROM fact_trip LIMIT 10").fetchall()
for row in rows:
    print(row)
    print(row)

When running multiple sql checks you might have issues with duckdb locking use :
  ps aux | grep python   to check for processes causing the locks
  kill -9 [id] to kill the process

📊 Outputs
Output File	Description
bike_insurance.duckdb	local database with all models
fact_trip	enriched trip-level data joined with weather
agg_exposure_daily	daily premiums and exposure aggregation
data_quality_report.json	data validation summary

📁 Repository Structure
root--assessment/
├── data/
│   └── data_quality_report.json
|   └── weather.csv
|   └── citi_bike.csv
│
├── duckdb/
│   └── bike_insurance.duckdb
│
├── dbt_project/
│   ├── models/
        ├── marts/
│   │           ├── fact_trip.sql
│   │            └── agg_exposure_daily.sql
│       ├── staging
│   │           ├── stg_citi_bikes.sql
│   │           ├── stg_weather.sql
│   └── dbt_project.yml
│
├── scripts/
│   ├── get_weather.py
│
├── requirements.txt
└── README.md

🤝 Contributing
git checkout main && git pull

# Create feature branch
git checkout -b feature/new-feature

# Commit and push
git add .
git commit -m "feat: add new model"
git push -u origin feature/new-feature

🪄 Data Ingestion Recap

Download Citi Bike trip CSVs → data/

Run get_weather.py to download and save hourly weather data → data/

Run dbt seed to load raw CSVs into the duckdb database

Execute transformations:
To run individual models
e.g dbt run -m stg_weather 
To run all models in order:
dbt run

## Part A
1. The main trade offs with regards to using JSONB  compared to using a normalised schema is that json allows for schema-less storage, making it ideal for variable attributes for example working with the weather data as it is rather what I did in my script where I change to csv format. It supports faster ingestion and easier schema evolution because no table changes are required when attributes change.

But it is difficult to enforce data integrity and constraints such as foreign keys or datatypes and also difficult to join with normalised data in this case the citi bike data came as a csv so it was easier to work with the data in the same format. Query performance can also be compromised on large datasets since JSON must be parsed at runtime, and indexing on nested keys is limited.

JSONB is useful for flexibility and rapid ingestion, but a normalised schema is better for performance, consistency, and long-term analytical use once the schema stabilises.

2.Before sharing the data with actuaries I would start by making sure the data is  complete at ingestion of raw data I would check for any missing trip dates, product types. I would also check row count and ensure it aligns with a working average. I would also check that each ride_id is unique so there are no duplicates. I would also make sure that premiums are always be positive and match the set pricing rules (for example, R15 for a classic bike or R20 for an electric bike). I’d make sure every trip correctly maps to an existing policy and its related weather record to keep referential integrity intact.

I’d also include checks for timeliness to ensure there are no future-dated trips or invalid timestamps, and consistency checks so trip durations make sense (the start time must be before the end time). For business rules, I’d make sure the 120% weather adjustment only applies when is_rainy_or_windy = 1, and that the daily premium correctly adds up per-bike premiums plus the R5 base fee for non-usage.

3.To ensure idempotency, I would make use of primary keys such as ride_id, or composite keys like ride_id + trip_date. This would support incremental loads using a merge strategy that checks row uniqueness before inserting or updating. I’d have the merge logic look back a few days (say, three) so that any delayed or corrected data always gets updated before new rows are inserted. I’d also partition the data by trip_date to make querying faster and reprocessing easier. This setup would make sure that even if older data is re-ingested, it won’t cause duplicates — it’ll simply update existing records and add new ones as needed.

4.If data volume increases a hundredfold, several architectural upgrades are necessary. Move from local DuckDB to a distributed processing engine such as Spark or BigQuery for scalability. Store data in Parquet format and partition by trip_date. Pre-aggregate daily metrics to minimise row-level joins. Introduce indexing or Z-ordering to improve frequent query performance and consider using materialised views.

For orchestration, migrate from ad-hoc execution to Airflow with incremental dbt models. At scale, implement more rigorous data-quality enforcement using dbt tests.

5.A daily active_policy table can be designed using Slowly Changing Dimension Type 2 logic. Each record would include valid_from, valid_to, and an is_current column, where is_current is a boolean (1 for current, 0 for not current). These fields define the date range during which a specific policy state is active. When a policy changes for example, it’s renewed, lapses, or gets cancelled  the existing record is closed by setting valid_to = change_date - 1 and is_current = 0. A new record is then inserted with the updated details, valid_from = change_date, and is_current = 1. This setup makes it easy to track policy history and see which version was active at any given time.

On any date, active policies can be retrieved using:
WHERE is_current = 1

This approach can be implemented in dbt using the is_incremental() macro, with a composite key made up of (policy_id, valid_from) to track each policy’s state changes over time.

##Part B
4. Even if utilisation stays the same, the season still matters. Premiums go up in wetter or windier months because the 120% weather adjustment kicks in more often. Whereas in dry seasons most days would use the base rate, so premiums drop. With the same level of usage, seasonal weather differences naturally cause the premiums to fluctuate.

Things to note:
I only used 100 rows of the citi_bike data from September because there were too many rows and vs code kept on crashing.
I would have ingested all the files using Airflow and used a better db for example databricks to manage this but because of the Duckdb requirement I kept the load minimal.
I kept the weather and citi_bike csv but emptied them when pushing to remote.

Not an improvement but suggestion for the challenge:
Is to allow candidates to use their own database tools as someone who had never worked with duckdb, I spent quite a bit of time trying to understand and it, in terms of installations as well.