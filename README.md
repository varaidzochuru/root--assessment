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
