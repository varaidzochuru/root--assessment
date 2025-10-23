
  
import json
import pandas as pd

def model(dbt, session):
    print("Running data_quality_report model...")

    fact_trip = session.execute("SELECT * FROM main.fact_trip").fetchdf()
    citi_bike = session.execute("SELECT * FROM main.citi_bike").fetchdf()
    weather = session.execute("SELECT * FROM main.weather").fetchdf()

    report = {
        "missing_started_at": int(citi_bike["started_at"].isnull().sum()),
        "missing_ended_at": int(citi_bike["ended_at"].isnull().sum()),
        "missing_precipitation_records": int(weather["precipitation"].isnull().sum()),
        "missing_wind_records": int(weather["wind_speed_10m"].isnull().sum()),
        "invalid_durations": int((fact_trip["duration_minutes"] <= 0).sum()),
        "invalid_rideable_types": list(set(fact_trip["rideable_type"].unique()) - {"classic_bike","electric_bike"}),
        "negative_premiums": int((fact_trip["premium"] < 0).sum()),
        "min_trip_date": str(fact_trip["trip_date"].min()),
        "max_trip_date": str(fact_trip["trip_date"].max()),
        "rainy_day_premium_mean": float(fact_trip.loc[fact_trip["is_rainy_or_windy"] == 1, "premium"].mean()),
        "normal_day_premium_mean": float(fact_trip.loc[fact_trip["is_rainy_or_windy"] == 0, "premium"].mean()),
        "total_trips": len(fact_trip),
        "nulls_in_premium": int(fact_trip["premium"].isnull().sum()),
        "avg_premium": float(fact_trip["premium"].mean()),
        "rows_with_missing_weather": int(fact_trip["is_rainy_or_windy"].isnull().sum()),
        "total_days": int(fact_trip["trip_date"].nunique()),
    }

    output_path = "/home/varaidzochuru/root--assessment/data/data_quality_report.json"
    print(f"Writing data quality report to {output_path}")
    with open(output_path, "w") as f:
        json.dump(report, f, indent=4)

    empty_df = pd.DataFrame(columns=["status"])
    return empty_df