import requests
import pandas as pd
import os
import json

os.makedirs("data", exist_ok=True)

url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 52.52,
    "longitude": 13.41,
    # request current and hourly fields you showed
    "current_weather": "true",   # optional - returns a 'current_weather' object
    "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
    "timezone": "UTC"  # ensure timestamps are consistent; change if you want local
}

r = requests.get(url, params=params)
r.raise_for_status()
data = r.json()

# If using archive API (era5) it returns hourly similarly; adapt endpoint accordingly.

hourly = data.get("hourly", {})
times = hourly.get("time", [])

# Build dataframe, handling columns that might be absent
df = pd.DataFrame({"time": times})
for col in ["temperature_2m", "wind_speed_10m", "relative_humidity_2m", "precipitation"]:
    values = hourly.get(col)
    if values is not None:
        df[col] = values
    else:
        df[col] = pd.NA

# Save both JSON (full) and tidy CSV for dbt seeding
with open("data/weather_raw.json", "w") as f:
    json.dump(data, f, indent=2)

df.to_csv("data/weather.csv", index=False)
print("Saved: data/weather.csv and data/weather_raw.json")
