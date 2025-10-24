SELECT 
  trip_date,
  SUM(premium) AS total_premium,
  COUNT(DISTINCT ride_id) AS active_rides,
  CASE 
    WHEN AVG(is_rainy_or_windy) > 0 THEN 'Rainy/Windy'
    ELSE 'Normal'
  END AS weather
FROM {{ ref('fact_trip') }}
GROUP BY trip_date