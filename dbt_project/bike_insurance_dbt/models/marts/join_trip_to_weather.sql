SELECT
    trip_date,
    rideable_type,
    COUNT(DISTINCT ride_id) AS insured_trips,
    SUM(is_rainy_or_windy) AS rainy_or_windy_hours,
    CASE 
        WHEN SUM(is_rainy_or_windy) > 0 THEN 'Rainy/Windy'
        ELSE 'Normal'
    END AS weather_bucket
FROM {{ ref('fact_trip') }}
WHERE in_use = 1
GROUP BY trip_date, rideable_type
ORDER BY trip_date
