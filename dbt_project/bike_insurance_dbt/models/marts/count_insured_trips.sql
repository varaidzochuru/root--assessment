SELECT
    trip_date,
    rideable_type,
    COUNT(DISTINCT ride_id) AS insured_trips
FROM {{ ref('fact_trip') }}
WHERE in_use = 1
GROUP BY trip_date, rideable_type
ORDER BY trip_date