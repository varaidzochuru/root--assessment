
  {{ 
    config(
        materialized='table'
    ) 
}}
    
   with raw as (
   select * from {{ ref('citi_bike') }}
),

validated as (
    select
        ride_id,
        rideable_type,
        try_cast(started_at as timestamp) as started_at,
        try_cast(ended_at as timestamp) as ended_at,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual
    from raw
    where ride_id is not null
      and try_cast(started_at as timestamp) is not null
      and try_cast(ended_at as timestamp) is not null
)

select *
from validated
  
  