
  {{ 
    config(
        materialized='table'
    ) 
}}
    
    

      with raw as (
   select * from {{ ref('weather') }}
),

parsed as (
  select
    try_cast(time as timestamp) as hour_ts,
    case when temperature_2m is not null then temperature_2m::double else 0 end as temperature_2m,
    case when wind_speed_10m is not null then wind_speed_10m::double else 0 end as wind_speed_10m,
    case when relative_humidity_2m is not null then relative_humidity_2m::double else 0 end as relative_humidity_2m,
    case when precipitation is not null then precipitation::double else 0 end as precipitation
  from raw
  where try_cast(time as timestamp) is not null
)

select * from parsed

  