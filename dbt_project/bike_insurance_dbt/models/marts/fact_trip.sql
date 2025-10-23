  {{ 
    config(
        materialized='table'
    ) 
}}

      with trips as (
    select 
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        started_at::date as trip_date,
        date_diff('minute', started_at, ended_at) as duration_minutes,
        case when date_diff('minute', started_at, ended_at) > 0 then 1 else 0 end as in_use
     from {{ ref('stg_citi_bikes') }}
),

weather as (
    select
        hour_ts as weather_time,
        hour_ts::date as weather_date,
        precipitation,
        wind_speed_10m,
        relative_humidity_2m,
case 
    when precipitation > 0 or wind_speed_10m > 10 then 1 
    else 0 
end as is_rainy_or_windy
    from {{ ref('stg_weather') }}
)

select
    t.*,
    w.is_rainy_or_windy,
    case 
        when t.in_use = 1 then
            case 
                when t.rideable_type = 'classic_bike' then 15
                when t.rideable_type = 'electric_bike' then 20
                else 5
            end
        else 5
    end 
    *
    case when coalesce(w.is_rainy_or_windy, 0) = 1 then 1.2 else 1 end
    as premium
from trips t
left join weather w
    on date_trunc('hour', t.started_at) = w.weather_time
  
  