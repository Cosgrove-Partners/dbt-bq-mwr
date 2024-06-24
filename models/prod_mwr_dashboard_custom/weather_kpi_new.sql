select * from {{ ref("stg_weather_kpi") }} where event_date is not null
