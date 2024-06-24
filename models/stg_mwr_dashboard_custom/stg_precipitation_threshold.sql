with
    raw_data as (
        select
            cast(a.ingestion_time as date) as part_date,
            cast(
                parse_datetime('%Y-%m-%d %H:%M:%S', a.dt_txt) as datetime
            ) as event_date,
            ifnull(a.rain_3h / 25.4, 0) as precipitation
        from {{ source("weather", "forecast_weather_api") }} a
    ),
    raw_grouped_data as (
        select
            datetime_trunc(event_date, day) as event_date,
            round(sum(precipitation), 3) as precipitation
        from raw_data a
        where
            a.part_date = (
                select max(m.part_date)
                from raw_data m
                where m.event_date = a.event_date
            )
        group by datetime_trunc(a.event_date, day)
    ),
    raw_weather_data as (
        select precipitation, 'forecast' as type
        from raw_grouped_data a
        union all
        select
            (
                cast(
                    nullif(
                        replace(replace(dailyprecipitation, 's', ''), 'T', ''), ''
                    ) as float64
                )
            ) as precipitation,
            'historical' as type
        from {{ source("google_sheets", "weather_data") }}
    ),
    normalized_weather_data as (select precipitation from raw_weather_data),
    raw_prec_data as (
        select distinct round(precipitation, 3) as precipitation
        from normalized_weather_data
    ),
    raw_threshold_data as (
        select *, cast(floor(precipitation * 2) / 2 as string) as lower_lim
        from raw_prec_data
        where precipitation is not null
    ),
    raw_pre_final as (
        select precipitation, 0.000 as min_prec, 0.000 as max_prec, lower_lim
        from raw_threshold_data
        where precipitation <= 0
        union all
        select
            precipitation,
            min(precipitation) over (
                partition by lower_lim order by precipitation
            ) as min_prec,
            max(precipitation) over (
                partition by lower_lim order by precipitation desc
            ) as max_prec,
            lower_lim
        from raw_threshold_data
        where precipitation > 0
    ),
    raw_final as (select distinct min_prec, max_prec from raw_pre_final)
select
    ifnull(lag(max_prec) over (order by min_prec), 0) as min_prec,
    max_prec,
    concat(
        cast(ifnull(lag(max_prec) over (order by min_prec), 0) as string),
        '-',
        cast(max_prec as string)
    ) as threshold
from raw_final
