with
    raw_data as (
        select
            cast(a.ingestion_time as datetime) as part_date,
            a.main_temp_min as min_temp,
            a.main_temp_max as max_temp,
            a.main_pressure as pressure,
            cast(
                parse_datetime('%Y-%m-%d %H:%M:%S', a.dt_txt) as datetime
            ) as event_date,
            ifnull(a.rain_3h / 25.4, 0) as precipitation
        from {{ source("weather", "forecast_weather_api") }} a
    ),
    raw_grouped_data as (
        select
            datetime_trunc(event_date, day) as event_date,
            min(min_temp) as min_temp,
            max(max_temp) as max_temp,
            avg(pressure) as pressure,
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
    raw_prev_prec_data as (
        select
            *, lag(precipitation) over (order by event_date) as previous_precipitation
        from raw_grouped_data
    ),
    raw_job_data as (
        select
            date_trunc(date_received, day) as received_date,
            count(distinct job_number) as leads,
            sum(a.total_estimates) as total_estimates
        from {{ ref("stg_lead_kpi") }} a
        where division = 'Water'
        group by date_trunc(date_received, day)
    ),
    raw_final as (
        select
            a.*,
            ifnull(p.threshold, '-') as threshold,
            ifnull(pr.threshold, '-') as previous_threshold,
            ifnull(j.leads, 0) as leads
        from raw_prev_prec_data a
        left join
            {{ ref("stg_current_precipitation_threshold") }} p
            on a.precipitation >= p.min_prec
            and a.precipitation < p.max_prec
        left join
            {{ ref("stg_precipitation_threshold") }} pr
            on a.previous_precipitation >= pr.min_prec
            and a.previous_precipitation < pr.max_prec
        left join raw_job_data j on a.event_date = j.received_date
    )
select *, lag(leads) over (order by event_date) as previous_leads
from raw_final
