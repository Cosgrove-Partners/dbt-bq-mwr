with
    raw_weather_data as (
        select
            date_trunc(cast(nullif(date, 'null') as datetime), day) as event_date,
            cast(
                replace(dailydeparturefromnormalaveragetemperature, 's', '') as float64
            ) as depart_from_avg_temp,
            cast(
                replace(dailyaveragedrybulbtemperature, 's', '') as float64
            ) as avg_temp,
            cast(
                replace(dailyminimumdrybulbtemperature, 's', '') as float64
            ) as min_temp,
            cast(
                replace(dailymaximumdrybulbtemperature, 's', '') as float64
            ) as max_temp,
            ifnull(
                cast(
                    nullif(
                        replace(replace(dailyprecipitation, 's', ''), 'T', ''), ''
                    ) as float64
                ),
                0
            ) as precipitation
        from {{ source("google_sheets", "weather_data") }}
    ),
    normalized_weather_data as (
        select
            event_date,
            depart_from_avg_temp,
            avg_temp,
            min_temp,
            max_temp,
            round(precipitation, 3) as precipitation
        from raw_weather_data
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
    raw_yearly_data as (
        select extract(year from received_date) as event_year, avg(leads) as yearly_avg
        from raw_job_data
        group by extract(year from received_date)
    ),
    raw_yearlyw_data as (
        select extract(year from received_date) as event_year, avg(leads) as yearly_avg
        from raw_job_data
        where extract(dayofweek from received_date) in (2, 3, 4, 5, 6)
        group by extract(year from received_date)
    ),
    raw_tagged_weather_data as (
        select
            *,
            row_number() over (order by event_date) as row_num,
            if(precipitation > 0, true, false) is_precipitating,
            case
                when lag(precipitation) over (order by event_date) > 0
                then true
                else false
            end as is_previous_precipitating,
            lag(precipitation) over (order by event_date) as previous_precipitation
        from normalized_weather_data r
    ),
    raw_data as (
        select
            w.*,
            ifnull(j.leads, 0) as leads,
            ifnull(j.total_estimates, 0) as total_estimates,
            ifnull(yj.yearly_avg, 0) as yearly_avg,
            if(
                ifnull(yj.yearly_avg, 0) > 0, j.leads / yj.yearly_avg, 0
            ) as increase_pct,
            floor(w.min_temp) + floor(w.precipitation) as forecast_key,
            (
                select yearly_avg
                from raw_yearly_data
                where event_year = extract(year from current_date())
            ) as ytd_avg,
            (
                select yearly_avg
                from raw_yearlyw_data
                where event_year = extract(year from current_date())
            ) as weekday_ytd_avg,
            ifnull(ywj.yearly_avg, 0) as weekday_yearly_avg,
            ifnull(p.threshold, '-') as curr_threshold,
            ifnull(pr.threshold, '-') as prev_threshold
        from raw_tagged_weather_data w
        inner join raw_job_data j on w.event_date = j.received_date
        left join raw_yearly_data yj on extract(year from w.event_date) = yj.event_year
        left join
            raw_yearlyw_data ywj on extract(year from w.event_date) = ywj.event_year
        left join
            {{ ref("stg_current_precipitation_threshold") }}
            p on w.precipitation >= p.min_prec and w.precipitation < p.max_prec
        left join
            {{ ref("stg_precipitation_threshold") }}
            pr
            on w.previous_precipitation >= pr.min_prec
            and w.previous_precipitation < pr.max_prec
    )
select
    *,
    case
        when (is_precipitating and not (is_previous_precipitating))
        then
            avg(leads) over (
                order by event_date rows between current row and 3 following
            )
        else 0
    end as forecast_leads,
    case
        when (is_precipitating and not (is_previous_precipitating)) then true else false
    end as precipitation_event,
    extract(dayofweek from event_date) in (2, 3, 4, 5, 6) as weekday
from raw_data
order by event_date
