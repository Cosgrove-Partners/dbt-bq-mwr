with
    fedex_data as (
        select
            datetime_trunc(coalesce(date_started, date_closed), month) as date,
            if(date_started is not null, 1, 0) as started,
            if(date_closed is not null, 1, 0) as closed,
            total_invoiced,
            1 as jobs,
            'MWR' as company
        from {{ ref("stg_fedex_kpi") }}
    ),
    fedex_monthly as (
        select
            company,
            date,
            sum(started) as started,
            sum(closed) as closed,
            avg(if(total_invoiced > 0, total_invoiced, null)) as total_invoiced,
            sum(jobs) as jobs
        from fedex_data
        group by company, date
    ),
    fedex_mwr as (
        select
            company,
            avg(started) as avg_monthly_jobs_started,
            avg(total_invoiced) as avg_monthly_invoice_size,
            avg(closed) / avg(jobs) as closed_rate
        from fedex_monthly
        where date >= date_sub(current_date(), interval 12 month)
        group by company
    ),
    raw_data as (
        select
            branch as company,
            rev as total_income,
            gp as gross_profit,
            overtime as overtime,
            total_labor as total_labor,
            ar_o60 as ar_o60_value,
            ar_total as ar_total,
            first_value(emp) over (order by date desc) as employee_count,
            0 as avg_monthly_job_starts_mitigation,
            0 as avg_monthly_job_size_mitigation,
            0 as close_rate_mitigation
        from {{ ref("stg_monthly_kpi_magna_mwr") }}
        where date >= date_sub(current_date(), interval 12 month)
        union all
        select
            branch as company,
            rev as total_income,
            gp as gross_profit,
            overtime as overtime,
            total_labor as total_labor,
            ar_o60 as ar_o60_value,
            ar_total as ar_total,
            first_value(emp) over (order by date desc) as employee_count,
            0 as avg_monthly_job_starts_mitigation,
            0 as avg_monthly_job_size_mitigation,
            0 as close_rate_mitigation
        from {{ ref("stg_monthly_kpi_water_mwr") }}
        where date >= date_sub(current_date(), interval 12 month)
        union all
        select
            branch as company,
            rev as total_income,
            gp as gross_profit,
            overtime as overtime,
            total_labor as total_labor,
            ar_o60 as ar_o60_value,
            ar_total as ar_total,
            first_value(emp) over (order by date desc) as employee_count,
            (
                select avg_monthly_jobs_started from fedex_mwr
            ) as avg_monthly_job_starts_mitigation,
            (
                select avg_monthly_invoice_size from fedex_mwr
            ) as avg_monthly_job_size_mitigation,
            (select closed_rate from fedex_mwr) as close_rate_mitigation
        from {{ ref("stg_monthly_kpi_mwr_mwr") }}
        where date >= date_sub(current_date(), interval 12 month)
    )
select
    company,
    sum(total_income) as total_income,
    sum(gross_profit) as gross_profit,
    sum(overtime) as overtime,
    sum(total_labor) as total_labor,
    avg(ar_o60_value) as ar_o60_value,
    avg(ar_total) as ar_total,
    avg(employee_count) as employee_count,
    avg(avg_monthly_job_starts_mitigation) as avg_monthly_job_starts_mitigation,
    avg(avg_monthly_job_size_mitigation) as avg_monthly_job_size_mitigation,
    avg(close_rate_mitigation) as close_rate_mitigation
from raw_data
group by company
union all
select *
from {{ source("google_sheets", "acquisition_comparison_kpi_other_mwr") }}
where company is not null
