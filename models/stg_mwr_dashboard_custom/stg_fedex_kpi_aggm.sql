with
    data_flagged as (
        select
            branch as source,
            job_number,
            month_date_received,
            month_win_avg_total_invoiced,
            total_invoiced,
            total_job_cost,
            working_gp_pct,
            consecutive_days,
            is_win,
            is_loss,
            is_pending,
            is_inprogress,
            is_incomplete,
            lead_to_contact,
            lead_to_inspect,
            month_win_med_total_invoiced
        from {{ ref("base_fedex_kpi") }}
    ),
    data_loaded as (
        select
            source,
            month_date_received as week_date,
            count(distinct job_number) as jobs,
            count(distinct if(is_win, job_number, null)) as wins,
            sum(if(is_win, total_invoiced, 0)) as total_invoiced,
            avg(month_win_avg_total_invoiced) as avg_total_invoiced,
            avg(month_win_med_total_invoiced) as med_total_invoiced
        from data_flagged
        group by source, month_date_received
    )
select *
from data_loaded
union all
select
    source,
    week_date,
    0 as jobs,
    0 as wins,
    0 as total_invoiced,
    0 as avg_total_invoiced,
    0 as med_total_invoiced
from data_loaded
where
    week_date = (select max(week_date) from data_loaded)
    and extract(year from current_date()) + extract(month from current_date()) not in (
        select distinct extract(year from week_date) + extract(month from week_date)
        from data_loaded
    )
