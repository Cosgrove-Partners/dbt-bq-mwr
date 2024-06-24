select *
from {{ ref("stg_weekly_kpi_magna_mwr") }}
where date is not null
union all
select *
from {{ ref("stg_weekly_kpi_mwr_mwr") }}
where date is not null
union all
select *
from {{ ref("stg_weekly_kpi_water_mwr") }}
where date is not null
