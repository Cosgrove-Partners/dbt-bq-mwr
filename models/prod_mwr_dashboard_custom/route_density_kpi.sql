select
    if(length(division) < 4, division, initcap(division)) as division,
    vehicle,
    type,
    case
        when start_location = 'East 38th street'
        then 'Leaving MWR'
        when end_location = 'East 38th street'
        then 'Returning to MWR'
        when start_location = 'Southyard Court'
        then 'Leaving FTW'
        when end_location = 'Southyard Court'
        then 'Returning to FTW'
        when start_location = 'Navco Drive'
        then 'Leaving LAF'
        when end_location = 'Navco Drive'
        then 'Returning to LAF'
        else 'A to B'
    end as trip_type,
    week_date,
    start_time,
    end_time,
    total_duration,
    driving_duration,
    parked_duration,
    start_location,
    end_location,
    distance_miles,
    odometer_miles
from {{ ref("stg_route_density_kpi_mwr") }}
