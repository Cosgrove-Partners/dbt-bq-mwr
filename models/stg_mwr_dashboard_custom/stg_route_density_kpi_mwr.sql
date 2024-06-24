select
    case
        trim(left(vehicle, instr(vehicle, ' ')))
        when 'WTR'
        then
            case
                when instr(vehicle, 'SALES') > 0
                then 'Water Inspectors'
                when instr(vehicle, 'Monitor') > 0
                then 'Water Monitors'
                when instr(vehicle, 'VAN') > 0
                then 'Water Other'
                when instr(vehicle, 'Carpet') > 0
                then 'Water Other'
                else 'Water Leads'
            end
        else
            trim(
                coalesce(
                    nullif(left(trim(vehicle), instr(vehicle, ' ')), ''), 'Not Informed'
                )
            )
    end as division,
    vehicle,
    initcap(type) as type,
    datetime_trunc(cast(date as datetime), week) as week_date,
    start_time,
    end_time,
    date_diff(
        cast(end_time as datetime), cast(start_time as datetime), minute
    ) as total_duration,
    round(driving_duration / 60.0, 2) as driving_duration,
    round(parked_duration / 60.0, 2) as parked_duration,
    start_location,
    end_location,
    distance_miles,
    odometer_miles
from {{ source("google_sheets", "route_density_mwr") }}
