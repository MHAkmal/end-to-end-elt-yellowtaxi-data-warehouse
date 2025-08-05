-- models/marts/business/mart_location_summary.sql

select
    loc.borough,
    loc.zone,
    count(fct.trip_sk) as total_trips,
    sum(fct.total_amount) as total_revenue,
    -- Calculate average tip percentage, avoiding division by zero
    cast( avg(
            case
                when fct.fare_amount > 0 then (fct.tip_amount / fct.fare_amount) * 100
                else 0
            end
        ) as numeric(10, 2)
    ) as average_tip_percentage

from {{ ref('fct_trip') }} fct
join {{ ref('dim_location') }} loc on fct.pickup_location_sk = loc.location_sk
group by 1, 2
order by total_trips desc

