-- models/marts/business/mart_payment_type_analysis.sql

select
    pt.payment_type_name,
    count(fct.trip_sk) as total_trips,
    cast(avg(fct.total_amount) as numeric(10, 2)) as average_total_amount,
    cast( avg(
            case
                when fct.fare_amount > 0 then (fct.tip_amount / fct.fare_amount) * 100
                else 0
            end
        ) as numeric(10, 2)
    ) as average_tip_percentage
from {{ ref('fct_trip') }} fct
join {{ ref('dim_payment_type') }} pt on fct.payment_type_sk = pt.payment_type_sk
group by 1
order by total_trips desc