-- models/marts/business/mart_trip_summary_hourly.sql

select
    dt.hour,
    dt.day,
    count(fct.trip_sk) as total_trips,
    cast(avg(fct.total_amount) as numeric(10, 2)) as average_fare,
    sum(fct.total_amount) as total_revenue
from {{ ref('fct_trip') }} fct
join {{ ref('dim_datetime') }} dt on fct.datetime_sk = dt.datetime_sk
group by 1, 2
order by 1, 2