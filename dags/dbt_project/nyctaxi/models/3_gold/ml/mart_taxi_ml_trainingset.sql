-- models/marts/ml/mart_ml_training_set.sql
-- Creates a denormalized table for predicting total_amount or trip_duration.

select
    -- Target Variables
    fct.total_amount,
    fct.trip_duration_minutes,
    -- Features
    dt.year as pickup_year,
    dt.month as pickup_month,
    dt.day as pickup_day,
    dt.hour as pickup_hour,
    fct.passenger_count,
    fct.trip_distance,
    rc.rate_code_name,
    pt.payment_type_name,
    ploc.borough as pickup_borough,
    ploc.zone as pickup_zone,
    dloc.borough as dropoff_borough,
    dloc.zone as dropoff_zone
    
from {{ ref('fct_trip') }} fct
join {{ ref('dim_datetime') }} dt on fct.datetime_sk = dt.datetime_sk
join {{ ref('dim_location') }} ploc on fct.pickup_location_sk = ploc.location_sk
join {{ ref('dim_location') }} dloc on fct.dropoff_location_sk = dloc.location_sk
join {{ ref('dim_rate_code') }} rc on fct.rate_code_sk = rc.rate_code_sk
join {{ ref('dim_payment_type') }} pt on fct.payment_type_sk = pt.payment_type_sk
where fct.total_amount > 0 and fct.trip_duration_minutes > 0