-- models/gold/core/fct_trip.sql

with int_trip as (
    select * from {{ ref('int_trip_detail') }}
),

    datetime_dim as (
    select * from {{ ref('dim_datetime') }}
),

    location_dim as (
    select * from {{ ref('dim_location')}}    
),

    payment_type_dim as (
    select * from {{ ref('dim_payment_type')}}    
),

    rate_code_dim as (
    select * from {{ ref('dim_rate_code')}}    
),

    vendor_dim as (
    select * from {{ ref('dim_vendor')}}    
)

select
    -- Surrogate Keys
    {{ dbt_utils.generate_surrogate_key(['int_trip.trip_id']) }} as trip_sk,
    datetime_dim.datetime_sk,
    pickup_dim.location_sk  AS pickup_location_sk,
    dropoff_dim.location_sk AS dropoff_location_sk,
    payment_type_dim.payment_type_sk,
    rate_code_dim.rate_code_sk,
    vendor_dim.vendor_sk,
    -- measures
    int_trip.trip_duration_minutes,
    int_trip.passenger_count,
    int_trip.trip_distance,
    int_trip.fare_amount,
    int_trip.tip_amount,
    int_trip.total_amount,
    int_trip.is_refunded

from int_trip
left join datetime_dim 
    on int_trip.pickup_datetime = datetime_dim.datetime_id
left join location_dim as pickup_dim 
    on int_trip.pickup_location_id = pickup_dim.location_id
left join location_dim as dropoff_dim 
    on int_trip.pickup_location_id = dropoff_dim.location_id
left join payment_type_dim 
    on int_trip.payment_type_id = payment_type_dim.payment_type_id
left join rate_code_dim 
    on int_trip.rate_code_id = rate_code_dim.rate_code_id
left join vendor_dim 
    on int_trip.vendor_id = vendor_dim.vendor_id 
