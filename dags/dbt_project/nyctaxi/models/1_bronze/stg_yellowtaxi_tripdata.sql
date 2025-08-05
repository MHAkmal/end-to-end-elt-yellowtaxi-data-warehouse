-- models/1_bronze/stg_yellow_tripdata.sql

with combined_data as (

    {{ dbt_utils.union_relations(
        relations=[
            source('nyctaxi_bronze', 'yellowtaxi_2021'),
            source('nyctaxi_bronze', 'yellowtaxi_2022'),
            source('nyctaxi_bronze', 'yellowtaxi_2023'),
            source('nyctaxi_bronze', 'yellowtaxi_2024')
        ]
    ) 
    }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as trip_id,
        cast(vendorid as integer) as vendor_id,
        cast(ratecodeid as integer) as rate_code_id,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        cast(payment_type as integer) as payment_type_id,
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric(10, 2)) as trip_distance,
        cast(fare_amount as numeric(10, 2)) as fare_amount,
        cast(extra as numeric(10, 2)) as extra,
        cast(mta_tax as numeric(10, 2)) as mta_tax,
        cast(tip_amount as numeric(10, 2)) as tip_amount,
        cast(tolls_amount as numeric(10, 2)) as tolls_amount,
        cast(improvement_surcharge as numeric(10, 2)) as improvement_surcharge,
        cast(total_amount as numeric(10, 2)) as total_amount,
        cast(congestion_surcharge as numeric(10, 2)) as congestion_surcharge,
        coalesce(cast(airport_fee as numeric(10, 2)), 0) as airport_fee

    from combined_data
)

select 
    * 
from 
    renamed
where
    trip_id is not null 
    and vendor_id is not null
    and rate_code_id is not null 
    and pickup_location_id is not null 
    and dropoff_location_id is not null 
    and payment_type_id is not null 
    and store_and_fwd_flag is not null 
    and pickup_datetime is not null 
    and dropoff_datetime is not null 
    and passenger_count is not null 
    and trip_distance is not null 
    and fare_amount is not null 
    and extra is not null 
    and mta_tax is not null 
    and tip_amount is not null 
    and tolls_amount is not null 
    and improvement_surcharge is not null 
    and total_amount is not null 
    and congestion_surcharge is not null 
    and airport_fee is not null