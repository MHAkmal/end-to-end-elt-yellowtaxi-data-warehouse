-- models/silver/int_trip_details_enriched.sql

with trips as (
    select * 
    from {{ ref('stg_yellowtaxi_tripdata') }}
),

pickup_zones as (
    select
        location_id,
        borough as pickup_borough,
        zone as pickup_zone
    from {{ ref('stg_taxizone_lookup') }}
),

dropoff_zones as (
    select
        location_id,
        borough as dropoff_borough,
        zone as dropoff_zone
    from {{ ref('stg_taxizone_lookup') }}
)

select
    trips.trip_id,
    trips.vendor_id,
    trips.rate_code_id,
    trips.payment_type_id,
    trips.pickup_location_id,
    trips.dropoff_location_id,
    trips.pickup_datetime,
    trips.dropoff_datetime,
    {{ dbt.datediff('trips.pickup_datetime', 'trips.dropoff_datetime', 'minute') }} as trip_duration_minutes,
    trips.passenger_count,
    trips.trip_distance,
    pz.pickup_borough,
    pz.pickup_zone,
    dz.dropoff_borough,
    dz.dropoff_zone,
    trips.fare_amount,
    trips.tip_amount,
    trips.total_amount,
    case
        when trips.fare_amount < 0 then 1
        else 0
    end as is_refunded

from 
    trips
left join pickup_zones as pz on trips.pickup_location_id = pz.location_id
left join dropoff_zones as dz on trips.dropoff_location_id = dz.location_id

