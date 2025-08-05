-- models/1_bronze/stg_taxizone_lookup.sql

with taxizone_lookup_raw as (
    select * 
    from {{ ref('taxi_zone_lookup') }}
)

select
    cast(locationid as integer) as location_id,
    cast(borough as string) as borough,
    cast(zone as string) as zone,
    replace(SERVICE_ZONE, 'Boro', 'Green') as service_zone

from 
    taxizone_lookup_raw
where 
    location_id is not null