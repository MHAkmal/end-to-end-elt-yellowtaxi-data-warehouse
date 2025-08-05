-- models/gold/core/dim_location.sql

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id']) }} as location_sk, -- Key for a SINGLE location
    location_id,
    borough,
    zone
FROM {{ ref('stg_taxizone_lookup') }}