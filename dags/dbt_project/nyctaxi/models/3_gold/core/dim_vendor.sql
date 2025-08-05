

select  
    {{ dbt_utils.generate_surrogate_key(['vendor_id']) }} as vendor_sk,
    vendor_id,
    case vendor_id
        when 1 then 'Creative Mobile Technologies, LLC'
        when 2 then 'Curb Mobility, LLC'
        when 6 then 'Myle Technologies Inc'
        when 7 then 'Helix'
        else 'Not available'
    end as vendor_name
from (select distinct vendor_id from {{ ref('stg_yellowtaxi_tripdata') }})