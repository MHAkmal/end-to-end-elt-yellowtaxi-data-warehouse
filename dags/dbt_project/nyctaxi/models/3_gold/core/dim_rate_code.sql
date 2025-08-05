-- models/gold/core/dim_rate_code.sql

select
    {{ dbt_utils.generate_surrogate_key(['rate_code_id']) }} as rate_code_sk,
    rate_code_id,
    case rate_code_id
        when 1 then 'Standard rate'
        when 2 then 'JFK'
        when 3 then 'Newark'
        when 4 then 'Nassau or Westchester'
        when 5 then 'Negotiated fare'
        when 6 then 'Group ride'
        else 'Not available'
    end as rate_code_name
from (select distinct rate_code_id from {{ ref('stg_yellowtaxi_tripdata') }})