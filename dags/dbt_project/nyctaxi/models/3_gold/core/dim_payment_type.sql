-- models/gold/core/dim_payment_type.sql

select
    {{ dbt_utils.generate_surrogate_key(['payment_type_id']) }} as payment_type_sk,
    payment_type_id,
    case payment_type_id
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'Null'
    end as payment_type_name
from (select distinct payment_type_id from {{ ref('int_trip_detail') }})
