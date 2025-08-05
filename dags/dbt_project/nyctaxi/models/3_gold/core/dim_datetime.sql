

with extracted_datetime as (
select
    {{ dbt_utils.generate_surrogate_key(['pickup_datetime']) }} as datetime_sk,
    pickup_datetime as datetime_id,
    date(pickup_datetime) as pickup_date,
    time(pickup_datetime) as pickup_time,
    date(dropoff_datetime) as dropoff_date,
    time(dropoff_datetime) as dropoff_time,
    extract(year from pickup_datetime) as year,
    extract(month from pickup_datetime) as month,
    extract(day from pickup_datetime) as day,
    extract(hour from pickup_datetime) as hour,
    extract(dayofweekiso from pickup_datetime) as day_of_week

FROM {{ ref('int_trip_detail') }}

)

select
    datetime_sk,
    datetime_id,
    pickup_date,
    pickup_time,
    dropoff_date,
    dropoff_time,
    year,
    month,
    case month
        when 1 then 'January'
        when 2 then 'February'
        when 3 then 'March'
        when 4 then 'April'
        when 5 then 'May'
        when 6 then 'June'
        when 7 then 'July'
        when 8 then 'August'
        when 9 then 'September'
        when 10 then 'October'
        when 11 then 'November'
        when 12 then 'December'
        else 'Not a Month'
    end as month_name,
    day as date_of_month,
    case day_of_week
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
        when 7 then 'Sunday'
        else 'Not a Day'
    end as day,
    hour
    
from extracted_datetime