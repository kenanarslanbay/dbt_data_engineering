{{ config(materialized='table') }}

with trips_data as (
    select *, 
    -- Convert pickup_datetime from nanoseconds to TIMESTAMP
    TIMESTAMP_SECONDS(CAST(pickup_datetime / 1e9 AS INT64)) as pickup_datetime_converted
    from {{ ref('fact_trips') }}
)
select 
    -- Revenue grouping 
    pickup_zone as revenue_zone,
    -- Use BigQuery's DATE_TRUNC function directly
    DATE_TRUNC(pickup_datetime_converted, MONTH) as revenue_month,

    service_type, 

    -- Revenue calculation 
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(ehail_fee) as revenue_monthly_ehail_fee,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,

    -- Additional calculations
    count(tripid) as total_monthly_trips,
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance

from trips_data
group by 1,2,3
