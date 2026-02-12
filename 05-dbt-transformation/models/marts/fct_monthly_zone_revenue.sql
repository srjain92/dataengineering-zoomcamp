-- Data mart for monthly revenue analysis by pickup zone and service type
-- This aggregation is optimized for business reporting and dashboards
-- Enables analysis of revenue trends across different zones and taxi types

select pickup_zone,
       service_type,
       date_trunc(pickup_datetime, month) as revenue_month,
       -- Revenue calculation 
       sum(fare_amount) as revenue_monthly_fare,
       sum(extra) as revenue_monthly_extra,
       sum(mta_tax) as revenue_monthly_mta_tax,
       sum(tip_amount) as revenue_monthly_tip_amount,
       sum(tolls_amount) as revenue_monthly_tolls_amount,
       sum(ehail_fee) as revenue_monthly_ehail_fee,
       sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
       sum(total_amount) as revenue_monthly_total_amount,
       -- Additional metrics for operational analysis
       count(tripid) as total_monthly_trips,
       avg(passenger_count) as avg_monthly_passenger_count,
       avg(trip_distance) as avg_monthly_trip_distance
from {{ ref('fct_trips') }}
group by 1,2,3