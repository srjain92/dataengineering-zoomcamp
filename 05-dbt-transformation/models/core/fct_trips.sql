{{
  config(
    materialized='incremental',
    unique_key='tripid',
    partition_by={
      "field": "pickup_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["service_type", "pickup_locationid"],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
  )
}}

select trips.tripid,
       trips.vendorid,
       trips.ratecodeid,

    -- Location details (enriched with human-readable zone names from dimension)
       trips.pickup_locationid,
       pz.borough as pickup_borough,
       pz.zone as pickup_zone,
       trips.dropoff_locationid,
       dz.borough as dropoff_borough,
       dz.zone as dropoff_zone,

    -- Trip timing
       trips.pickup_datetime,
       trips.dropoff_datetime,
       trips.store_and_fwd_flag,

    -- Trip metrics
       trips.passenger_count,
       trips.trip_distance,
       trips.trip_type,

    -- Payment breakdown
       trips.fare_amount,
       trips.extra,
       trips.mta_tax,
       trips.tip_amount,
       trips.tolls_amount,
       trips.ehail_fee,
       trips.improvement_surcharge,
       trips.total_amount,
       trips.payment_type,
       trips.congestion_surcharge,
       trips.service_type
from {{ ref('int_trips_unioned') }} as trips
inner join {{ ref('dim_zones') }} as pz on pz.locationid = trips.pickup_locationid and pz.borough != 'Unknown'
inner join {{ ref('dim_zones') }} as dz on dz.locationid = trips.dropoff_locationid and dz.borough != 'Unknown'


{% if is_incremental() %}

  -- This filter only applies on incremental runs
  where pickup_datetime > (select max(pickup_datetime) from {{ this }})

{% endif %}