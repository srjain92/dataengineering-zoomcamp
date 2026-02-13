with tripdata as 
(
select *
from {{ source('staging', 'fhv_taxi_data_2019') }}
where dispatching_base_num is not null
)

select {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
       cast(dispatching_base_num as string) as dispatching_base_num,
       cast(pickup_datetime as timestamp) as pickup_datetime,
       cast(dropoff_datetime as timestamp) as dropoff_datetime,
       cast(PUlocationID as integer) as pickup_location_id,
       cast(DOlocationID as integer) as dropoff_location_id,
       cast(SR_Flag as string) as sr_flag,
       cast(Affiliated_base_number as string) as affiliated_base_number
from tripdata