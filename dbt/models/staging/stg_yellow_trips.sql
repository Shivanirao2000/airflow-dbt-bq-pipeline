with source as (

    select * from {{ source('nyc_taxi_raw', 'yellow_trips_raw') }}

),

renamed_and_cast as (

    select
        -- identifiers
        cast(VendorID          as integer)  as vendor_id,
        cast(PULocationID      as integer)  as pickup_location_id,
        cast(DOLocationID      as integer)  as dropoff_location_id,
        cast(RatecodeID        as integer)  as rate_code_id,
        cast(payment_type      as integer)  as payment_type,

        -- timestamps
        cast(tpep_pickup_datetime  as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip details
        coalesce(cast(passenger_count as integer), 1) as passenger_count,
        cast(trip_distance     as numeric)  as trip_distance,
        store_and_fwd_flag,

        -- fares
        cast(fare_amount            as numeric) as fare_amount,
        cast(extra                  as numeric) as extra,
        cast(mta_tax                as numeric) as mta_tax,
        cast(tip_amount             as numeric) as tip_amount,
        cast(tolls_amount           as numeric) as tolls_amount,
        cast(improvement_surcharge  as numeric) as improvement_surcharge,
        cast(congestion_surcharge   as numeric) as congestion_surcharge,
        cast(airport_fee            as numeric) as airport_fee,
        cast(total_amount           as numeric) as total_amount

    from source

),

with_derived as (

    select
        *,
        timestamp_diff(dropoff_datetime, pickup_datetime, minute) as trip_duration_minutes

    from renamed_and_cast

),

filtered as (

    select *
    from with_derived
    where
        trip_distance > 0
        and fare_amount > 0
        -- remove physically impossible durations
        and trip_duration_minutes > 0
        and trip_duration_minutes < 1440   -- 24 hours

)

select * from filtered
