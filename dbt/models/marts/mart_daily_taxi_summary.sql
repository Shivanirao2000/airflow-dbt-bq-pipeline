{{
    config(
        materialized='table'
    )
}}

with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

daily_summary as (

    select
        date(pickup_datetime)                               as pickup_date,
        pickup_location_id,

        count(*)                                            as total_trips,
        sum(total_amount)                                   as total_revenue,
        round(avg(fare_amount),          2)                 as avg_fare,
        round(avg(trip_distance),        2)                 as avg_trip_distance,
        round(avg(trip_duration_minutes), 1)                as avg_trip_duration_minutes,

        -- supporting metrics useful for downstream analysis
        sum(tip_amount)                                     as total_tips,
        round(avg(tip_amount / nullif(fare_amount, 0)), 4)  as avg_tip_ratio,
        sum(passenger_count)                                as total_passengers,
        countif(payment_type = 1)                           as credit_card_trips,
        countif(payment_type = 2)                           as cash_trips

    from trips
    group by 1, 2

)

select * from daily_summary
