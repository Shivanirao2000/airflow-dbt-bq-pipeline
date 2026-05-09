{% snapshot dim_taxi_zones_scd2 %}

{{
    config(
        target_schema='nyc_taxi_analytics',
        unique_key='LocationID',
        strategy='check',
        check_cols=['Zone', 'Borough'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    LocationID,
    Borough,
    Zone,
    service_zone
FROM {{ ref('taxi_zone_lookup') }}

{% endsnapshot %}