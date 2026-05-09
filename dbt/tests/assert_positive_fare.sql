-- Singular test: assert that no rows in stg_yellow_trips have a non-positive
-- fare_amount.  dbt considers this test passing when the query returns 0 rows.
--
-- The staging model filters fare_amount <= 0 at build time, so this is a
-- belt-and-suspenders check that the filter was not accidentally removed.

select
    vendor_id,
    pickup_datetime,
    pickup_location_id,
    fare_amount
from {{ ref('stg_yellow_trips') }}
where fare_amount <= 0
