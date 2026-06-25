-- Mart dimension: one row per TLC taxi zone (grain: 1 row per zone, PK location_id).
-- Built from the zone lookup, NOT from facts -- fct_trips relates to this by FK
-- (pickup/dropoff_location_id), a tested relationship rather than a build dependency.
-- Materialized as a table: small, queried often, joined by BI.

select
    location_id,
    zone,
    borough,
    service_zone
from {{ ref('stg_taxi_zones') }}
