-- Staging: one clean row per TLC taxi zone (grain: 1 row per zone, PK location_id).
-- Thin 1:1 read of the taxi_zone_lookup seed -> snake_case names + explicit cast.
-- No joins, no filtering: this is the lookup that dim_zones and int_trips_enriched
-- both build on.

with source as (

    select * from {{ ref('taxi_zone_lookup') }}

)

select
    cast(locationid as integer) as location_id,
    borough                     as borough,
    zone                        as zone,
    service_zone                as service_zone
from source
