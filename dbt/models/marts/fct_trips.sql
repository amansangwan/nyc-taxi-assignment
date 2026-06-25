-- Mart fact: one row per valid trip (grain: 1 row per trip, PK trip_id).
-- The central fact of the star schema. Reads from int_trips_enriched, which has
-- already enriched + filtered, so this model just selects the public column set
-- in a stable order. Materialized as a table: BI hits it repeatedly, build once.
--
-- pickup_location_id / dropoff_location_id are FKs into dim_zones (tested via a
-- relationships test, not a build dependency).

select
    -- primary key
    trip_id,

    -- foreign keys into dim_zones
    pickup_location_id,
    dropoff_location_id,

    -- denormalised zone attributes (carried for query convenience)
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,

    -- event timestamps
    pickup_datetime,
    dropoff_datetime,

    -- measures
    trip_duration_minutes,
    trip_distance,
    passenger_count,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type

from {{ ref('int_trips_enriched') }}
