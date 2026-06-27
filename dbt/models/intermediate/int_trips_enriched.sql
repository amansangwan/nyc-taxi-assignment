-- Intermediate: business logic that prepares trips for the marts
-- (grain: 1 row per VALID trip, PK trip_id). Ephemeral -- dbt inlines this into
-- fct_trips / the aggs, so it never lands as its own warehouse object.
--
-- Two jobs here, both intentional:
--   1. Enrich each trip with pickup/dropoff zone + borough names.
--   2. Drop implausible trips so the marts only ever see valid records.

with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

zones as (

    select * from {{ ref('stg_taxi_zones') }}

),

enriched as (

    select
        trips.trip_id,
        trips.vendor_id,
        trips.pickup_datetime,
        trips.dropoff_datetime,
        trips.passenger_count,
        trips.trip_distance,
        trips.pickup_location_id,
        trips.dropoff_location_id,
        trips.payment_type,
        trips.fare_amount,
        trips.tip_amount,
        trips.total_amount,
        trips.trip_duration_minutes,

        coalesce(pickup_zones.zone, 'Unknown')      as pickup_zone,
        coalesce(pickup_zones.borough, 'Unknown')   as pickup_borough,
        coalesce(dropoff_zones.zone, 'Unknown')     as dropoff_zone,
        coalesce(dropoff_zones.borough, 'Unknown')  as dropoff_borough

    from trips
    left join zones as pickup_zones
        on trips.pickup_location_id = pickup_zones.location_id
    left join zones as dropoff_zones
        on trips.dropoff_location_id = dropoff_zones.location_id

)

select * from enriched
where trip_distance > 0
  and fare_amount > 0
  and passenger_count > 0
  and trip_duration_minutes between {{ var('min_trip_duration_minutes') }}
                                and {{ var('max_trip_duration_minutes') }}
