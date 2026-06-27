select
    -- primary key
    trip_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    trip_distance,
    passenger_count,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type

from {{ ref('int_trips_enriched') }}
