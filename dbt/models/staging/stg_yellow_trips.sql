-- Staging: one clean row per raw Yellow Taxi trip (grain: 1 row per trip, PK trip_id).
-- Thin 1:1 read of the raw source: snake_case + explicit casts + a couple of light
-- derived columns. No joins, no business filtering -- that belongs downstream.
--
-- trip_id is a surrogate hash over the natural key of a trip. TLC ships exact
-- duplicate rows, so we dedup on that key (LOCKED) to keep the grain at one row
-- per trip and let the `unique` test on trip_id pass.

with source as (

    select * from {{ source('nyc_tlc', 'yellow_tripdata') }}

),

renamed as (

    select
        -- surrogate key over the natural identity of a trip
        {{ dbt_utils.generate_surrogate_key([
            'vendorid',
            'tpep_pickup_datetime',
            'tpep_dropoff_datetime',
            'pulocationid',
            'dolocationid',
            'total_amount'
        ]) }}                                                  as trip_id,

        cast(vendorid as integer)                              as vendor_id,
        cast(tpep_pickup_datetime as timestamp)                as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp)               as dropoff_datetime,
        cast(passenger_count as integer)                       as passenger_count,
        cast(trip_distance as number(38, 2))                   as trip_distance,
        cast(pulocationid as integer)                          as pickup_location_id,
        cast(dolocationid as integer)                          as dropoff_location_id,
        cast(payment_type as integer)                          as payment_type,
        cast(fare_amount as number(38, 2))                     as fare_amount,
        cast(tip_amount as number(38, 2))                      as tip_amount,
        cast(total_amount as number(38, 2))                    as total_amount,

        -- fractional minutes on purpose: datediff('minute', ...) truncates,
        -- which would distort the downstream duration validity filter.
        datediff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0
                                                               as trip_duration_minutes

    from source

)

select * from renamed

-- LOCKED: dedup exact-duplicate TLC rows on the surrogate key so the grain stays
-- one row per trip. order by total_amount is a deterministic, arbitrary tie-break.
qualify row_number() over (partition by trip_id order by total_amount) = 1
