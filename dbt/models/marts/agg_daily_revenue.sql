-- Mart aggregate: daily revenue summary (grain: 1 row per day, PK trip_date).
-- Rolls fct_trips up to the calendar day for trend/BI reporting.
-- Materialized as a table.

select
    cast(pickup_datetime as date)               as trip_date,

    count(*)                                     as total_trips,
    sum(fare_amount)                             as total_fare,
    avg(fare_amount)                             as avg_fare,
    sum(tip_amount)                              as total_tips,

    -- LOCKED: tips as a % of FARE (not total), with a divide-by-zero guard.
    -- nullif(...,0) makes a zero-fare day return NULL rather than erroring.
    100.0 * sum(tip_amount) / nullif(sum(fare_amount), 0)
                                                 as tip_rate_pct

from {{ ref('fct_trips') }}
group by 1
