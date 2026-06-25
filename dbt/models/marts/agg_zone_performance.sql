-- Mart aggregate: pickup-zone performance per month
-- (grain: 1 row per (pickup_location_id, trip_month), composite PK).
-- Rolls fct_trips up to zone x month for ops reporting. Materialized as a table.

select
    -- composite primary key
    pickup_location_id,
    cast(date_trunc('month', pickup_datetime) as date)  as trip_month,

    -- carried for display (functionally dependent on pickup_location_id)
    pickup_zone,

    -- measures
    count(*)                                            as total_trips,
    avg(trip_distance)                                  as avg_trip_distance,
    avg(fare_amount)                                    as avg_fare,
    sum(total_amount)                                   as total_revenue,

    -- LOCKED: revenue rank WITHIN each month, not across the whole year.
    --
    -- Brainstormer answer: a per-month rank is the more useful business view.
    -- This table drives monthly operational decisions (where to push drivers /
    -- vehicles), and NYC demand is strongly seasonal -- a zone that ranks #3 in
    -- December can sit at #15 in July. Ranking across the whole dataset collapses
    -- those swings into a single stale annual number that hides the seasonality
    -- ops actually plans around. Per-month ranks also compose upward: they roll
    -- up into an annual leaderboard, but a single annual rank cannot be split
    -- back into months. So partitioning by month carries strictly more
    -- information and is the actionable choice.
    rank() over (
        partition by date_trunc('month', pickup_datetime)
        order by sum(total_amount) desc
    )                                                   as revenue_rank,

    -- simple ops flag: a busy zone-month
    count(*) > 10000                                    as is_high_volume_zone

from {{ ref('fct_trips') }}
group by
    pickup_location_id,
    date_trunc('month', pickup_datetime),
    pickup_zone
