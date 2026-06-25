/* ============================================================================
   q3_consecutive_gap_analysis.sql
   ----------------------------------------------------------------------------
   Question: for each day in 2023, the MAX gap (in minutes) between one trip's
   dropoff and the NEXT trip's pickup that originates from the SAME pickup zone,
   and which zone that maximal gap occurred in.

   Source: NYC_TAXI.DBT.FCT_TRIPS.

   Method: order trips within each (pickup zone, day) partition by pickup time,
   use LAG to pull the previous trip's dropoff, and measure the idle gap until
   the current pickup. The first trip in each partition has a NULL lag and is
   dropped. Per day we keep the single largest gap and carry its zone.

   ----------------------------------------------------------------------------
   BRAINSTORMER -- making this performant on Snowflake at ~38M rows:

   (1) CLUSTERING KEY on (cast(pickup_datetime as date), pickup_location_id).
       The window is partitioned by exactly (day, pickup zone). Clustering the
       physical storage on the same keys co-locates each day+zone's rows into a
       small set of micro-partitions, so the engine PRUNES to those partitions
       instead of scanning all 38M rows, and each partition's LAG runs over a
       tight, already-sorted slice. This is the single biggest lever here.

   (2) RESULT CACHE. Snowflake caches a query's result for 24h; an identical
       re-run (same SQL, unchanged underlying data, cache enabled) returns
       instantly with ZERO warehouse compute. Great for a report rerun all day.

   (3) MATERIALIZE the gap intermediate. The per-trip gap (the LAG step) is the
       expensive part. Persisting it -- a dbt model / mart, or a daily-max table
       -- means repeated analysis reads pre-computed gaps instead of recomputing
       the window every time. Trade storage + a refresh for cheap reads.

   (4) SEARCH OPTIMIZATION IS THE WRONG TOOL HERE. The Search Optimization
       Service accelerates highly SELECTIVE point / equality (and some IN /
       range) lookups that return a few rows from a huge table -- e.g. "find
       trips for LocationID = 132". This query is the opposite: a FULL-POPULATION
       sequential window scan (LAG over every ordered partition) that aggregates
       across the whole table. There is no selective predicate for SOS to
       satisfy, so it would add maintenance cost and storage for no speedup.
       Physical CLUSTERING (3 above), not SOS, is the right optimization for
       partitioned window/range workloads.
   ----------------------------------------------------------------------------

   Dialect: Snowflake. Run in a Snowsight worksheet.
   ============================================================================ */

with gaps as (

    select
        cast(pickup_datetime as date)               as trip_date,
        pickup_location_id,
        pickup_zone,
        pickup_datetime,

        -- previous trip's dropoff within the SAME (zone, day), by pickup time
        lag(dropoff_datetime) over (
            partition by pickup_location_id, cast(pickup_datetime as date)
            order by pickup_datetime
        )                                           as prev_dropoff

    from NYC_TAXI.DBT.FCT_TRIPS

),

trip_gaps as (

    select
        trip_date,
        pickup_location_id,
        pickup_zone,
        -- idle minutes between the previous dropoff and this pickup
        datediff('second', prev_dropoff, pickup_datetime) / 60.0
                                                    as gap_minutes
    from gaps
    where prev_dropoff is not null                  -- drop the first trip / partition

)

-- one row per day: the largest same-zone gap and the zone it happened in
select
    trip_date,
    pickup_location_id                              as max_gap_pickup_location_id,
    pickup_zone                                     as max_gap_pickup_zone,
    round(gap_minutes, 2)                           as max_gap_minutes
from trip_gaps
where year(trip_date) = 2023
qualify row_number() over (
    partition by trip_date
    order by gap_minutes desc
) = 1
order by trip_date;
