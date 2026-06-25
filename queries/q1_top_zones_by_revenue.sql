/* ============================================================================
   q1_top_zones_by_revenue.sql
   ----------------------------------------------------------------------------
   Question: the Top 10 pickup zones by revenue, for EACH month of 2023.

   Source: NYC_TAXI.DBT.AGG_ZONE_PERFORMANCE — already pre-aggregated by dbt to
   one row per (pickup zone x month). We rank within each month and keep the
   top 10.

   Performance strategy — PRE-AGGREGATION IN THE MART:
     * This query scans the ~3,076 pre-aggregated zone x month rows, NOT the
       ~35M-row fct_trips. The heavy GROUP BY already happened once, in dbt.
     * At ~3k rows the whole thing is a sub-second, in-memory sort+rank on an
       X-Small warehouse — comfortably under 30s, no fact-table scan.
     * Micro-partition pruning is effectively MOOT here: the table is far
       smaller than a single micro-partition, so there is nothing to prune.
       The performance win was paid upstream by aggregating once in the mart
       and is reused by every query that reads it.

   Dialect: Snowflake. Run in a Snowsight worksheet.
   ============================================================================ */

with ranked as (

    select
        trip_month,
        pickup_location_id,
        pickup_zone,
        total_trips,
        total_revenue,

        -- window function (visible here, per the requirement): rank zones by
        -- revenue WITHIN each month, so #1 restarts every month.
        rank() over (
            partition by trip_month
            order by total_revenue desc
        ) as revenue_rank

    from NYC_TAXI.DBT.AGG_ZONE_PERFORMANCE
    where year(trip_month) = 2023   -- drop the few stray out-of-2023 buckets

)

select
    trip_month,
    revenue_rank,
    pickup_location_id,
    pickup_zone,
    total_trips,
    total_revenue
from ranked
where revenue_rank <= 10
order by trip_month, revenue_rank;
