/* ============================================================================
   q2_hour_of_day_pattern.sql
   ----------------------------------------------------------------------------
   Question: for each hour of the day (0-23), the total trips, average fare,
   average tip %, and a 3-hour rolling average of the trip count.

   Source: NYC_TAXI.DBT.FCT_TRIPS (one row per valid trip), bucketed by the
   hour of pickup.

   Two modelling choices worth calling out:

   1) avg_tip_pct = avg(tip_amount / nullif(fare_amount, 0)) * 100
      This is the AVERAGE OF PER-TRIP RATIOS: every trip contributes its own
      tip% equally, regardless of fare size. That is deliberately DIFFERENT from
      the ratio-of-sums sum(tip)/sum(fare)*100, which weights by fare and is
      dominated by big-fare trips. For "how do riders tip in this hour?" the
      per-trip average is the rider-centric answer. nullif guards zero-fare
      trips (they become NULL and avg() ignores them).

   2) trips_3h_rolling_avg is a TRAILING 3-hour window (current hour + 2
      preceding) over the 24 ordered hourly rows. It does NOT wrap around
      midnight: hour 0 averages just itself, hour 1 averages hours 0-1, and from
      hour 2 on it is a true 3-hour trailing mean. A trailing window is the
      natural smoother for a time-of-day curve; if a circular (wrap 23->0)
      window were wanted, you would pad the sequence (e.g. UNION the tail hours
      with an offset) before the window — intentionally not done here.

   Dialect: Snowflake. Run in a Snowsight worksheet.
   ============================================================================ */

with hourly as (

    select
        hour(pickup_datetime)                       as pickup_hour,
        count(*)                                    as total_trips,
        round(avg(fare_amount), 2)                  as avg_fare,
        round(avg(tip_amount / nullif(fare_amount, 0)) * 100, 2)
                                                    as avg_tip_pct
    from NYC_TAXI.DBT.FCT_TRIPS
    group by hour(pickup_datetime)

)

select
    pickup_hour,
    total_trips,
    avg_fare,
    avg_tip_pct,

    -- 3-hour trailing rolling average of trip count over the 24 hourly rows.
    round(
        avg(total_trips) over (
            order by pickup_hour
            rows between 2 preceding and current row
        ),
        2
    )                                               as trips_3h_rolling_avg

from hourly
order by pickup_hour;
