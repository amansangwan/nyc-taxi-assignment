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

    round(
        avg(total_trips) over (
            order by pickup_hour
            rows between 2 preceding and current row
        ),
        2
    )                                               as trips_3h_rolling_avg

from hourly
order by pickup_hour;
