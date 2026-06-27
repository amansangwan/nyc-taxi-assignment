select
    cast(pickup_datetime as date)               as trip_date,

    count(*)                                     as total_trips,
    sum(fare_amount)                             as total_fare,
    avg(fare_amount)                             as avg_fare,
    sum(tip_amount)                              as total_tips,

    100.0 * sum(tip_amount) / nullif(sum(fare_amount), 0)
                                                 as tip_rate_pct

from {{ ref('fct_trips') }}
group by 1
