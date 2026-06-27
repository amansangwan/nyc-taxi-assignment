

select

    pickup_location_id,
    cast(date_trunc('month', pickup_datetime) as date)  as trip_month,

    pickup_zone,

    count(*)                                            as total_trips,
    avg(trip_distance)                                  as avg_trip_distance,
    avg(fare_amount)                                    as avg_fare,
    sum(total_amount)                                   as total_revenue,

    rank() over (
        partition by date_trunc('month', pickup_datetime)
        order by sum(total_amount) desc
    )                                                   as revenue_rank

from {{ ref('fct_trips') }}
group by
    pickup_location_id,
    date_trunc('month', pickup_datetime),
    pickup_zone
