

with ranked as (

    select
        trip_month,
        pickup_location_id,
        pickup_zone,
        total_trips,
        total_revenue,
        rank() over (
            partition by trip_month
            order by total_revenue desc
        ) as revenue_rank

    from NYC_TAXI.DBT.AGG_ZONE_PERFORMANCE
    where year(trip_month) = 2023

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
