with source as (
    select * from {{ ref('taxi_zone_lookup') }}
)

select
    cast(locationid as integer) as location_id,
    borough                     as borough,
    zone                        as zone,
    service_zone                as service_zone
from source
