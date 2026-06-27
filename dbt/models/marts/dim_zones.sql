select
    location_id,
    zone,
    borough,
    service_zone
from {{ ref('stg_taxi_zones') }}
