-- Singular test: total_amount must never be less than fare_amount.
-- total_amount = fare + tips + tolls + taxes + surcharges, all of which are >= 0,
-- so total should always be >= fare. Any row where it isn't signals a data or
-- modelling error. The test returns offending rows; it PASSES when zero come back.

select
    trip_id,
    fare_amount,
    total_amount
from {{ ref('fct_trips') }}
where total_amount < fare_amount
