{% test value_in_range(model, column_name, min_value, max_value) %}

-- Custom generic test: fails any row whose {{ column_name }} falls outside the
-- inclusive range [min_value, max_value]. Parametrised so the same test guards
-- different columns/bounds; wired on fct_trips.trip_duration_minutes and fed by
-- the project duration vars, so the test and the validity filter share one
-- source of truth. Returns offending rows; PASSES when zero come back.

select
    {{ column_name }} as offending_value
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}

{% endtest %}
