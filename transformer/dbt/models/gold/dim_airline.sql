{{ config(
    schema = "dbt_gold",
    materialized = "table",
    tags = ["gold", "dim", "airline"]
) }}

with airlines as (
    select distinct
        airline_iata_code,
        airline_name
    from {{ ref('silver_flights') }}
    where airline_iata_code is not null
)

select
    row_number() over (order by airline_iata_code)::bigint as airline_id,
    airline_iata_code,
    airline_name
from airlines
order by airline_iata_code
