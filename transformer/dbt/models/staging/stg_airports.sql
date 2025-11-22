{{ config(
    schema = "dbt_bronze",
    materialized="view",
    tags=["bronze"]
) }}

select
    iata_code,
    airport,
    city,
    state,
    country,
    latitude,
    longitude
from {{ source('bronze_raw', 'airports') }}
