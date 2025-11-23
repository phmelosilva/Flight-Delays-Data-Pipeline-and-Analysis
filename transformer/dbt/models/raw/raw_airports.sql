{{ config(
    schema = "dbt_raw",
    materialized="view",
    tags=["raw"]
) }}

select
    iata_code,
    airport,
    city,
    state,
    country,
    latitude,
    longitude
from {{ source('raw', 'airports') }}
