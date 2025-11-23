{{ config(
    schema = "dbt_raw",
    materialized="view",
    tags=["raw"]
) }}

select
    iata_code,
    airline
from {{ source('raw', 'airlines') }}
