{{ config(
    schema = "dbt_bronze",
    materialized="view",
    tags=["bronze"]
) }}

select
    iata_code,
    airline
from {{ source('bronze_raw', 'airlines') }}
