-- Model: raw_airports
-- Descrição: Expõe os dados de aeroportos da camada Raw.

{{ config(
    materialized = "view",
    schema       = "dbt_raw",
    tags         = ["raw"]
) }}

SELECT
    iata_code,
    airport,
    city,
    state,
    country,
    latitude,
    longitude
FROM {{ source('raw', 'airports') }}
