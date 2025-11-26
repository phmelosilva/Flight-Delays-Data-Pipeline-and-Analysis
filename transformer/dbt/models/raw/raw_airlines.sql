-- Model: raw_airlines
-- Descrição: Expõe a tabela de companhias aéreas da camada Raw.

{{ config(
    materialized = "view",
    schema       = "dbt_raw",
    tags         = ["raw"]
) }}

SELECT
    iata_code,
    airline
FROM {{ source('raw', 'airlines') }}
