-- Model: dim_air
-- Descrição: Dimensão de companhias aéreas derivada da tabela OBT.

{{ config(
    materialized = "table",
    schema       = "dbt_gold",
    tags         = ["gold", "dim", "airline"]
) }}


WITH airlines AS (
    SELECT DISTINCT
        airline_iata_code,
        airline_name
    FROM {{ ref('silver_flights') }}
    WHERE airline_iata_code IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY airline_iata_code)::BIGINT AS airline_id,
    airline_iata_code,
    airline_name
FROM airlines
ORDER BY airline_iata_code
