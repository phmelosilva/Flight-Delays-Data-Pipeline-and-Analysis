-- Model: dim_apt
-- Descrição: Dimensão de aeroportos derivada da tabela OBT.

{{ config(
    materialized = "table",
    schema       = "dbt_gold",
    tags         = ["gold", "dim", "airport"]
) }}


-- Consolidação dos aeroportos de origem e destino a partir da silver_flights.
WITH airports_raw AS (

    SELECT DISTINCT
        origin_airport_iata_code AS airport_iata_code,
        origin_airport_name      AS airport_name,
        origin_city              AS city_name,
        origin_state             AS state_code,
        NULL::VARCHAR(100)       AS state_name,
        origin_latitude          AS latitude,
        origin_longitude         AS longitude
    FROM {{ ref('silver_flights') }}
    WHERE origin_airport_iata_code IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        dest_airport_iata_code   AS airport_iata_code,
        dest_airport_name        AS airport_name,
        dest_city                AS city_name,
        dest_state               AS state_code,
        NULL::VARCHAR(100)       AS state_name,
        dest_latitude            AS latitude,
        dest_longitude           AS longitude
    FROM {{ ref('silver_flights') }}
    WHERE dest_airport_iata_code IS NOT NULL

),

-- Projeção e organização das colunas finais da dimensão.
clean AS (
    SELECT
        airport_iata_code,
        airport_name,
        state_code,
        state_name,
        city_name,
        latitude,
        longitude
    FROM airports_raw
)

-- Criação da chave substituta e ordenação da dimensão.
SELECT
    ROW_NUMBER() OVER (ORDER BY airport_iata_code)::BIGINT AS airport_id,
    airport_iata_code,
    airport_name,
    state_code,
    state_name,
    city_name,
    latitude,
    longitude
FROM clean
ORDER BY airport_iata_code
