-- Model: fat_flt
-- Descrição: Fato de voos contendo métricas operacionais e chaves substitutas das dimensões, derivado da OBT.


{{ config(
    materialized = "table",
    schema       = "dbt_gold",
    tags         = ["gold", "fato", "flights"]
) }}

-- Seleção e preparação dos campos da silver
WITH src AS (
    SELECT
        flight_id,
        flight_date,
        airline_iata_code,
        origin_airport_iata_code,
        dest_airport_iata_code,

        scheduled_departure,
        departure_time,
        scheduled_arrival,
        arrival_time,
        wheels_off,
        wheels_on,

        distance,
        air_time,
        elapsed_time,
        scheduled_time,
        taxi_out,
        taxi_in,
        departure_delay,
        arrival_delay,

        is_overnight_flight,

        air_system_delay,
        security_delay,
        airline_delay,
        late_aircraft_delay,
        weather_delay
    FROM {{ ref('silver_flights') }}
),

-- Junção com a dimensão de companhias aéreas
with_dim_air AS (
    SELECT
        s.*,
        da.airline_id
    FROM src s
    LEFT JOIN {{ ref('dim_air') }} da
        ON s.airline_iata_code = da.airline_iata_code
),

-- Junção com a dimensão de aeroportos (origem e destino)
with_dim_apt AS (
    SELECT
        s.*,
        ao.airport_id AS origin_airport_id,
        ad.airport_id AS dest_airport_id
    FROM with_dim_air s
    LEFT JOIN {{ ref('dim_apt') }} ao
        ON s.origin_airport_iata_code = ao.airport_iata_code
    LEFT JOIN {{ ref('dim_apt') }} ad
        ON s.dest_airport_iata_code = ad.airport_iata_code
),

-- Junção com a dimensão de datas
with_dim_dat AS (
    SELECT
        wda.*,
        dd.full_date
    FROM with_dim_apt wda
    LEFT JOIN {{ ref('dim_dat') }} dd
        ON wda.flight_date = dd.full_date
),

-- Projeção final dos campos do fato
final AS (
    SELECT
        flight_id,
        full_date,
        airline_id,
        origin_airport_id,
        dest_airport_id,

        scheduled_departure,
        departure_time,
        scheduled_arrival,
        arrival_time,
        wheels_off,
        wheels_on,

        distance,
        air_time,
        elapsed_time,
        scheduled_time,
        taxi_out,
        taxi_in,
        departure_delay,
        arrival_delay,

        is_overnight_flight,

        air_system_delay,
        security_delay,
        airline_delay,
        late_aircraft_delay,
        weather_delay
    FROM with_dim_dat
    ORDER BY flight_id
)

SELECT *
FROM final
