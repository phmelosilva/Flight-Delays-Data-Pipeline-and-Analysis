-- Model: silver_flights
-- Descrição: Normaliza horários, corrige inconsistências e enriquece dados de voos.

{{ config(
    materialized = "table",
    schema       = "dbt_silver",
    tags         = ["silver", "transformation", "obt"]
) }}


-- Limpeza inicial e normalização de tipos
WITH raw AS (

    SELECT
        nullif(year, '')::smallint         AS flight_year,
        nullif(month, '')::smallint        AS flight_month,
        nullif(day, '')::smallint          AS flight_day,
        nullif(day_of_week, '')::smallint  AS flight_day_of_week,

        make_date(
            nullif(year, '')::int,
            nullif(month, '')::int,
            nullif(day, '')::int
        ) AS flight_date,

        airline               AS airline_iata_code,
        nullif(flight_number, '')::int    AS flight_number,
        tail_number,
        origin_airport        AS origin_airport_iata_code,
        destination_airport   AS dest_airport_iata_code,

        nullif(distance, '')::double precision        AS distance,
        nullif(elapsed_time, '')::double precision    AS elapsed_time,
        nullif(air_time, '')::double precision        AS air_time,
        nullif(scheduled_time, '')::double precision  AS scheduled_time,
        nullif(taxi_out, '')::double precision        AS taxi_out,
        nullif(taxi_in, '')::double precision         AS taxi_in,
        nullif(departure_delay, '')::double precision AS departure_delay,
        nullif(arrival_delay, '')::double precision   AS arrival_delay,

        -- Atrasos detalhados (convertendo strings para números)
        coalesce(nullif(air_system_delay, '')::double precision, 0)    AS air_system_delay,
        coalesce(nullif(security_delay, '')::double precision, 0)      AS security_delay,
        coalesce(nullif(airline_delay, '')::double precision, 0)       AS airline_delay,
        coalesce(nullif(late_aircraft_delay, '')::double precision, 0) AS late_aircraft_delay,
        coalesce(nullif(weather_delay, '')::double precision, 0)       AS weather_delay,

        -- Normalização cancelado/divertido
        {{ parse_int('cancelled') }} AS cancelled,
        {{ parse_int('diverted') }}  AS diverted,

        scheduled_departure,
        departure_time,
        scheduled_arrival,
        arrival_time,
        wheels_off,
        wheels_on

    FROM {{ ref('raw_flights') }}

    -- Remoção de cancelados/divertidos e registros inválidos
    WHERE {{ parse_int('cancelled') }} != 1
      AND {{ parse_int('diverted') }}  != 1
      AND origin_airport      !~ '^[0-9]+$'
      AND destination_airport !~ '^[0-9]+$'

),

-- Normalização de campos HHMM
times AS (

    SELECT
        r.*,
        {{ normalize_hhmm('scheduled_departure') }} AS sched_dep_hhmm,
        {{ normalize_hhmm('departure_time') }}      AS dep_time_hhmm,
        {{ normalize_hhmm('scheduled_arrival') }}   AS sched_arr_hhmm,
        {{ normalize_hhmm('arrival_time') }}        AS arr_time_hhmm,
        {{ normalize_hhmm('wheels_off') }}          AS wheels_off_hhmm,
        {{ normalize_hhmm('wheels_on') }}           AS wheels_on_hhmm
    FROM raw r

),

-- Conversão HHMM -> timestamp
ts AS (

    SELECT
        t.*,
        {{ build_ts('flight_date', 'sched_dep_hhmm') }} AS scheduled_departure_ts,
        {{ build_ts('flight_date', 'dep_time_hhmm') }}  AS departure_time_ts,
        {{ build_ts('flight_date', 'sched_arr_hhmm') }} AS scheduled_arrival_ts,
        {{ build_ts('flight_date', 'arr_time_hhmm') }}  AS arrival_time_ts,
        {{ build_ts('flight_date', 'wheels_off_hhmm') }} AS wheels_off_ts,
        {{ build_ts('flight_date', 'wheels_on_hhmm') }}  AS wheels_on_ts
    FROM times t

),

-- Cálculo dos deltas para detectar horários invertidos
deltas AS (

    SELECT
        ts.*,
        abs(extract(epoch FROM (departure_time_ts - scheduled_arrival_ts))   / 60.0) AS d1,
        abs(extract(epoch FROM (departure_time_ts - scheduled_departure_ts)) / 60.0) AS d2,
        abs(extract(epoch FROM (arrival_time_ts   - scheduled_departure_ts)) / 60.0) AS d3,
        abs(extract(epoch FROM (arrival_time_ts   - scheduled_arrival_ts))   / 60.0) AS d4
    FROM ts

),

-- Correção de horários invertidos (swap)
swap AS (

    SELECT
        d.*,

        CASE
            WHEN d1 < d2 AND d3 < d4 THEN arrival_time_ts
            ELSE departure_time_ts
        END AS departure_time_fixed,

        CASE
            WHEN d1 < d2 AND d3 < d4 THEN departure_time_ts
            ELSE arrival_time_ts
        END AS arrival_time_fixed

    FROM deltas d

),

-- Ajuste de voos overnight (+1 dia)
overnight AS (

    SELECT
        s.*,

        CASE
            WHEN arrival_time_fixed IS NOT NULL
             AND departure_time_fixed IS NOT NULL
             AND extract(hour FROM arrival_time_fixed) < extract(hour FROM departure_time_fixed)
            THEN arrival_time_fixed + INTERVAL '1 day'
            ELSE arrival_time_fixed
        END AS arrival_time_adj,

        CASE
            WHEN arrival_time_fixed IS NOT NULL
             AND departure_time_fixed IS NOT NULL
             AND extract(hour FROM arrival_time_fixed) < extract(hour FROM departure_time_fixed)
            THEN TRUE
            ELSE FALSE
        END AS is_overnight_flight

    FROM swap s

),

-- Enriquecimento com dimensões (companhias e aeroportos)
joined AS (

    SELECT
        o.*,
        a.airline   AS airline_name,
        a.iata_code AS airline_iata_code_norm,

        ao.airport  AS origin_airport_name,
        ao.city     AS origin_city,
        ao.state    AS origin_state,
        CASE
            WHEN o.origin_airport_iata_code = 'ECP' THEN 30.3549
            WHEN o.origin_airport_iata_code = 'PBG' THEN 44.6895
            WHEN o.origin_airport_iata_code = 'UST' THEN 42.0703
            ELSE nullif(ao.latitude, '')::double precision
        END AS origin_latitude,
        CASE
            WHEN o.origin_airport_iata_code = 'ECP' THEN -86.6160
            WHEN o.origin_airport_iata_code = 'PBG' THEN -68.0448
            WHEN o.origin_airport_iata_code = 'UST' THEN -87.9539
            ELSE nullif(ao.longitude, '')::double precision
        END AS origin_longitude,

        ad.airport  AS dest_airport_name,
        ad.city     AS dest_city,
        ad.state    AS dest_state,
        CASE
            WHEN o.dest_airport_iata_code = 'ECP' THEN 30.3549
            WHEN o.dest_airport_iata_code = 'PBG' THEN 44.6895
            WHEN o.dest_airport_iata_code = 'UST' THEN 42.0703
            ELSE nullif(ad.latitude, '')::double precision
        END AS dest_latitude,
        CASE
            WHEN o.dest_airport_iata_code = 'ECP' THEN -86.6160
            WHEN o.dest_airport_iata_code = 'PBG' THEN -68.0448
            WHEN o.dest_airport_iata_code = 'UST' THEN -87.9539
            ELSE nullif(ad.longitude, '')::double precision
        END AS dest_longitude

    FROM overnight o
    LEFT JOIN {{ ref('raw_airlines') }} a
        ON o.airline_iata_code = a.iata_code
    LEFT JOIN {{ ref('raw_airports') }} ao
        ON o.origin_airport_iata_code = ao.iata_code
    LEFT JOIN {{ ref('raw_airports') }} ad
        ON o.dest_airport_iata_code = ad.iata_code

),

-- Ordenação, geração de PK e filtros
final AS (
    SELECT
        row_number() OVER (
            ORDER BY
                flight_date,
                airline_iata_code,
                flight_number,
                origin_airport_iata_code,
                dest_airport_iata_code
        )::bigint AS flight_id,

        flight_year,
        flight_month,
        flight_day,
        flight_day_of_week,
        flight_date,

        airline_iata_code,
        airline_name,
        flight_number,
        tail_number,
        origin_airport_iata_code,
        origin_airport_name,
        origin_city,
        origin_state,
        origin_latitude,
        origin_longitude,
        dest_airport_iata_code,
        dest_airport_name,
        dest_city,
        dest_state,
        dest_latitude,
        dest_longitude,

        scheduled_departure_ts AS scheduled_departure,
        departure_time_fixed   AS departure_time,
        scheduled_arrival_ts   AS scheduled_arrival,
        arrival_time_adj       AS arrival_time,
        wheels_off_ts          AS wheels_off,
        wheels_on_ts           AS wheels_on,

        departure_delay,
        arrival_delay,
        taxi_out,
        taxi_in,
        air_time,
        elapsed_time,
        scheduled_time,
        distance,
        is_overnight_flight,

        air_system_delay,
        security_delay,
        airline_delay,
        late_aircraft_delay,
        weather_delay

    FROM joined
    WHERE departure_time_fixed IS NOT NULL
      AND arrival_time_adj    IS NOT NULL
      AND arrival_time_adj > departure_time_fixed
)

SELECT *
FROM final
