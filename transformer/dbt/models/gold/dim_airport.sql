{{ config(
    schema = "dbt_gold",
    materialized = "table",
    tags = ["gold", "dim", "airport"]
) }}

with airports_raw as (
    select distinct
        origin_airport_iata_code as airport_iata_code,
        origin_airport_name      as airport_name,
        origin_city              as city_name,
        origin_state             as state_code,
        null::varchar(100)       as state_name,
        origin_latitude          as latitude,
        origin_longitude         as longitude
    from {{ ref('silver_flights') }}
    where origin_airport_iata_code is not null

    union distinct

    select distinct
        dest_airport_iata_code   as airport_iata_code,
        dest_airport_name        as airport_name,
        dest_city                as city_name,
        dest_state               as state_code,
        null::varchar(100)       as state_name,
        dest_latitude            as latitude,
        dest_longitude           as longitude
    from {{ ref('silver_flights') }}
    where dest_airport_iata_code is not null
),

clean as (
    select
        airport_iata_code,
        airport_name,
        state_code,
        state_name,
        city_name,
        latitude,
        longitude
    from airports_raw
)

select
    row_number() over (order by airport_iata_code)::bigint as airport_id,
    airport_iata_code,
    airport_name,
    state_code,
    state_name,
    city_name,
    latitude,
    longitude
from clean
order by airport_iata_code
