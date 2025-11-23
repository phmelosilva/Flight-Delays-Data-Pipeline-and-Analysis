{{ config(
    schema = "dbt_gold",
    materialized = "table",
    tags = ["gold", "fato", "flights"]
) }}

with src as (
    select
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
    from {{ ref('silver_flights') }}
),

with_dim_airline as (
    select
        s.*,
        da.airline_id
    from src s
    left join {{ ref('dim_airline') }} da
        on s.airline_iata_code = da.airline_iata_code
),

with_dim_airport as (
    select
        s.*,
        ao.airport_id as origin_airport_id,
        ad.airport_id as dest_airport_id
    from with_dim_airline s
    left join {{ ref('dim_airport') }} ao
        on s.origin_airport_iata_code = ao.airport_iata_code
    left join {{ ref('dim_airport') }} ad
        on s.dest_airport_iata_code = ad.airport_iata_code
),

with_dim_date as (
    select
        wda.*,
        dd.full_date
    from with_dim_airport wda
    left join {{ ref('dim_date') }} dd
        on wda.flight_date = dd.full_date
),

final as (
    select
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
    from with_dim_date
    order by flight_id
)

select *
from final
