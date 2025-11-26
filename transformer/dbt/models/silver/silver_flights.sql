{{ config(
    schema = "dbt_silver",
    materialized = "table",
    tags = ["silver", "transformation", "obt"]
) }}

-- Leitura da raw (pré-join)
with raw as (
    select
        nullif(year, '')::smallint        as flight_year,
        nullif(month, '')::smallint       as flight_month,
        nullif(day, '')::smallint         as flight_day,
        nullif(day_of_week, '')::smallint as flight_day_of_week,

        make_date(
            nullif(year, '')::int,
            nullif(month, '')::int,
            nullif(day, '')::int
        ) as flight_date,

        airline              as airline_iata_code,
        nullif(flight_number, '')::int    as flight_number,
        tail_number,
        origin_airport       as origin_airport_iata_code,
        destination_airport  as dest_airport_iata_code,

        nullif(distance, '')::double precision       as distance,
        nullif(elapsed_time, '')::double precision   as elapsed_time,
        nullif(air_time, '')::double precision       as air_time,
        nullif(scheduled_time, '')::double precision as scheduled_time,
        nullif(taxi_out, '')::double precision       as taxi_out,
        nullif(taxi_in, '')::double precision        as taxi_in,
        nullif(departure_delay, '')::double precision as departure_delay,
        nullif(arrival_delay, '')::double precision   as arrival_delay,

        coalesce(nullif(air_system_delay, '')::double precision, 0)    as air_system_delay,
        coalesce(nullif(security_delay, '')::double precision, 0)      as security_delay,
        coalesce(nullif(airline_delay, '')::double precision, 0)       as airline_delay,
        coalesce(nullif(late_aircraft_delay, '')::double precision, 0) as late_aircraft_delay,
        coalesce(nullif(weather_delay, '')::double precision, 0)       as weather_delay,

        {{ parse_int('cancelled') }} as cancelled,
        {{ parse_int('diverted') }}  as diverted,

        scheduled_departure,
        departure_time,
        scheduled_arrival,
        arrival_time,
        wheels_off,
        wheels_on
    from {{ ref('raw_flights') }}
    where
        {{ parse_int('cancelled') }} != 1
        and {{ parse_int('diverted') }}  != 1
        and origin_airport !~ '^[0-9]+$'
        and destination_airport !~ '^[0-9]+$'
),

-- Normalização HHMM
times as (
    select
        r.*,
        {{ normalize_hhmm('scheduled_departure') }} as sched_dep_hhmm,
        {{ normalize_hhmm('departure_time') }}      as dep_time_hhmm,
        {{ normalize_hhmm('scheduled_arrival') }}   as sched_arr_hhmm,
        {{ normalize_hhmm('arrival_time') }}        as arr_time_hhmm,
        {{ normalize_hhmm('wheels_off') }}          as wheels_off_hhmm,
        {{ normalize_hhmm('wheels_on') }}           as wheels_on_hhmm
    from raw r
),

-- Conversão HHMM -> timestamp
ts as (
    select
        t.*,
        {{ build_ts('flight_date', 'sched_dep_hhmm') }} as scheduled_departure_ts,
        {{ build_ts('flight_date', 'dep_time_hhmm') }}  as departure_time_ts,
        {{ build_ts('flight_date', 'sched_arr_hhmm') }} as scheduled_arrival_ts,
        {{ build_ts('flight_date', 'arr_time_hhmm') }}  as arrival_time_ts,
        {{ build_ts('flight_date', 'wheels_off_hhmm') }} as wheels_off_ts,
        {{ build_ts('flight_date', 'wheels_on_hhmm') }}  as wheels_on_ts
    from times t
),

-- Cálculo dos deltas
deltas as (
    select
        ts.*,
        abs(extract(epoch from (departure_time_ts - scheduled_arrival_ts)) / 60.0)   as d1,
        abs(extract(epoch from (departure_time_ts - scheduled_departure_ts)) / 60.0) as d2,
        abs(extract(epoch from (arrival_time_ts   - scheduled_departure_ts)) / 60.0) as d3,
        abs(extract(epoch from (arrival_time_ts   - scheduled_arrival_ts)) / 60.0)   as d4
    from ts
),

-- Correção de horários trocados (swap)
swap as (
    select
        d.*,
        case when d1 < d2 and d3 < d4
             then arrival_time_ts else departure_time_ts
        end as departure_time_fixed,

        case when d1 < d2 and d3 < d4
             then departure_time_ts else arrival_time_ts
        end as arrival_time_fixed
    from deltas d
),

-- Ajuste overnight (arrival + 1 dia)
overnight as (
    select
        s.*,
        case
            when arrival_time_fixed is not null
             and departure_time_fixed is not null
             and extract(hour from arrival_time_fixed) < extract(hour from departure_time_fixed)
            then arrival_time_fixed + interval '1 day'
            else arrival_time_fixed
        end as arrival_time_adj,

        case
            when arrival_time_fixed is not null
             and departure_time_fixed is not null
             and extract(hour from arrival_time_fixed) < extract(hour from departure_time_fixed)
            then true
            else false
        end as is_overnight_flight
    from swap s
),

-- Joins
joined as (
    select
        o.*,
        a.airline   as airline_name,
        a.iata_code as airline_iata_code_norm,

        -- Join com aeroportos (origem)
        ao.airport  as origin_airport_name,
        ao.city     as origin_city,
        ao.state    as origin_state,
        case
            when o.origin_airport_iata_code = 'ECP' then 30.3549
            when o.origin_airport_iata_code = 'PBG' then 44.6895
            when o.origin_airport_iata_code = 'UST' then 42.0703
            else nullif(ao.latitude, '')::double precision
        end as origin_latitude,
        case
            when o.origin_airport_iata_code = 'ECP' then -86.6160
            when o.origin_airport_iata_code = 'PBG' then -68.0448
            when o.origin_airport_iata_code = 'UST' then -87.9539
            else nullif(ao.longitude, '')::double precision
        end as origin_longitude,

        -- Join com aeroportos (destino)
        ad.airport  as dest_airport_name,
        ad.city     as dest_city,
        ad.state    as dest_state,
        case
            when o.dest_airport_iata_code = 'ECP' then 30.3549
            when o.dest_airport_iata_code = 'PBG' then 44.6895
            when o.dest_airport_iata_code = 'UST' then 42.0703
            else nullif(ad.latitude, '')::double precision
        end as dest_latitude,
        case
            when o.dest_airport_iata_code = 'ECP' then -86.6160
            when o.dest_airport_iata_code = 'PBG' then -68.0448
            when o.dest_airport_iata_code = 'UST' then -87.9539
            else nullif(ad.longitude, '')::double precision
        end as dest_longitude

    from overnight o
    left join {{ ref('raw_airlines') }} a
        on o.airline_iata_code = a.iata_code
    left join {{ ref('raw_airports') }} ao
        on o.origin_airport_iata_code = ao.iata_code
    left join {{ ref('raw_airports') }} ad
        on o.dest_airport_iata_code = ad.iata_code
),

-- Projeção final + filtros
final as (
    select
        row_number() over (
            order by flight_date, airline_iata_code, flight_number,
                     origin_airport_iata_code, dest_airport_iata_code
        )::bigint as flight_id,
        *
    from joined
    where departure_time_fixed is not null
      and arrival_time_adj     is not null
      and arrival_time_adj > departure_time_fixed
)

select *
from final
