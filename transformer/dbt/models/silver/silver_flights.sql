{{ config(
    schema = "dbt_silver",
    materialized = "table",
    tags = ["silver", "transformation", "obt"]
) }}

-- Leitura da raw (pré-join)
with flights_raw as (
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
        flight_number,
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
),

-- Aplica filtros iniciais
filtered_raw as (
    select *
    from flights_raw
    where
        cancelled != 1
        and diverted  != 1
        and origin_airport_iata_code !~ '^[0-9]+$'
        and dest_airport_iata_code   !~ '^[0-9]+$'
),

-- Normalização HHMM -> string de 4 dígitos
normalized_times as (
    select
        fr.*,
        {{ normalize_hhmm('scheduled_departure') }} as sched_dep_hhmm,
        {{ normalize_hhmm('departure_time') }}      as dep_time_hhmm,
        {{ normalize_hhmm('scheduled_arrival') }}   as sched_arr_hhmm,
        {{ normalize_hhmm('arrival_time') }}        as arr_time_hhmm,
        {{ normalize_hhmm('wheels_off') }}          as wheels_off_hhmm,
        {{ normalize_hhmm('wheels_on') }}           as wheels_on_hhmm
    from filtered_raw fr
),

-- HHMM normalizado -> timestamp base
timestamps as (
    select
        nt.*,

        {{ build_ts('flight_date', 'sched_dep_hhmm') }} as scheduled_departure_ts,
        {{ build_ts('flight_date', 'dep_time_hhmm') }}  as departure_time_ts,
        {{ build_ts('flight_date', 'sched_arr_hhmm') }} as scheduled_arrival_ts,
        {{ build_ts('flight_date', 'arr_time_hhmm') }}  as arrival_time_ts,
        {{ build_ts('flight_date', 'wheels_off_hhmm') }} as wheels_off_ts,
        {{ build_ts('flight_date', 'wheels_on_hhmm') }}  as wheels_on_ts
    from normalized_times nt
),

-- Correção de horários trocados (swap)
swapped_times as (
    select
        t.*,
        abs(extract(epoch from (departure_time_ts   - scheduled_arrival_ts))   / 60.0) as diff_dep_sched_arr,
        abs(extract(epoch from (departure_time_ts   - scheduled_departure_ts)) / 60.0) as diff_dep_sched_dep,
        abs(extract(epoch from (arrival_time_ts     - scheduled_departure_ts)) / 60.0) as diff_arr_sched_dep,
        abs(extract(epoch from (arrival_time_ts     - scheduled_arrival_ts))   / 60.0) as diff_arr_sched_arr
    from timestamps t
),

swapped_fixed as (
    select
        s.*,
        case
            when diff_dep_sched_arr < diff_dep_sched_dep
             and diff_arr_sched_dep < diff_arr_sched_arr
            then arrival_time_ts
            else departure_time_ts
        end as departure_time_fixed,
        case
            when diff_dep_sched_arr < diff_dep_sched_dep
             and diff_arr_sched_dep < diff_arr_sched_arr
            then departure_time_ts
            else arrival_time_ts
        end as arrival_time_fixed
    from swapped_times s
),

-- Ajuste overnight (arrival + 1 dia)
derived as (
    select
        sf.*,
        case
            when arrival_time_fixed is not null
             and departure_time_fixed is not null
             and extract(hour from arrival_time_fixed) < extract(hour from departure_time_fixed)
            then true
            else false
        end as is_overnight_flight,
        case
            when arrival_time_fixed is not null
             and departure_time_fixed is not null
             and extract(hour from arrival_time_fixed) < extract(hour from departure_time_fixed)
            then arrival_time_fixed + interval '1 day'
            else arrival_time_fixed
        end as arrival_time_adj
    from swapped_fixed sf
),

-- Join com airlines
with_airlines as (
    select
        d.*,
        a.iata_code as airline_iata_code_norm,
        a.airline   as airline_name
    from derived d
    left join {{ ref('raw_airlines') }} a
        on d.airline_iata_code = a.iata_code
),

-- Join com airports
with_airports as (
    select
        wa.*,

        ao.airport  as origin_airport_name,
        ao.city     as origin_city,
        ao.state    as origin_state,
        case
            when wa.origin_airport_iata_code = 'ECP' then 30.3549
            when wa.origin_airport_iata_code = 'PBG' then 44.6895
            when wa.origin_airport_iata_code = 'UST' then 42.0703
            else nullif(ao.latitude, '')::double precision
        end as origin_latitude,
        case
            when wa.origin_airport_iata_code = 'ECP' then -86.6160
            when wa.origin_airport_iata_code = 'PBG' then -68.0448
            when wa.origin_airport_iata_code = 'UST' then -87.9539
            else nullif(ao.longitude, '')::double precision
        end as origin_longitude,

        ad.airport  as dest_airport_name,
        ad.city     as dest_city,
        ad.state    as dest_state,
        case
            when wa.dest_airport_iata_code = 'ECP' then 30.3549
            when wa.dest_airport_iata_code = 'PBG' then 44.6895
            when wa.dest_airport_iata_code = 'UST' then 42.0703
            else nullif(ad.latitude, '')::double precision
        end as dest_latitude,
        case
            when wa.dest_airport_iata_code = 'ECP' then -86.6160
            when wa.dest_airport_iata_code = 'PBG' then -68.0448
            when wa.dest_airport_iata_code = 'UST' then -87.9539
            else nullif(ad.longitude, '')::double precision
        end as dest_longitude

    from with_airlines wa
    left join {{ ref('raw_airports') }} ao
        on wa.origin_airport_iata_code = ao.iata_code
    left join {{ ref('raw_airports') }} ad
        on wa.dest_airport_iata_code = ad.iata_code
),

-- Projeção final + filtros
final as (
    select
        row_number() over (
            order by flight_date, airline_iata_code, flight_number,
                     origin_airport_iata_code, dest_airport_iata_code
        )::bigint as flight_id,

        flight_year,
        flight_month,
        flight_day,
        flight_day_of_week,
        flight_date,

        coalesce(airline_iata_code_norm, airline_iata_code) as airline_iata_code,
        airline_name,

        nullif(flight_number, '')::integer as flight_number,
        case
            when tail_number is null or tail_number = '' then null
            else left(tail_number, 10)
        end as tail_number,

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

        scheduled_departure_ts as scheduled_departure,
        departure_time_fixed   as departure_time,
        scheduled_arrival_ts   as scheduled_arrival,
        arrival_time_adj       as arrival_time,
        wheels_off_ts          as wheels_off,
        wheels_on_ts           as wheels_on,

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
    from with_airports
    where
        departure_time_fixed is not null
        and arrival_time_adj is not null
        and arrival_time_adj > departure_time_fixed
)

select *
from final
