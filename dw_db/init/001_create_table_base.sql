-- -----------------------------------------------------------------------------------------------------------------------
--
--                                        CRIPT DE CRIAÇÃO (DDL)                                                
-- 
-- Data Criação ...........: 09/10/2025
-- Autor(es) ..............: Júlia Takaki, Matheus Henrique Dos Santos
-- Banco de Dados .........: PostgreSQL 16
-- Banco de Dados(nome) ...: dw_db
-- 
-- Últimas alterações:
--
-- PROJETO => 01 Base de Dados
--         => 01 Tabelas
--
-- ------------------------------------------------------------------------------------------------------------------------
SET search_path TO silver;


CREATE TABLE IF NOT EXISTS flights_silver (
    flight_id VARCHAR(64) PRIMARY KEY,
    flight_year SMALLINT NOT NULL,
    flight_month SMALLINT NOT NULL,
    flight_day SMALLINT NOT NULL,
    flight_day_of_week SMALLINT NOT NULL,

    flight_date DATE NOT NULL,

    airline_iata_code VARCHAR(3) NOT NULL,
    airline_name VARCHAR(100) NOT NULL,

    flight_number INTEGER NOT NULL,
    tail_number VARCHAR(10),

    origin_airport_iata_code VARCHAR(3) NOT NULL,
    origin_airport_name VARCHAR(100) NOT NULL,
    origin_city VARCHAR(50),
    origin_state VARCHAR(3),
    origin_latitude DOUBLE PRECISION,
    origin_longitude DOUBLE PRECISION,

    dest_airport_iata_code VARCHAR(3) NOT NULL,
    dest_airport_name VARCHAR(100),
    dest_city VARCHAR(50),
    dest_state VARCHAR(3),
    dest_latitude DOUBLE PRECISION,
    dest_longitude DOUBLE PRECISION,

    scheduled_departure DOUBLE PRECISION,
    departure_time DOUBLE PRECISION,
    scheduled_arrival DOUBLE PRECISION,
    arrival_time DOUBLE PRECISION,
    wheels_off DOUBLE PRECISION,
    wheels_on DOUBLE PRECISION,

    departure_delay DOUBLE PRECISION,
    arrival_delay DOUBLE PRECISION,
    taxi_out DOUBLE PRECISION,
    taxi_in DOUBLE PRECISION,
    air_time DOUBLE PRECISION,
    elapsed_time DOUBLE PRECISION,
    scheduled_time DOUBLE PRECISION,
    distance DOUBLE PRECISION,

    air_system_delay DOUBLE PRECISION DEFAULT 0,
    security_delay DOUBLE PRECISION DEFAULT 0,
    airline_delay DOUBLE PRECISION DEFAULT 0,
    late_aircraft_delay DOUBLE PRECISION DEFAULT 0,
    weather_delay DOUBLE PRECISION DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_flights_flight_date
    ON silver.flights_silver (flight_date);

CREATE INDEX IF NOT EXISTS idx_flights_airline_iata
    ON silver.flights_silver (airline_iata_code);

CREATE INDEX IF NOT EXISTS idx_flights_origin_dest
    ON silver.flights_silver (origin_airport_iata_code, dest_airport_iata_code);
