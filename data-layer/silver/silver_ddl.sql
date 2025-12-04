-- -------------------------------------------------------------------------------------------------
--
--                                        SCRIPT DE CRIAÇÃO (DDL)                                                
--
-- Data Criação ...........: 01/12/2025
-- Autor(es) ..............: Matheus Henrique Dos Santos
-- Banco de Dados .........: PostgreSQL 16
-- Banco de Dados(nome) ...: dw
-- 
-- Últimas alterações:
--
-- PROJETO => 05 Base de Dados
--         => 13 Tabelas
--         => 03 Views
--
-- -------------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS silver;
SET search_path TO silver;


CREATE TABLE IF NOT EXISTS silver_flights (
    flight_id INTEGER,
    flight_year SMALLINT NOT NULL,
    flight_month SMALLINT NOT NULL,
    flight_day SMALLINT NOT NULL,
    flight_day_of_week SMALLINT NOT NULL,
    flight_date DATE NOT NULL,
    flight_number INTEGER NOT NULL,

    airline_iata_code VARCHAR(3) NOT NULL,
    airline_name VARCHAR(100) NOT NULL,

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

    scheduled_departure TIMESTAMP,
    departure_time TIMESTAMP,
    scheduled_arrival TIMESTAMP,
    arrival_time TIMESTAMP,
    wheels_off TIMESTAMP,
    wheels_on TIMESTAMP,

    taxi_out DOUBLE PRECISION,
    taxi_in DOUBLE PRECISION,
    air_time DOUBLE PRECISION,
    elapsed_time DOUBLE PRECISION,
    scheduled_time DOUBLE PRECISION,
    distance DOUBLE PRECISION,

    departure_delay DOUBLE PRECISION DEFAULT 0,
    arrival_delay DOUBLE PRECISION DEFAULT 0,
    air_system_delay DOUBLE PRECISION DEFAULT 0,
    security_delay DOUBLE PRECISION DEFAULT 0,
    airline_delay DOUBLE PRECISION DEFAULT 0,
    late_aircraft_delay DOUBLE PRECISION DEFAULT 0,
    weather_delay DOUBLE PRECISION DEFAULT 0,

    is_overnight_flight BOOLEAN NOT NULL DEFAULT FALSE
);

ALTER TABLE silver_flights ADD CONSTRAINT pk_silver_flights 
    PRIMARY KEY (flight_id);

CREATE INDEX IF NOT EXISTS idx_silver_flights_date 
    ON silver_flights(flight_date);
CREATE INDEX IF NOT EXISTS idx_silver_flights_airline 
    ON silver_flights(airline_iata_code);
CREATE INDEX IF NOT EXISTS idx_silver_flights_origin_dest 
    ON silver_flights(origin_airport_iata_code, dest_airport_iata_code);
CREATE INDEX IF NOT EXISTS idx_silver_flights_times 
    ON silver_flights(departure_time, arrival_time);

COMMENT ON SCHEMA silver IS 'Modelagem OBT para a camada silver.';
