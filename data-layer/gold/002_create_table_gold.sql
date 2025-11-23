-- ---------------------------------------------------------------------------------------------------------------------
--
--                                        SCRIPT DE CRIAÇÃO (DDL)                                                
-- 
-- Data Criação ...........: 04/11/2025
-- Autor(es) ..............: Matheus Henrique Dos Santos, Joao Matheus de Oliveira Schmitz
-- Banco de Dados .........: PostgreSQL 16
-- Banco de Dados(nome) ...: dw
--
-- Últimas alterações:
--      08/11/2025 => Altera atributos com datas para TIMESTAMP;
--                 => Adiciona atributo "is_overnight_flight" na tabela "fato_flights";
--                 => Remove atributo "date_id" da tabela "dim_date";
--                 => Altera modelagem nas tabelas "dim_date" e "fato_flights";
--
--      09/11/2025 => Remove atributo 'date_id' da tabela 'dim_date';
--                 => Altera modelagem nas tabelas 'dim_date' e 'fato_flights';
--                 => Adiciona CASCADE para operações de DELETE e UPDATE;
--                 => Corrige tipos dos atributos 'airport_id', 'airline_id' e 'flight_id';
--
-- PROJETO => 05 Base de Dados
--         => 13 Tabelas
--         => 03 Views
--
-- ---------------------------------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS gold;
SET search_path TO gold;


CREATE TABLE IF NOT EXISTS dim_airport (
    airport_id BIGSERIAL,
    airport_iata_code VARCHAR(3) UNIQUE NOT NULL,
    airport_name VARCHAR(100) NOT NULL,
    
    state_code VARCHAR(3) NOT NULL,
    state_name VARCHAR(100),

    city_name VARCHAR(100) NOT NULL,

    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dim_airline (
    airline_id BIGSERIAL,
    airline_iata_code VARCHAR(3) UNIQUE NOT NULL,
    airline_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    full_date DATE,
    year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    quarter SMALLINT,
    is_holiday BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS fato_flights (
    flight_id BIGINT,
    full_date DATE NOT NULL,
    airline_id BIGINT NOT NULL,
    origin_airport_id BIGINT NOT NULL,
    dest_airport_id BIGINT NOT NULL,

    scheduled_departure TIMESTAMP,
    departure_time TIMESTAMP,
    scheduled_arrival TIMESTAMP,
    arrival_time TIMESTAMP,
    wheels_off TIMESTAMP,
    wheels_on TIMESTAMP,

    distance DOUBLE PRECISION,
    air_time DOUBLE PRECISION,
    elapsed_time DOUBLE PRECISION,
    scheduled_time DOUBLE PRECISION,
    taxi_out DOUBLE PRECISION,
    taxi_in DOUBLE PRECISION,
    departure_delay DOUBLE PRECISION,
    arrival_delay DOUBLE PRECISION,

    is_overnight_flight BOOLEAN NOT NULL DEFAULT FALSE,

    air_system_delay DOUBLE PRECISION DEFAULT 0,
    security_delay DOUBLE PRECISION DEFAULT 0,
    airline_delay DOUBLE PRECISION DEFAULT 0,
    late_aircraft_delay DOUBLE PRECISION DEFAULT 0,
    weather_delay DOUBLE PRECISION DEFAULT 0
);

ALTER TABLE dim_airport ADD CONSTRAINT pk_dim_airport PRIMARY KEY (airport_id);
ALTER TABLE dim_airline ADD CONSTRAINT pk_dim_airline PRIMARY KEY (airline_id);
ALTER TABLE dim_date ADD CONSTRAINT pk_dim_date PRIMARY KEY (full_date);
ALTER TABLE fato_flights ADD CONSTRAINT pk_fato_flights PRIMARY KEY (flight_id);

ALTER TABLE fato_flights ADD CONSTRAINT fk_fato_flights_full_date
    FOREIGN KEY (full_date) REFERENCES dim_date(full_date)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE fato_flights ADD CONSTRAINT fk_fato_flights_airline_id
    FOREIGN KEY (airline_id) REFERENCES dim_airline(airline_id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE fato_flights ADD CONSTRAINT fk_fato_flights_origin_airport_id
    FOREIGN KEY (origin_airport_id) REFERENCES dim_airport(airport_id)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE fato_flights ADD CONSTRAINT fk_fato_flights_dest_airport_id
    FOREIGN KEY (dest_airport_id) REFERENCES dim_airport(airport_id)
    ON DELETE CASCADE ON UPDATE CASCADE;

CREATE INDEX IF NOT EXISTS idx_flights_date ON fato_flights(full_date);
CREATE INDEX IF NOT EXISTS idx_flights_airline ON fato_flights(airline_id);
CREATE INDEX IF NOT EXISTS idx_flights_origin ON fato_flights(origin_airport_id);
CREATE INDEX IF NOT EXISTS idx_flights_dest ON fato_flights(dest_airport_id);

CREATE INDEX IF NOT EXISTS idx_fato_flights_arrival_delay ON fato_flights (arrival_delay);
CREATE INDEX IF NOT EXISTS idx_fato_flights_scheduled_departure ON fato_flights (scheduled_departure);
CREATE INDEX IF NOT EXISTS idx_fato_flights_scheduled_time ON fato_flights (scheduled_time);

CREATE INDEX IF NOT EXISTS idx_dim_airline_airline_name ON dim_airline (airline_name);
CREATE INDEX IF NOT EXISTS idx_dim_airport_airport_name ON dim_airport (airport_name);
CREATE INDEX IF NOT EXISTS idx_dim_airport_state_code ON dim_airport (state_code);

COMMENT ON SCHEMA gold IS 'Modelagem Star, otimizada para BI e IA.';
