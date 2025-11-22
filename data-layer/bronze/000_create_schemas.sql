-- -----------------------------------------------------------------------------------------------------------------------
--
--                                        SCRIPT DE CRIAÇÃO (DDL)                                                
-- 
-- Data Criação ...........: 09/10/2025
-- Autor(es) ..............: Júlia Takaki, Matheus Henrique Dos Santos
-- Banco de Dados .........: PostgreSQL 16
-- Banco de Dados(nome) ...: dw
-- 
-- Últimas alterações:
--
-- PROJETO => 03 Base de Dados
--         => 05 Tabelas
--
-- ------------------------------------------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS dbt_bronze;
CREATE SCHEMA IF NOT EXISTS dbt_silver;
CREATE SCHEMA IF NOT EXISTS dbt_gold;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
SET search_path TO dbt_bronze;


CREATE TABLE IF NOT EXISTS airlines (
    iata_code TEXT,
    airline   TEXT
);

CREATE TABLE IF NOT EXISTS airports (
    iata_code  TEXT,
    airport    TEXT,
    city       TEXT,
    state      TEXT,
    country    TEXT,
    latitude   TEXT,
    longitude  TEXT
);

CREATE TABLE IF NOT EXISTS flights (
    year                 TEXT,
    month                TEXT,
    day                  TEXT,
    day_of_week          TEXT,
    airline              TEXT,
    flight_number        TEXT,
    tail_number          TEXT,
    origin_airport       TEXT,
    destination_airport  TEXT,
    scheduled_departure  TEXT,
    departure_time       TEXT,
    departure_delay      TEXT,
    taxi_out             TEXT,
    wheels_off           TEXT,
    scheduled_time       TEXT,
    elapsed_time         TEXT,
    air_time             TEXT,
    distance             TEXT,
    wheels_on            TEXT,
    taxi_in              TEXT,
    scheduled_arrival    TEXT,
    arrival_time         TEXT,
    arrival_delay        TEXT,
    diverted             TEXT,
    cancelled            TEXT,
    cancellation_reason  TEXT,
    air_system_delay     TEXT,
    security_delay       TEXT,
    airline_delay        TEXT,
    late_aircraft_delay  TEXT,
    weather_delay        TEXT
);
