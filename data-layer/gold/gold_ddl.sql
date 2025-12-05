-- -------------------------------------------------------------------------------------------------
--
--                                   SCRIPT DE CRIAÇÃO (DDL)                                                
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
CREATE SCHEMA IF NOT EXISTS gold;
SET search_path TO gold;


CREATE TABLE IF NOT EXISTS dim_air (
    srk_air SERIAL NOT NULL,
    air_iata VARCHAR(3) UNIQUE NOT NULL,
    air_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_apt (
    srk_apt SERIAL NOT NULL,
    apt_iata VARCHAR(3) UNIQUE NOT NULL,
    apt_name VARCHAR(100) NOT NULL,
    
    st_cd VARCHAR(3) NOT NULL,
    st_name VARCHAR(100) NOT NULL,
    cty_name VARCHAR(100) NOT NULL,

    lat_val DOUBLE PRECISION,
    lon_val DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dim_dat (
    srk_dat SERIAL NOT NULL,

    full_dat DATE NOT NULL,
    yr SMALLINT NOT NULL,
    mm SMALLINT NOT NULL,
    dd SMALLINT NOT NULL,
    dow SMALLINT NOT NULL,
    
    qtr SMALLINT,
    is_hol BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS fat_flt (
    srk_flt SERIAL NOT NULL,
    srk_dat SERIAL NOT NULL,
    srk_air SERIAL NOT NULL,
    srk_ori SERIAL NOT NULL,
    srk_dst SERIAL NOT NULL,

    sch_dep TIMESTAMP,
    dep_time TIMESTAMP,
    sch_arr TIMESTAMP,
    arr_time TIMESTAMP,

    dist_val DOUBLE PRECISION,

    air_time DOUBLE PRECISION,
    elp_time DOUBLE PRECISION,
    sch_time DOUBLE PRECISION,

    dep_dly DOUBLE PRECISION DEFAULT 0,
    arr_dly DOUBLE PRECISION DEFAULT 0,
    sys_dly DOUBLE PRECISION DEFAULT 0,
    sec_dly DOUBLE PRECISION DEFAULT 0,
    air_dly DOUBLE PRECISION DEFAULT 0,
    acft_dly DOUBLE PRECISION DEFAULT 0,
    wx_dly DOUBLE PRECISION DEFAULT 0,

    is_ovn_flt BOOLEAN NOT NULL DEFAULT FALSE
);

ALTER TABLE dim_apt ADD CONSTRAINT pk_dim_apt PRIMARY KEY (srk_apt);
ALTER TABLE dim_air ADD CONSTRAINT pk_dim_air PRIMARY KEY (srk_air);
ALTER TABLE dim_dat ADD CONSTRAINT pk_dim_dat PRIMARY KEY (srk_dat);
ALTER TABLE fat_flt ADD CONSTRAINT pk_fat_flt PRIMARY KEY (srk_flt);

ALTER TABLE fat_flt ADD CONSTRAINT fk_fat_flt_srk_dat
    FOREIGN KEY (srk_dat) REFERENCES dim_dat(srk_dat)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE fat_flt ADD CONSTRAINT fk_fat_flt_srk_air
    FOREIGN KEY (srk_air) REFERENCES dim_air(srk_air)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE fat_flt ADD CONSTRAINT fk_fat_flt_srk_ori
    FOREIGN KEY (srk_ori) REFERENCES dim_apt(srk_apt)
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE fat_flt ADD CONSTRAINT fk_fat_flt_srk_dst
    FOREIGN KEY (srk_dst) REFERENCES dim_apt(srk_apt)
    ON DELETE CASCADE ON UPDATE CASCADE;

CREATE INDEX IF NOT EXISTS idx_flt_dat ON fat_flt(srk_dat);
CREATE INDEX IF NOT EXISTS idx_flt_air ON fat_flt(srk_air);
CREATE INDEX IF NOT EXISTS idx_flt_ori ON fat_flt(srk_ori);
CREATE INDEX IF NOT EXISTS idx_flt_dst ON fat_flt(srk_dst);

CREATE INDEX IF NOT EXISTS idx_flt_arr_dly ON fat_flt(arr_dly);
CREATE INDEX IF NOT EXISTS idx_flt_sch_dep ON fat_flt(sch_dep);
CREATE INDEX IF NOT EXISTS idx_flt_sch_time ON fat_flt(sch_time);

CREATE INDEX IF NOT EXISTS idx_dim_air_air_name ON dim_air(air_name);
CREATE INDEX IF NOT EXISTS idx_dim_apt_apt_name ON dim_apt(apt_name);

COMMENT ON SCHEMA gold IS 'Modelagem star schema, otimizada para bi e ia.';
