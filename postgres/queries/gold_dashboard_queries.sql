-- -----------------------------------------------------------------------------------------------------------------------
--
--                                        SCRIPT DE CONSULTAS (SELECTS)                                                
-- 
-- Data Criação ...........: 06/11/2025
-- Autor(es) ..............: Joao Matheus de Oliveira Schmitz
-- Banco de Dados .........: PostgreSQL 16
-- Banco de Dados(nome) ...: dw
-- 
-- Objetivo ...............: Consultas para alimentar o Dashboard de Performance de Voos.
-- Tabela Principal .......: gold.fato_flights
--
-- ------------------------------------------------------------------------------------------------------------------------

-- Definir o schema padrão para a sessão
SET search_path TO gold;

-- =======================================================================================================================
-- CONSULTA 1: MÉTRICAS PRINCIPAIS (OS 3 CARTÕES DO TOPO)
-- Objetivo: Alimentar os cartões "Total de Voos", "Média Atraso (Chegada)" e "% Voos com Atraso".
-- =======================================================================================================================
SELECT
    -- Métrica 1: Total de Voos
    COUNT(f.flight_id) AS total_voos,
    
    -- Métrica 2: Média de Atraso na Chegada
    AVG(f.arrival_delay) AS media_atraso_chegada,
    
    -- Métrica 3: Porcentagem de Voos com Atraso
    (SUM(CASE WHEN f.arrival_delay > 0 THEN 1 ELSE 0 END) / COUNT(f.flight_id)::float) * 100 AS pct_voos_com_atraso
FROM
    fato_flights AS f;


-- =======================================================================================================================
-- CONSULTA 2: GRÁFICO DE DISPERSÃO - PERFORMANCE VS. VOLUME (COMPANHIA AÉREA)
-- Objetivo: Alimentar o gráfico de dispersão (scatter plot) das companhias.
-- =======================================================================================================================
SELECT
    a.airline_name, -- Detalhe da bolha
    COUNT(f.flight_id) AS volume_voos, -- Eixo X
    AVG(f.arrival_delay) AS media_atraso, -- Eixo Y
    SUM(f.arrival_delay) AS impacto_total_atraso -- Tamanho da bolha
FROM
    fato_flights AS f
JOIN
    dim_airline AS a ON f.airline_id = a.airline_id
GROUP BY
    a.airline_name;


-- =======================================================================================================================
-- CONSULTA 3: GRÁFICO DE DISPERSÃO - PERFORMANCE VS. VOLUME (AEROPORTO DE ORIGEM)
-- Objetivo: Alimentar o gráfico de dispersão (scatter plot) dos aeroportos.
-- =======================================================================================================================
SELECT
    ap.airport_name AS origin_airport_name, -- Detalhe da bolha
    ap.city_name AS origin_city,            -- Tooltip
    ap.state_name AS origin_state,          -- Tooltip
    COUNT(f.flight_id) AS volume_voos,      -- Eixo X
    AVG(f.arrival_delay) AS media_atraso,   -- Eixo Y
    SUM(f.arrival_delay) AS impacto_total_atraso -- Tamanho da bolha
FROM
    fato_flights AS f
JOIN
    dim_airport AS ap ON f.origin_airport_id = ap.airport_id
GROUP BY
    ap.airport_name,
    ap.city_name,
    ap.state_name;


-- =======================================================================================================================
-- CONSULTA 4: GRÁFICO DE COLUNAS - ATRASOS POR DIA DA SEMANA
-- Objetivo: Mostrar a média de atraso para cada dia da semana.
-- =======================================================================================================================
SELECT
    d.day_of_week, -- Eixo X
    AVG(f.arrival_delay) AS media_atraso -- Eixo Y
FROM
    fato_flights AS f
JOIN
    dim_date AS d ON f.date_id = d.date_id
GROUP BY
    d.day_of_week
ORDER BY
    d.day_of_week;


-- =======================================================================================================================
-- CONSULTA 5: GRÁFICO DE COLUNAS/LINHA - ATRASOS POR MÊS
-- Objetivo: Mostrar a média de atraso para cada mês.
-- =======================================================================================================================
SELECT
    d.month, -- Eixo X
    AVG(f.arrival_delay) AS media_atraso -- Eixo Y
FROM
    fato_flights AS f
JOIN
    dim_date AS d ON f.date_id = d.date_id
GROUP BY
    d.month
ORDER BY
    d.month;


-- =======================================================================================================================
-- CONSULTA 6: GRÁFICO DE PIZZA - MOTIVO COM MAIOR IMPACTO
-- Objetivo: Mostrar a soma total de minutos de atraso por categoria. 
-- =======================================================================================================================
SELECT
    SUM(f.air_system_delay) AS total_atraso_sistema,
    SUM(f.security_delay) AS total_atraso_seguranca,
    SUM(f.airline_delay) AS total_atraso_companhia,
    SUM(f.late_aircraft_delay) AS total_atraso_aeronave,
    SUM(f.weather_delay) AS total_atraso_clima
FROM
    fato_flights AS f
WHERE
    f.arrival_delay > 0;
