-- -----------------------------------------------------------------------------------------------------------------------
--
--                                        SCRIPT DE CONSULTAS (SELECTS)                                                
-- 
-- Data Criação ...........: 04/11/2025
-- Autor(es) ..............: Pedro Henrique da Silva Melo
-- Banco de Dados .........: PostgreSQL 16
-- Banco de Dados(nome) ...: dw
-- 
-- Objetivo ...............: Consultas para alimentar o Dashboard de Performance de Voos.
-- Tabela Principal .......: silver.flights_silver
--
-- ------------------------------------------------------------------------------------------------------------------------

-- =======================================================================================================================
-- CONSULTA 1: MÉTRICAS PRINCIPAIS (OS 3 CARTÕES DO TOPO)
-- Objetivo: Alimentar os cartões "Total de Voos", "Média Atraso (Chegada)" e "% Voos com Atraso".
-- =======================================================================================================================
SELECT
    -- Métrica 1: Total de Voos
    COUNT(flight_id) AS total_voos,
    
    -- Métrica 2: Média de Atraso na Chegada
    AVG(arrival_delay) AS media_atraso_chegada,
    
    -- Métrica 3: Porcentagem de Voos com Atraso
    -- (Contamos voos com atraso > 0 e dividimos pelo total)
    (SUM(CASE WHEN arrival_delay > 0 THEN 1 ELSE 0 END) / COUNT(flight_id)::float) * 100 AS pct_voos_com_atraso
FROM
    silver.flights_silver;


-- =======================================================================================================================
-- CONSULTA 2: GRÁFICO DE DISPERSÃO - PERFORMANCE VS. VOLUME (COMPANHIA AÉREA)
-- Objetivo: Alimentar o gráfico de dispersão (scatter plot) das companhias.
-- =======================================================================================================================
SELECT
    airline_name, -- Detalhe da bolha 
    COUNT(flight_id) AS volume_voos, -- Eixo X
    AVG(arrival_delay) AS media_atraso, -- Eixo Y
    SUM(arrival_delay) AS impacto_total_atraso -- Tamanho da bolha
FROM
    silver.flights_silver
GROUP BY
    airline_name;


-- =======================================================================================================================
-- CONSULTA 3: GRÁFICO DE DISPERSÃO - PERFORMANCE VS. VOLUME (AEROPORTO DE ORIGEM)
-- Objetivo: Alimentar o gráfico de dispersão (scatter plot) dos aeroportos.
-- =======================================================================================================================
SELECT
    origin_airport_name, -- Detalhe da bolha 
    origin_city,
    origin_state,
    COUNT(flight_id) AS volume_voos, -- Eixo X
    AVG(arrival_delay) AS media_atraso, -- Eixo Y
    SUM(arrival_delay) AS impacto_total_atraso -- Tamanho da bolha
FROM
    silver.flights_silver
GROUP BY
    origin_airport_name,
    origin_city,
    origin_state;


-- =======================================================================================================================
-- CONSULTA 4: GRÁFICO DE COLUNAS - ATRASOS POR DIA DA SEMANA
-- Objetivo: Mostrar a média de atraso para cada dia da semana.
-- =======================================================================================================================
SELECT
    flight_day_of_week, -- Eixo X 
    AVG(arrival_delay) AS media_atraso -- Eixo Y
FROM
    silver.flights_silver
GROUP BY
    flight_day_of_week
ORDER BY
    flight_day_of_week;


-- =======================================================================================================================
-- CONSULTA 5: GRÁFICO DE COLUNAS/LINHA - ATRASOS POR MÊS
-- Objetivo: Mostrar a média de atraso para cada mês.
-- =======================================================================================================================
SELECT
    flight_month, -- Eixo X
    AVG(arrival_delay) AS media_atraso -- Eixo Y
FROM
    silver.flights_silver
GROUP BY
    flight_month
ORDER BY
    flight_month;


-- =======================================================================================================================
-- CONSULTA 6: GRÁFICO DE PIZZA - MOTIVO COM MAIOR IMPACTO
-- Objetivo: Mostrar a soma total de minutos de atraso por categoria. 
-- =======================================================================================================================
-- Nota: Ferramentas de BI (como o Power BI) lidam melhor com isso se
-- "desempacotarmos" (unpivot) os dados, mas uma consulta com SUM() diretos
-- também funciona para alimentar o gráfico de pizza.
SELECT
    SUM(air_system_delay) AS total_atraso_sistema,
    SUM(security_delay) AS total_atraso_seguranca,
    SUM(airline_delay) AS total_atraso_companhia,
    SUM(late_aircraft_delay) AS total_atraso_aeronave,
    SUM(weather_delay) AS total_atraso_clima
FROM
    silver.flights_silver
WHERE
    arrival_delay > 0;