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
-- Tabela Principal .......: gold.fat_flt
--
-- Últimas alterações:
--      27/11/2025 => Adiciona mais 4 consultas para análises adicionais;
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
    (SUM(CASE WHEN f.arrival_delay > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(f.flight_id), 0)::float) * 100 AS pct_voos_com_atraso
FROM
    fat_flt AS f;


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
    fat_flt AS f
JOIN
    dim_air AS a ON f.airline_id = a.airline_id
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
    fat_flt AS f
JOIN
    dim_apt AS ap ON f.origin_airport_id = ap.airport_id
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
    fat_flt AS f
JOIN
    dim_dat AS d ON f.full_date = d.full_date
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
    fat_flt AS f
JOIN
    dim_dat AS d ON f.full_date = d.full_date
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
    fat_flt AS f
WHERE
    f.arrival_delay > 0;


-- =======================================================================================================================
-- CONSULTA 7: ANÁLISE DE ROTAS CRÍTICAS (TOP 10 PIORES ROTAS)
-- Objetivo: Identificar quais conexões (Origem -> Destino) sofrem mais atrasos.
-- =======================================================================================================================
SELECT 
    origem.airport_iata_code AS origem,
    destino.airport_iata_code AS destino,
    COUNT(f.flight_id) AS total_voos,
    AVG(f.arrival_delay) AS media_atraso_chegada
FROM 
    fat_flt AS f
JOIN 
    dim_apt AS origem ON f.origin_airport_id = origem.airport_id
JOIN 
    dim_apt AS destino ON f.dest_airport_id = destino.airport_id
GROUP BY 
    origem.airport_iata_code, 
    destino.airport_iata_code
HAVING 
    COUNT(f.flight_id) > 50
ORDER BY 
    media_atraso_chegada DESC
LIMIT 10;


-- =======================================================================================================================
-- CONSULTA 8: ATRASOS POR PERÍODO DO DIA (MADRUGADA, MANHÃ, TARDE, NOITE)
-- Objetivo: Validar se o atraso acumula ao longo do dia.
-- =======================================================================================================================
SELECT 
    CASE 
        WHEN EXTRACT(HOUR FROM f.scheduled_departure) BETWEEN 0 AND 5 THEN 'Madrugada'
        WHEN EXTRACT(HOUR FROM f.scheduled_departure) BETWEEN 6 AND 11 THEN 'Manhã'
        WHEN EXTRACT(HOUR FROM f.scheduled_departure) BETWEEN 12 AND 17 THEN 'Tarde'
        WHEN EXTRACT(HOUR FROM f.scheduled_departure) >= 18 THEN 'Noite'
    END AS periodo_dia,
    COUNT(f.flight_id) AS total_voos,
    AVG(f.departure_delay) AS media_atraso_partida,
    AVG(f.arrival_delay) AS media_atraso_chegada
FROM 
    fat_flt AS f
GROUP BY 
    1 -- Agrupa pela primeira coluna calculada (periodo_dia)
ORDER BY 
    media_atraso_chegada ASC;


-- =======================================================================================================================
-- CONSULTA 9: PERFORMANCE POR DURAÇÃO DE VOO
-- Objetivo: Analisar se voos mais longos tendem a recuperar ou aumentar o atraso.
-- =======================================================================================================================
SELECT 
    CASE 
        WHEN f.elapsed_time <= 90 THEN 'Curta Duração'
        WHEN f.elapsed_time > 90 AND f.elapsed_time <= 240 THEN 'Média Duração'
        WHEN f.elapsed_time > 240 THEN 'Longa Duração'
        ELSE 'Desconhecido'
    END AS categoria_duracao,
    COUNT(f.flight_id) AS total_voos,
    AVG(f.arrival_delay) AS media_atraso_chegada
FROM 
    fat_flt AS f
WHERE 
    f.elapsed_time IS NOT NULL
GROUP BY 
    1
ORDER BY 
    media_atraso_chegada DESC;


-- =======================================================================================================================
-- CONSULTA 10: MAPA DE CALOR POR ESTADO DE ORIGEM
-- Objetivo: Alimentar o mapa de bolhas.
-- =======================================================================================================================
SELECT 
    ap.state_code AS estado_origem, -- Usando a sigla do estado que está na dimensão
    COUNT(f.flight_id) AS volume_voos,
    AVG(f.arrival_delay) AS media_atraso_chegada,
    SUM(f.arrival_delay) AS total_minutos_atraso
FROM 
    fat_flt AS f
JOIN
    dim_apt AS ap ON f.origin_airport_id = ap.airport_id
GROUP BY 
    ap.state_code
ORDER BY 
    volume_voos DESC;
