-- -----------------------------------------------------------------------------------------------------------------------
--
--                                        SCRIPT DE CONSULTAS (SELECTS)                                                
-- 
-- Data Criação ...........: 04/11/2025
-- Autor(es) ..............: Pedro Henrique da Silva Melo
-- Banco de Dados .........: PostgreSQL 16
-- Banco de Dados(nome) ...: dw
-- Últimas alterações:
--      26/11/2025 => Adiciona mais 4 consultas para análises adicionais, modifica flights_silver para silver_flights;
-- 
-- Objetivo ...............: Consultas para alimentar o Dashboard de Performance de Voos.
-- Tabela Principal .......: silver.silver_flights
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
    silver.silver_flights;

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
    silver.silver_flights
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
    silver.silver_flights
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
    silver.silver_flights
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
    silver.silver_flights
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
    silver.silver_flights
WHERE
    arrival_delay > 0;

-- =======================================================================================================================
-- CONSULTA 7: ANÁLISE DE ROTAS CRÍTICAS (TOP 10 PIORES ROTAS)
-- Objetivo: Identificar quais conexões (Origem -> Destino) sofrem mais atrasos.
-- Ajuda a responder: "Quais rotas impulsionam os atrasos?"
-- =======================================================================================================================
SELECT 
    origin_airport_iata_code AS origem,
    dest_airport_iata_code AS destino,
    COUNT(flight_id) AS total_voos,
    AVG(arrival_delay) AS media_atraso_chegada
FROM 
    silver.silver_flights
GROUP BY 
    origin_airport_iata_code, 
    dest_airport_iata_code
HAVING 
    COUNT(flight_id) > 50 -- Filtra rotas com poucos voos para evitar distorções
ORDER BY 
    media_atraso_chegada DESC
LIMIT 10;

-- =======================================================================================================================
-- CONSULTA 8: ATRASOS POR PERÍODO DO DIA (MADRUGADA, MANHÃ, TARDE, NOITE)
-- Objetivo: Validar se o atraso acumula ao longo do dia (Efeito Bola de Neve).
-- Baseado nos filtros de horário definidos (Madrugada 0-6, Manhã 6-12, Tarde 12-18, Noite 18-24).
-- =======================================================================================================================
SELECT 
    CASE 
        WHEN EXTRACT(HOUR FROM scheduled_departure) BETWEEN 0 AND 5 THEN 'Madrugada'
        WHEN EXTRACT(HOUR FROM scheduled_departure) BETWEEN 6 AND 11 THEN 'Manhã'
        WHEN EXTRACT(HOUR FROM scheduled_departure) BETWEEN 12 AND 17 THEN 'Tarde'
        WHEN EXTRACT(HOUR FROM scheduled_departure) >= 18 THEN 'Noite'
    END AS periodo_dia,
    COUNT(flight_id) AS total_voos,
    AVG(departure_delay) AS media_atraso_partida,
    AVG(arrival_delay) AS media_atraso_chegada
FROM 
    silver.silver_flights
GROUP BY 
    1
ORDER BY 
    media_atraso_chegada ASC;

-- =======================================================================================================================
-- CONSULTA 9: PERFORMANCE POR DURAÇÃO DE VOO (CURTA, MÉDIA, LONGA)
-- Objetivo: Analisar se voos mais longos tendem a recuperar ou aumentar o atraso.
-- Baseado nas categorias: Curta (<=90min), Média (90-240min), Longa (>240min).
-- =======================================================================================================================
SELECT 
    CASE 
        WHEN elapsed_time <= 90 THEN 'Curta Duração'
        WHEN elapsed_time > 90 AND elapsed_time <= 240 THEN 'Média Duração'
        WHEN elapsed_time > 240 THEN 'Longa Duração'
        ELSE 'Desconhecido'
    END AS categoria_duracao,
    COUNT(flight_id) AS total_voos,
    AVG(arrival_delay) AS media_atraso_chegada
FROM 
    silver.silver_flights
WHERE 
    elapsed_time IS NOT NULL
GROUP BY 
    1
ORDER BY 
    media_atraso_chegada DESC;

-- =======================================================================================================================
-- CONSULTA 10: MAPA DE CALOR POR ESTADO DE ORIGEM
-- Objetivo: Alimentar uma visualização de mapa para identificar estados problemáticos.
-- Baseado na alternativa visual "Pontos Quentes" mencionada.
-- =======================================================================================================================
SELECT 
    origin_state AS estado_origem,
    COUNT(flight_id) AS volume_voos,
    AVG(arrival_delay) AS media_atraso_chegada,
    SUM(arrival_delay) AS total_minutos_atraso
FROM 
    silver.silver_flights
WHERE 
    origin_state IS NOT NULL
GROUP BY 
    origin_state
ORDER BY 
    volume_voos DESC;