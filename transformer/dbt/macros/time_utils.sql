-- Macro: normalize_hhmm
-- Descrição: Normaliza valores no formato HHMM.
-- Remove sufixo '.0', preenche à esquerda com zeros e retorna NULL para valores vazios.

{% macro normalize_hhmm(col_name) -%}

    CASE
        WHEN {{ col_name }} IS NULL OR {{ col_name }} = ''
            THEN NULL

        ELSE LPAD(
            REGEXP_REPLACE({{ col_name }}::text, '\.0$', ''),
            4,
            '0'
        )
    END

{%- endmacro %}


-- Macro: build_ts
-- Descrição: Constrói um timestamp combinando uma coluna de data (YYYY-MM-DD)
-- com uma coluna HHMM já normalizada. Trata valores inválidos e o caso especial '2400'.

{% macro build_ts(date_col, hhmm_col) -%}

    CASE
        WHEN {{ hhmm_col }} IS NULL
            THEN NULL

        -- Caso especial: '2400' representa virada para o próximo dia
        WHEN {{ hhmm_col }} = '2400'
            THEN ({{ date_col }}::date + INTERVAL '1 day')::timestamp

        -- Valores HHMM inválidos
        WHEN {{ hhmm_col }} !~ '^[0-2][0-9][0-5][0-9]$'
            THEN NULL

        ELSE TO_TIMESTAMP(
            {{ date_col }}::text || ' ' || {{ hhmm_col }},
            'YYYY-MM-DD HH24MI'
        )
    END

{%- endmacro %}
