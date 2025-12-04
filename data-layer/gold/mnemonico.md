# Mnemônicos

Este documento é um apêndice do dicionário de dados e define os padrões de nomenclatura e o significado dos atributos utilizadas no Data Warehouse do projeto.

## 1. Padrões de Nomenclatura

Usamos um conjunto de prefixos padronizados para identificar rapidamente o propósito de cada Entidade e objeto no banco de dados.

| Prefixo/Sufixo | Significado       | Descrição                                                                  |
| :------------- | :---------------- | :------------------------------------------------------------------------- |
| `dim_`         | Dimensão          | Prefixo para Entidades de dimensão.                                        |
| `fat_`         | Fato              | Prefixo para Entidades fato.                                               |
| `srk_`         | Identificador     | Prefixo para chaves primárias e estrangeiras (na fato).                    |
| `pk_`          | Chave Primária    | Prefixo usado na definição de constraints de chave primária.               |
| `fk_`          | Chave Estrangeira | Prefixo usado na definição de constraints de chave estrangeira.            |
| `idx_`         | Índice            | Prefixo para índices, utilizados para otimizar a performance de consultas. |
| `_iata`        | IATA Code         | Sufixo para Atributos que armazenam os códigos no padrão da IATA.          |

---

## 2. Nomenclaturas e Significados

| Atributo     | Descrição                                                 |
| :----------- | :-------------------------------------------------------- |
| `srk_air`    | Chave primária gerada para dimensão companhia aérea.      |
| `air_iata`   | Código IATA da companhia aérea.                           |
| `air_name`   | Nome completo da companhia aérea.                         |
| `srk_apt`    | Chave primária gerada para dimensão aeroporto.            |
| `apt_iata`   | Código IATA do aeroporto.                                 |
| `apt_name`   | Nome completo do aeroporto.                               |
| `st_cd`      | Sigla do estado do aeroporto (ex: 'NY').                  |
| `st_name`    | Nome do estado.                                           |
| `cty_name`   | Nome da cidade onde o aeroporto está localizado.          |
| `lat_val`    | Latitude na qual o aeroporto de localiza.                 |
| `lon_val`    | Longitude na qual o aeroporto de localiza.                |
| `srk_dat`    | Chave primária gerada para dimensão de data.              |
| `full_date`  | A data completa (ex: '2015-01-01').                       |
| `yr`         | Ano (ex: 2015).                                           |
| `mm`         | Mês (1-12).                                               |
| `dd`         | Dia (1-31).                                               |
| `dow`        | Dia da semana (0=Domingo).                                |
| `qtr`        | Trimestre do ano (1-4).                                   |
| `is_hol`     | Indicador se a data é um feriado.                         |
| `srk_flt`    | Chave primária gerada para a fato de voos.                |
| `srk_ori`    | Chave estrangeira que aponta para o aeroporto de origem.  |
| `srk_dst`    | Chave estrangeira que aponta para o aeroporto de destino. |
| `sch_dep`    | Horário agendado da partida.                              |
| `dep_time`   | Horário real da partida.                                  |
| `sch_arr`    | Horário agendado da chegada.                              |
| `arr_time`   | Horário real da chegada.                                  |
| `dist_val`   | Distância do voo.                                         |
| `air_time`   | Tempo total em que o avião esteve no ar.                  |
| `elp_time`   | Tempo total do voo (de portão a portão).                  |
| `sch_time`   | Tempo de voo agendado (de portão a portão).               |
| `dep_dly`    | Atraso na partida (em minutos).                           |
| `arr_dly`    | Atraso na chegada (em minutos).                           |
| `sys_dly`    | Atraso causado pelo sistema aéreo (em minutos).           |
| `sec_dly`    | Atraso causado pela segurança (em minutos).               |
| `air_dly`    | Atraso causado pela companhia aérea (em minutos).         |
| `acft_dly`   | Atraso causado por aeronave atrasada (em minutos).        |
| `wx_dly`     | Atraso causado por condições climáticas (em minutos).     |
| `is_ovn_flt` | Indica se um voo atravesou o dia.                         |
