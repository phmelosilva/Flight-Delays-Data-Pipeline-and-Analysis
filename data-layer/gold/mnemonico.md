# Arquivo de Mnemônico e Dicionário de Dados (Camada Gold)

Este documento define os padrões de nomenclatura e o significado das colunas utilizadas no Data Warehouse (camada Gold) do projeto, conforme definido no DDL `gold_ddl.sql`.

## 1. Padrões de Nomenclatura (mnemônico)

Usamos um conjunto de prefixos padronizados para identificar rapidamente o propósito de cada tabela e objeto no banco de dados.

| Prefixo/Sufixo | Significado          | Descrição                                                                                              |
| :------------- | :------------------- | :----------------------------------------------------------------------------------------------------- |
| `dim_`         | Dimensão (Dimension) | Prefixo para tabelas de dimensão. Elas contêm o "quem, o quê, onde, quando" (o contexto).              |
| `fat_`         | Fato (Fact)          | Prefixo para tabelas fato. Elas contêm os eventos, métricas e valores numéricos.                       |
| `_id`          | Identificador        | Sufixo para chaves primárias e estrangeiras. Substitui o mnemônico `SRK` (Surrogate Key) por clareza.  |
| `pk_`          | Primary Key          | Prefixo usado na definição de constraints de Chave Primária.                                           |
| `fk_`          | Foreign Key          | Prefixo usado na definição de constraints de Chave Estrangeira (ligações entre fato e dimensões).      |
| `idx_`         | Índice (Index)       | Prefixo para índices, que são usados para otimizar a performance de consultas (`SELECT`).              |
| `_iata_code`   | IATA Code            | Sufixo para colunas que armazenam os códigos de negócio (ex: `LA` para LATAM, `JFK` para o aeroporto). |

---

## 2. Dicionário de Tabelas (Star Schema)

Nosso Data Warehouse segue um **Star Schema**, composto por uma tabela Fato central (`fat_flt`) e três dimensões (`dim_apt`, `dim_air`, `dim_dat`).

### 2.1. Tabelas de Dimensão

#### `dim_apt`

Armazena informações descritivas e de localização de cada aeroporto.

| Coluna                   | Descrição                                                | Tipo de Dado       | Chave |
| :----------------------- | :------------------------------------------------------- | :----------------- | :---- |
| `airport_id`             | **Chave Surrogada (SRK)** da dimensão aeroporto.         | `SERIAL` (Inteiro) | PK    |
| `airport_iata_code`      | Chave de negócio (código IATA de 3 letras) do aeroporto. | `VARCHAR(3)`       |       |
| `airport_name`           | Nome completo do aeroporto.                              | `VARCHAR(100)`     |       |
| `state_code`             | Sigla do estado do aeroporto (ex: 'NY').                 | `VARCHAR(3)`       |       |
| `state_name`             | Nome do estado.                                          | `VARCHAR(100)`     |       |
| `city_name`              | Nome da cidade onde o aeroporto está localizado.         | `VARCHAR(100)`     |       |
| `latitude` / `longitude` | Coordenadas geográficas para uso em mapas.               | `DOUBLE PRECISION` |       |

#### `dim_air`

Armazena informações descritivas de cada companhia aérea.

| Coluna              | Descrição                                                     | Tipo de Dado       | Chave |
| :------------------ | :------------------------------------------------------------ | :----------------- | :---- |
| `airline_id`        | **Chave Surrogada (SRK)** da dimensão companhia aérea.        | `SERIAL` (Inteiro) | PK    |
| `airline_iata_code` | Chave de negócio (código IATA de 2 ou 3 letras) da companhia. | `VARCHAR(3)`       |       |
| `airline_name`      | Nome completo da companhia aérea.                             | `VARCHAR(100)`     |       |

#### `dim_dat`

Armazena atributos de tempo para permitir análises sazonais. Esta é uma dimensão padrão em qualquer DW.

| Coluna        | Descrição                                      | Tipo de Dado       | Chave |
| :------------ | :--------------------------------------------- | :----------------- | :---- |
| `date_id`     | **Chave Surrogada (SRK)** da dimensão de data. | `SERIAL` (Inteiro) | PK    |
| `full_date`   | A data completa (ex: '2015-01-01').            | `DATE`             |       |
| `year`        | Ano (ex: 2015).                                | `SMALLINT`         |       |
| `month`       | Mês (1-12).                                    | `SMALLINT`         |       |
| `day`         | Dia (1-31).                                    | `SMALLINT`         |       |
| `day_of_week` | Dia da semana (ex: 0=Domingo, 1=Segunda).      | `SMALLINT`         |       |
| `quarter`     | Trimestre do ano (1-4).                        | `SMALLINT`         |       |
| `is_holiday`  | Indicador (True/False) se a data é um feriado. | `BOOLEAN`          |       |

---

### 2.2. Tabela Fato

#### `fat_flt`

Tabela central que armazena os eventos (voos) e suas métricas (medidas) numéricas.

| Coluna                | Descrição                                                                          | Tipo de Dado       | Chave |
| :-------------------- | :--------------------------------------------------------------------------------- | :----------------- | :---- |
| **Chaves (FKs)**      |                                                                                    |                    |       |
| `flight_id`           | Identificador único do voo (chave primária da fato).                               | `INTEGER`          | PK    |
| `date_id`             | Chave estrangeira que aponta para `dim_date.date_id`.                              | `INTEGER`          | FK    |
| `airline_id`          | Chave estrangeira que aponta para `dim_airline.airline_id`.                        | `INTEGER`          | FK    |
| `origin_airport_id`   | Chave estrangeira que aponta para `dim_airport.airport_id` (Aeroporto de Origem).  | `INTEGER`          | FK    |
| `dest_airport_id`     | Chave estrangeira que aponta para `dim_airport.airport_id` (Aeroporto de Destino). | `INTEGER`          | FK    |
| **Medidas (Tempos)**  |                                                                                    |                    |       |
| `scheduled_departure` | Horário agendado da partida.                                                       | `DOUBLE PRECISION` |       |
| `departure_time`      | Horário real da partida.                                                           | `DOUBLE PRECISION` |       |
| `scheduled_arrival`   | Horário agendado da chegada.                                                       | `DOUBLE PRECISION` |       |
| `arrival_time`        | Horário real da chegada.                                                           | `DOUBLE PRECISION` |       |
| `air_time`            | Tempo total em que o avião esteve no ar.                                           | `DOUBLE PRECISION` |       |
| `elapsed_time`        | Tempo total do voo (de portão a portão).                                           | `DOUBLE PRECISION` |       |
| `scheduled_time`      | Tempo de voo agendado (de portão a portão).                                        | `DOUBLE PRECISION` |       |
| `distance`            | Distância do voo.                                                                  | `DOUBLE PRECISION` |       |
| **Medidas (Atrasos)** |                                                                                    |                    |       |
| `departure_delay`     | Atraso na partida (em minutos).                                                    | `DOUBLE PRECISION` |       |
| `arrival_delay`       | Atraso na chegada (em minutos).                                                    | `DOUBLE PRECISION` |       |
| `air_system_delay`    | Atraso causado pelo sistema aéreo (em minutos).                                    | `DOUBLE PRECISION` |       |
| `security_delay`      | Atraso causado pela segurança (em minutos).                                        | `DOUBLE PRECISION` |       |
| `airline_delay`       | Atraso causado pela companhia aérea (em minutos).                                  | `DOUBLE PRECISION` |       |
| `late_aircraft_delay` | Atraso causado por aeronave atrasada (em minutos).                                 | `DOUBLE PRECISION` |       |
| `weather_delay`       | Atraso causado por condições climáticas (em minutos).                              | `DOUBLE PRECISION` |       |

## Histórico de Versões

| Versão | Data       | Descrição                                                   | Autor(es)                                        | Revisor(es)                                      |
| ------ | ---------- | ----------------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------ |
| `1.0`  | 07/11/2025 | Criação inicial do arquivo de mimemônicos do DataWarehouse. | [Pedro Henrique](https://github.com/phmelosilva) | [Joao Schmitz](https://github.com/joaoschmitz)   |
| `1.1`  | 26/11/2025 | Corrige nomenclaturas.                                      | [Matheus Henrique](https://github.com/mathonaut) | [Pedro Henrique](https://github.com/phmelosilva) |
