## Camada Gold

- Na **camada Gold**, os dados da camada Silver (ou intermediária) foram **modelados e agregados**.
- O modelo adotado é o **Star Schema** (Esquema Estrela), que é otimizado para consultas analíticas (**BI**) e inteligência artificial (**IA**).
- Os dados estão separados em uma tabela **Fato** central (`fat_flt`) e tabelas **Dimensão** (`dim_apt`, `dim_air`, `dim_dat`) que fornecem contexto.

### Modelo Entidade-Relacionamento (ME-R) - Star Schema

**Tabela Fato**

- fat_flt (Contém as métricas e chaves estrangeiras)

**Tabelas Dimensão**

- dim_apt (Contexto sobre os aeroportos)
- dim_air (Contexto sobre as companhias aéreas)
- dim_dat (Contexto sobre a data)

### Dicionário de Dados – Gold

#### **Tabela: dim_apt**

| Coluna            | Tipo             | Descrição                                          |
| ----------------- | ---------------- | -------------------------------------------------- |
| airport_id        | int              | Identificador único do aeroporto (chave primária). |
| airport_iata_code | varchar(3)       | Código IATA do aeroporto (chave única).            |
| airport_name      | varchar(100)     | Nome do aeroporto.                                 |
| state_code        | varchar(3)       | Código do estado onde o aeroporto se encontra.     |
| state_name        | varchar(100)     | Nome do estado onde o aeroporto se encontra.       |
| city_name         | varchar(100)     | Nome da cidade onde o aeroporto se encontra.       |
| latitude          | double precision | Latitude do aeroporto.                             |
| longitude         | double precision | Longitude do aeroporto.                            |

---

#### **Tabela: dim_air**

| Coluna            | Tipo         | Descrição                                                |
| ----------------- | ------------ | -------------------------------------------------------- |
| airline_id        | int          | Identificador único da companhia aérea (chave primária). |
| airline_iata_code | varchar(3)   | Código IATA da companhia aérea (chave única).            |
| airline_name      | varchar(100) | Nome da companhia aérea.                                 |

---

#### **Tabela: dim_dat**

| Coluna      | Tipo     | Descrição                                     |
| ----------- | -------- | --------------------------------------------- |
| date_id     | int      | Identificador único da data (chave primária). |
| full_date   | date     | Data completa (AAAA-MM-DD).                   |
| year        | smallint | Ano (ex: 2025).                               |
| month       | smallint | Mês (1 a 12).                                 |
| day         | smallint | Dia do mês.                                   |
| day_of_week | smallint | Dia da semana (ex: 1=Segunda, 7=Domingo).     |
| quarter     | smallint | Trimestre (1 a 4).                            |
| is_holiday  | boolean  | Indica se é feriado (True/False).             |

---

#### **Tabela: fat_flt**

| Coluna              | Tipo             | Descrição                                        |
| ------------------- | ---------------- | ------------------------------------------------ |
| flight_id           | int              | Identificador único do voo (chave primária).     |
| date_id             | int              | Chave estrangeira para dim_dat (FK).             |
| airline_id          | int              | Chave estrangeira para dim_air (FK).             |
| origin_airport_id   | int              | Chave estrangeira para dim_apt (origem) (FK).    |
| dest_airport_id     | int              | Chave estrangeira para dim_apt (destino) (FK).   |
| scheduled_departure | double precision | Horário programado de partida (HHMM).            |
| departure_time      | double precision | Horário real de partida (HHMM).                  |
| scheduled_arrival   | double precision | Horário programado de chegada (HHMM).            |
| arrival_time        | double precision | Horário real de chegada (HHMM).                  |
| wheels_off          | double precision | Horário que a aeronave saiu do solo (HHMM).      |
| wheels_on           | double precision | Horário que a aeronave tocou o solo (HHMM).      |
| distance            | double precision | Distância do voo (milhas/km).                    |
| air_time            | double precision | Tempo em voo (minutos).                          |
| elapsed_time        | double precision | Tempo total do voo (minutos).                    |
| scheduled_time      | double precision | Tempo de voo programado (minutos).               |
| taxi_out            | double precision | Tempo de táxi na saída (minutos).                |
| taxi_in             | double precision | Tempo de táxi na chegada (minutos).              |
| departure_delay     | double precision | Atraso na decolagem (minutos).                   |
| arrival_delay       | double precision | Atraso na chegada (minutos).                     |
| air_system_delay    | double precision | Atraso devido ao sistema aéreo (minutos).        |
| security_delay      | double precision | Atraso devido a questões de segurança (minutos). |
| airline_delay       | double precision | Atraso devido à companhia aérea (minutos).       |
| late_aircraft_delay | double precision | Atraso por aeronave anterior atrasada (minutos). |
| weather_delay       | double precision | Atraso devido ao clima (minutos).                |

## Histórico de Versões

| Versão | Data       | Descrição                                              | Autor(es)                                        | Revisor(es)                                      |
| ------ | ---------- | ------------------------------------------------------ | ------------------------------------------------ | ------------------------------------------------ |
| `1.0`  | 07/11/2025 | Criação inicial do dicionário de dados da Camada Gold. | [Joao Schmitz](https://github.com/joaoschmitz)   | [Matheus Henrique](https://github.com/mathonaut) |
| `1.1`  | 26/11/2025 | Alteração se adequar aos mnemônicos.                   | [Matheus Henrique](https://github.com/mathonaut) | [Pedro Henrique](https://github.com/phmelosilva) |
