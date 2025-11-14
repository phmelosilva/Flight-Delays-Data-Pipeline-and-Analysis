## Camada Silver

- Na **camada Silver**, os dados da Bronze foram **limpos e integrados**.
- As entidades **AIRLINE** e **AIRPORT** foram unificadas na entidade **FLIGHT**.
- Foram removidos atributos irrelevantes ou redundantes e criado um identificador único `id_flight`.

### Modelo Entidade-Relacionamento (ME-R)

**Entidades**

- FLIGHT (única consolidada)

### Dicionário de Dados – Silver

#### **Tabela: FLIGHT**

| Coluna                        | Tipo    | Descrição                                                      |
| ----------------------------- | ------- | -------------------------------------------------------------- |
| id_flight                     | int     | Identificador único do voo (chave primária).                   |
| month                         | int     | Mês do voo (1 a 12).                                           |
| day                           | int     | Dia do mês.                                                    |
| day_of_week                   | int     | Dia da semana (1=Segunda, 7=Domingo).                          |
| airline_iata_code             | varchar | Código IATA da companhia aérea.                                |
| airline_name                  | varchar | Nome da companhia aérea.                                       |
| tail_number                   | varchar | Identificação da aeronave.                                     |
| origin_airport_iata_code      | varchar | Código IATA do aeroporto de origem.                            |
| origin_airport_name           | varchar | Nome do aeroporto de origem.                                   |
| origin_airport_city           | varchar | Cidade do aeroporto de origem.                                 |
| origin_airport_state          | varchar | Estado do aeroporto de origem.                                 |
| origin_airport_latitude       | decimal | Latitude do aeroporto de origem.                               |
| origin_airport_longitude      | decimal | Longitude do aeroporto de origem.                              |
| destination_airport_iata_code | varchar | Código IATA do aeroporto de destino.                           |
| destination_airport_name      | varchar | Nome do aeroporto de destino.                                  |
| destination_airport_city      | varchar | Cidade do aeroporto de destino.                                |
| destination_airport_state     | varchar | Estado do aeroporto de destino.                                |
| destination_airport_latitude  | decimal | Latitude do aeroporto de destino.                              |
| destination_airport_longitude | decimal | Longitude do aeroporto de destino.                             |
| scheduled_departure           | int     | Horário programado de partida (HHMM).                          |
| departure_time                | int     | Horário real de partida (HHMM).                                |
| departure_delay               | int     | Atraso na decolagem (minutos).                                 |
| scheduled_time                | int     | Tempo de voo programado (minutos).                             |
| elapsed_time                  | int     | Tempo total do voo (minutos).                                  |
| air_time                      | int     | Tempo em voo (minutos).                                        |
| schedule_arrival              | int     | Horário programado de chegada (HHMM).                          |
| arrival_time                  | int     | Horário real de chegada (HHMM).                                |
| arrival_delay                 | int     | Atraso na chegada (minutos).                                   |
| air_system_delay              | int     | Atraso devido ao sistema aéreo (minutos).                      |
| security_delay                | int     | Atraso devido a questões de segurança (minutos).               |
| airline_delay                 | int     | Atraso devido à companhia aérea (minutos).                     |
| late_aircraft_delay           | int     | Atraso causado por chegada tardia de outra aeronave (minutos). |
| weather_delay                 | int     | Atraso devido ao clima (minutos).                              |

## Histórico de Versões

| Versão | Data       | Descrição                                           | Autor(es)                                        | Revisor(es)                                      |
| ------ | ---------- | --------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------ |
| `1.0`  | 21/09/2025 | Criação inicial do README com dicionário de dados.  | [Júlia Takaki](https://github.com/juliatakaki)   | [Matheus Henrique](https://github.com/mathonaut) |
| `1.1`  | 24/09/2025 | Ajustes no README com explicação sobre camada Gold. | [Pedro Henrique](https://github.com/phmelosilva) | [Matheus Henrique](https://github.com/mathonaut) |
