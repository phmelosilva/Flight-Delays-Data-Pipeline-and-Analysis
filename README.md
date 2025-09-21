# Dicionário de Dados: Voos Atrasados em 2015

## Sobre o Projeto
Este projeto documenta as camadas de dados **Bronze** e **Silver** de um pipeline sobre voos atrasados nos Estados Unidos em 2015.
O objetivo é organizar, tratar e disponibilizar os dados de forma confiável, garantindo a rastreabilidade das transformações realizadas.

##  Estrutura do Repositório
- `README.md`: documentação geral e dicionário de dados.  
- `bronze/`: camada de dados brutos (dados originais, sem tratamento).  
- `silver/`: camada de dados tratados e enriquecidos (com atributos integrados e padronizados).  

## Camada Bronze

A **camada Bronze** contém os dados originais (raw), preservando sua granularidade.

### Modelo Entidade-Relacionamento (ME-R)
**Entidades**  
- FLIGHT  
- AIRLINE  
- AIRPORT  

**Relacionamentos**  
- AIRLINE – realiza – FLIGHT (N:1)  
- FLIGHT – decola – AIRPORT (1:N)  
- FLIGHT – aterrissa – AIRPORT (1:N)  

### Dicionário de Dados – Bronze

#### **Tabela: FLIGHT**
| Coluna            b    | Tipo        | Descrição                                                                 |
|------------------------|-------------|---------------------------------------------------------------------------|
| year                   | int         | Ano do voo.                                                               |
| month                  | int         | Mês do voo (1 a 12).                                                      |
| day                    | int         | Dia do mês.                                                               |
| day_of_week            | int         | Dia da semana (1=Segunda, 7=Domingo).                                     |
| airline                | varchar     | Código da companhia aérea (IATA).                                         |
| flight_number          | varchar     | Número do voo.                                                            |
| tail_number            | varchar     | Identificação da aeronave.                                                |
| origin_airport         | varchar     | Código IATA do aeroporto de origem.                                       |
| destination_airport    | varchar     | Código IATA do aeroporto de destino.                                      |
| scheduled_departure    | int         | Horário programado de partida (HHMM).                                     |
| departure_time         | int         | Horário real de partida (HHMM).                                           |
| departure_delay        | int         | Atraso na decolagem (minutos).                                            |
| taxi_out               | int         | Tempo gasto em solo antes da decolagem (minutos).                         |
| wheels_off             | int         | Horário em que a aeronave decolou (HHMM).                                 |
| scheduled_time         | int         | Tempo de voo programado (minutos).                                        |
| elapsed_time           | int         | Tempo total do voo (minutos).                                             |
| air_time               | int         | Tempo em voo (minutos).                                                   |
| distance               | int         | Distância percorrida (milhas).                                            |
| wheels_on              | int         | Horário em que a aeronave pousou (HHMM).                                  |
| taxi_in                | int         | Tempo gasto em solo após o pouso (minutos).                               |
| schedule_arrival       | int         | Horário programado de chegada (HHMM).                                     |
| arrival_time           | int         | Horário real de chegada (HHMM).                                           |
| arrival_delay          | int         | Atraso na chegada (minutos).                                              |
| diverted               | boolean     | Indica se o voo foi desviado (1=Sim, 0=Não).                              |
| cancelled              | boolean     | Indica se o voo foi cancelado (1=Sim, 0=Não).                             |
| cancellation_reason    | varchar     | Motivo do cancelamento (A=Companhia, B=Clima, C=Segurança, D=Outros).     |
| air_system_delay       | int         | Atraso devido ao sistema aéreo (minutos).                                 |
| security_delay         | int         | Atraso devido a questões de segurança (minutos).                          |
| airline_delay          | int         | Atraso devido à companhia aérea (minutos).                                |
| late_aircraft_delay    | int         | Atraso causado por chegada tardia de outra aeronave (minutos).            |
| weather_delay          | int         | Atraso devido ao clima (minutos).                                         |

#### **Tabela: AIRLINE**
| Coluna     | Tipo    | Descrição                        |
|------------|---------|----------------------------------|
| iata_code  | varchar | Código IATA da companhia aérea.  |
| airline    | varchar | Nome da companhia aérea.         |

#### **Tabela: AIRPORT**
| Coluna     | Tipo    | Descrição                                |
|------------|---------|------------------------------------------|
| iata_code  | varchar | Código IATA do aeroporto.                |
| airport    | varchar | Nome do aeroporto.                       |
| city       | varchar | Cidade onde o aeroporto está localizado. |
| state      | varchar | Estado onde o aeroporto está localizado. |
| country    | varchar | País onde o aeroporto está localizado.   |
| latitude   | decimal | Latitude geográfica.                     |
| longitude  | decimal | Longitude geográfica.                    |

## Camada Silver

Na **camada Silver**, os dados da Bronze foram **limpos e integrados**.  
As entidades **AIRLINE** e **AIRPORT** foram unificadas na entidade **FLIGHT**.  
Foram removidos atributos irrelevantes ou redundantes e criado um identificador único `id_flight`.

### Modelo Entidade-Relacionamento (ME-R)
**Entidades**  
- FLIGHT (única consolidada)  

### Dicionário de Dados – Silver

#### **Tabela: FLIGHT**
| Coluna                        | Tipo     | Descrição                                                                 |
|-------------------------------|----------|---------------------------------------------------------------------------|
| id_flight                     | int      | Identificador único do voo (chave primária).                              |
| month                         | int      | Mês do voo (1 a 12).                                                      |
| day                           | int      | Dia do mês.                                                               |
| day_of_week                   | int      | Dia da semana (1=Segunda, 7=Domingo).                                     |
| airline_iata_code             | varchar  | Código IATA da companhia aérea.                                           |
| airline_name                  | varchar  | Nome da companhia aérea.                                                  |
| tail_number                   | varchar  | Identificação da aeronave.                                                |
| origin_airport_iata_code      | varchar  | Código IATA do aeroporto de origem.                                       |
| origin_airport_name           | varchar  | Nome do aeroporto de origem.                                              |
| origin_airport_city           | varchar  | Cidade do aeroporto de origem.                                            |
| origin_airport_state          | varchar  | Estado do aeroporto de origem.                                            |
| origin_airport_latitude       | decimal  | Latitude do aeroporto de origem.                                          |
| origin_airport_longitude      | decimal  | Longitude do aeroporto de origem.                                         |
| destination_airport_iata_code | varchar  | Código IATA do aeroporto de destino.                                      |
| destination_airport_name      | varchar  | Nome do aeroporto de destino.                                             |
| destination_airport_city      | varchar  | Cidade do aeroporto de destino.                                           |
| destination_airport_state     | varchar  | Estado do aeroporto de destino.                                           |
| destination_airport_latitude  | decimal  | Latitude do aeroporto de destino.                                         |
| destination_airport_longitude | decimal  | Longitude do aeroporto de destino.                                        |
| scheduled_departure           | int      | Horário programado de partida (HHMM).                                     |
| departure_time                | int      | Horário real de partida (HHMM).                                           |
| departure_delay               | int      | Atraso na decolagem (minutos).                                            |
| scheduled_time                | int      | Tempo de voo programado (minutos).                                        |
| elapsed_time                  | int      | Tempo total do voo (minutos).                                             |
| air_time                      | int      | Tempo em voo (minutos).                                                   |
| schedule_arrival              | int      | Horário programado de chegada (HHMM).                                     |
| arrival_time                  | int      | Horário real de chegada (HHMM).                                           |
| arrival_delay                 | int      | Atraso na chegada (minutos).                                              |
| air_system_delay              | int      | Atraso devido ao sistema aéreo (minutos).                                 |
| security_delay                | int      | Atraso devido a questões de segurança (minutos).                          |
| airline_delay                 | int      | Atraso devido à companhia aérea (minutos).                                |
| late_aircraft_delay           | int      | Atraso causado por chegada tardia de outra aeronave (minutos).            |
| weather_delay                 | int      | Atraso devido ao clima (minutos).                                         |

## Histórico de Versões
- **v1.0 (21/09/2025)**: Criação inicial do README com dicionário de dados.
