# Dicionário de Dados: Voos Atrasados em 2015

## Sobre o Projeto
Este projeto documenta as camadas de dados **Bronze** e **Silver** de um pipeline sobre voos atrasados nos Estados Unidos em 2015.
O objetivo é organizar, tratar e disponibilizar os dados de forma confiável, garantindo a rastreabilidade das transformações realizadas.

--

##  Estrutura do Repositório
- `README.md`: documentação geral e dicionário de dados.  
- `bronze/`: camada de dados brutos (dados originais, sem tratamento).  
- `silver/`: camada de dados tratados e enriquecidos (com atributos integrados e padronizados).  

--

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

--

### Dicionário de Dados – Bronze

#### **Tabela: FLIGHT**
| Coluna                | Tipo        | Descrição                                                                  |
|------------------------|-------------|---------------------------------------------------------------------------|
| year                   | Inteiro     | Ano do voo.                                                               |
| month                  | Inteiro     | Mês do voo (1 a 12).                                                      |
| day                    | Inteiro     | Dia do mês.                                                               |
| day_of_week            | Inteiro     | Dia da semana (1=Segunda, 7=Domingo).                                     |
| airline                | Texto       | Código da companhia aérea (IATA).                                         |
| flight_number          | Texto       | Número do voo.                                                            |
| tail_number            | Texto       | Identificação da aeronave.                                                |
| origin_airport         | Texto       | Código IATA do aeroporto de origem.                                       |
| destination_airport    | Texto       | Código IATA do aeroporto de destino.                                      |
| scheduled_departure    | Inteiro     | Horário programado de partida (HHMM).                                     |
| departure_time         | Inteiro     | Horário real de partida (HHMM).                                           |
| departure_delay        | Inteiro     | Atraso na decolagem (minutos).                                            |
| taxi_out               | Inteiro     | Tempo gasto em solo antes da decolagem (minutos).                         |
| wheels_off             | Inteiro     | Horário em que a aeronave decolou (HHMM).                                 |
| scheduled_time         | Inteiro     | Tempo de voo programado (minutos).                                        |
| elapsed_time           | Inteiro     | Tempo total do voo (minutos).                                             |
| air_time               | Inteiro     | Tempo em voo (minutos).                                                   |
| distance               | Inteiro     | Distância percorrida (milhas).                                            |
| wheels_on              | Inteiro     | Horário em que a aeronave pousou (HHMM).                                  |
| taxi_in                | Inteiro     | Tempo gasto em solo após o pouso (minutos).                               |
| schedule_arrival       | Inteiro     | Horário programado de chegada (HHMM).                                     |
| arrival_time           | Inteiro     | Horário real de chegada (HHMM).                                           |
| arrival_delay          | Inteiro     | Atraso na chegada (minutos).                                              |
| diverted               | Booleano    | Indica se o voo foi desviado (1=Sim, 0=Não).                              |
| cancelled              | Booleano    | Indica se o voo foi cancelado (1=Sim, 0=Não).                             |
| cancellation_reason    | Texto       | Motivo do cancelamento (A=Companhia, B=Clima, C=Segurança, D=Outros).     |
| air_system_delay       | Inteiro     | Atraso devido ao sistema aéreo (minutos).                                 |
| security_delay         | Inteiro     | Atraso devido a questões de segurança (minutos).                          |
| airline_delay          | Inteiro     | Atraso devido à companhia aérea (minutos).                                |
| late_aircraft_delay    | Inteiro     | Atraso causado por chegada tardia de outra aeronave (minutos).            |
| weather_delay          | Inteiro     | Atraso devido ao clima (minutos).                                         |

--

#### **Tabela: AIRLINE**
| Coluna     | Tipo  | Descrição                        |
|------------|-------|----------------------------------|
| iata_code  | Texto | Código IATA da companhia aérea.  |
| airline    | Texto | Nome da companhia aérea.         |

--

#### **Tabela: AIRPORT**
| Coluna     | Tipo  | Descrição                                |
|------------|-------|------------------------------------------|
| iata_code  | Texto | Código IATA do aeroporto.                |
| airport    | Texto | Nome do aeroporto.                       |
| city       | Texto | Cidade onde o aeroporto está localizado. |
| state      | Texto | Estado onde o aeroporto está localizado. |
| country    | Texto | País onde o aeroporto está localizado.   |
| latitude   | Real  | Latitude geográfica.                     |
| longitude  | Real  | Longitude geográfica.                    |

--

## Camada Silver

Na **camada Silver**, os dados da Bronze foram **limpos e integrados**.  
As entidades **AIRLINE** e **AIRPORT** foram unificadas na entidade **FLIGHT**.  
Foram removidos atributos irrelevantes ou redundantes e criado um identificador único `id_flight`.

### Modelo Entidade-Relacionamento (ME-R)
**Entidades**  
- FLIGHT (única consolidada)  

--

### Dicionário de Dados – Silver

#### **Tabela: FLIGHT**
| Coluna                        | Tipo     | Descrição                                                                 |
|-------------------------------|----------|---------------------------------------------------------------------------|
| id_flight                     | Inteiro  | Identificador único do voo (chave primária).                              |
| month                         | Inteiro  | Mês do voo (1 a 12).                                                      |
| day                           | Inteiro  | Dia do mês.                                                               |
| day_of_week                   | Inteiro  | Dia da semana (1=Segunda, 7=Domingo).                                     |
| airline_iata_code             | Texto    | Código IATA da companhia aérea.                                           |
| airline_name                  | Texto    | Nome da companhia aérea.                                                  |
| tail_number                   | Texto    | Identificação da aeronave.                                                |
| origin_airport_iata_code      | Texto    | Código IATA do aeroporto de origem.                                       |
| origin_airport_name           | Texto    | Nome do aeroporto de origem.                                              |
| origin_airport_city           | Texto    | Cidade do aeroporto de origem.                                            |
| origin_airport_state          | Texto    | Estado do aeroporto de origem.                                            |
| origin_airport_latitude       | Real     | Latitude do aeroporto de origem.                                          |
| origin_airport_longitude      | Real     | Longitude do aeroporto de origem.                                         |
| destination_airport_iata_code | Texto    | Código IATA do aeroporto de destino.                                      |
| destination_airport_name      | Texto    | Nome do aeroporto de destino.                                             |
| destination_airport_city      | Texto    | Cidade do aeroporto de destino.                                           |
| destination_airport_state     | Texto    | Estado do aeroporto de destino.                                           |
| destination_airport_latitude  | Real     | Latitude do aeroporto de destino.                                         |
| destination_airport_longitude | Real     | Longitude do aeroporto de destino.                                        |
| scheduled_departure           | Inteiro  | Horário programado de partida (HHMM).                                     |
| departure_time                | Inteiro  | Horário real de partida (HHMM).                                           |
| departure_delay               | Inteiro  | Atraso na decolagem (minutos).                                            |
| scheduled_time                | Inteiro  | Tempo de voo programado (minutos).                                        |
| elapsed_time                  | Inteiro  | Tempo total do voo (minutos).                                             |
| air_time                      | Inteiro  | Tempo em voo (minutos).                                                   |
| schedule_arrival              | Inteiro  | Horário programado de chegada (HHMM).                                     |
| arrival_time                  | Inteiro  | Horário real de chegada (HHMM).                                           |
| arrival_delay                 | Inteiro  | Atraso na chegada (minutos).                                              |
| air_system_delay              | Inteiro  | Atraso devido ao sistema aéreo (minutos).                                 |
| security_delay                | Inteiro  | Atraso devido a questões de segurança (minutos).                          |
| airline_delay                 | Inteiro  | Atraso devido à companhia aérea (minutos).                                |
| late_aircraft_delay           | Inteiro  | Atraso causado por chegada tardia de outra aeronave (minutos).            |
| weather_delay                 | Inteiro  | Atraso devido ao clima (minutos).                                         |

--

## Histórico de Versões
- **v1.0 (21/09/2025)**: Criação inicial do README com dicionário de dados.
