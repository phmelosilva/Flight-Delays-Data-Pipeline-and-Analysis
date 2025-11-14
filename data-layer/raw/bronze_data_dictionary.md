### Dicionário de Dados – Bronze

#### **Tabela: FLIGHT**

| Coluna              | Tipo    | Descrição                                                             |
| ------------------- | ------- | --------------------------------------------------------------------- |
| year                | int     | Ano do voo.                                                           |
| month               | int     | Mês do voo (1 a 12).                                                  |
| day                 | int     | Dia do mês.                                                           |
| day_of_week         | int     | Dia da semana (1=Segunda, 7=Domingo).                                 |
| airline             | varchar | Código da companhia aérea (IATA).                                     |
| flight_number       | varchar | Número do voo.                                                        |
| tail_number         | varchar | Identificação da aeronave.                                            |
| origin_airport      | varchar | Código IATA do aeroporto de origem.                                   |
| destination_airport | varchar | Código IATA do aeroporto de destino.                                  |
| scheduled_departure | int     | Horário programado de partida (HHMM).                                 |
| departure_time      | int     | Horário real de partida (HHMM).                                       |
| departure_delay     | int     | Atraso na decolagem (minutos).                                        |
| taxi_out            | int     | Tempo gasto em solo antes da decolagem (minutos).                     |
| wheels_off          | int     | Horário em que a aeronave decolou (HHMM).                             |
| scheduled_time      | int     | Tempo de voo programado (minutos).                                    |
| elapsed_time        | int     | Tempo total do voo (minutos).                                         |
| air_time            | int     | Tempo em voo (minutos).                                               |
| distance            | int     | Distância percorrida (milhas).                                        |
| wheels_on           | int     | Horário em que a aeronave pousou (HHMM).                              |
| taxi_in             | int     | Tempo gasto em solo após o pouso (minutos).                           |
| schedule_arrival    | int     | Horário programado de chegada (HHMM).                                 |
| arrival_time        | int     | Horário real de chegada (HHMM).                                       |
| arrival_delay       | int     | Atraso na chegada (minutos).                                          |
| diverted            | boolean | Indica se o voo foi desviado (1=Sim, 0=Não).                          |
| cancelled           | boolean | Indica se o voo foi cancelado (1=Sim, 0=Não).                         |
| cancellation_reason | varchar | Motivo do cancelamento (A=Companhia, B=Clima, C=Segurança, D=Outros). |
| air_system_delay    | int     | Atraso devido ao sistema aéreo (minutos).                             |
| security_delay      | int     | Atraso devido a questões de segurança (minutos).                      |
| airline_delay       | int     | Atraso devido à companhia aérea (minutos).                            |
| late_aircraft_delay | int     | Atraso causado por chegada tardia de outra aeronave (minutos).        |
| weather_delay       | int     | Atraso devido ao clima (minutos).                                     |

#### **Tabela: AIRLINE**

| Coluna    | Tipo    | Descrição                       |
| --------- | ------- | ------------------------------- |
| iata_code | varchar | Código IATA da companhia aérea. |
| airline   | varchar | Nome da companhia aérea.        |

#### **Tabela: AIRPORT**

| Coluna    | Tipo    | Descrição                                |
| --------- | ------- | ---------------------------------------- |
| iata_code | varchar | Código IATA do aeroporto.                |
| airport   | varchar | Nome do aeroporto.                       |
| city      | varchar | Cidade onde o aeroporto está localizado. |
| state     | varchar | Estado onde o aeroporto está localizado. |
| country   | varchar | País onde o aeroporto está localizado.   |
| latitude  | decimal | Latitude geográfica.                     |
| longitude | decimal | Longitude geográfica.                    |

## Histórico de Versões

| Versão | Data       | Descrição                                           | Autor(es)                                        | Revisor(es)                                      |
| ------ | ---------- | --------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------ |
| `1.0`  | 21/09/2025 | Criação inicial do README com dicionário de dados.  | [Júlia Takaki](https://github.com/juliatakaki)   | [Matheus Henrique](https://github.com/mathonaut) |
| `1.1`  | 24/09/2025 | Ajustes no README com explicação sobre camada Gold. | [Pedro Henrique](https://github.com/phmelosilva) | [Matheus Henrique](https://github.com/mathonaut) |
