# Dicionário de Dados – Silver

## **Entidade: silver_flights**

| Atributo                 | Tipo             | Descrição                                            |
| ------------------------ | ---------------- | ---------------------------------------------------- |
| flight_id                | integer          | Identificador único do voo (chave primária).         |
| flight_year              | smallint         | Ano do voo.                                          |
| flight_month             | smallint         | Mês do voo.                                          |
| flight_day               | smallint         | Dia do mês.                                          |
| flight_day_of_week       | smallint         | Dia da semana (1=Segunda, 7=Domingo).                |
| flight_date              | date             | Data completa do voo.                                |
| flight_number            | integer          | Número do voo.                                       |
| airline_iata_code        | varchar(3)       | Código IATA da companhia aérea.                      |
| airline_name             | varchar(100)     | Nome da companhia aérea.                             |
| tail_number              | varchar(10)      | Identificação da aeronave.                           |
| origin_airport_iata_code | varchar(3)       | Código IATA do aeroporto de origem.                  |
| origin_airport_name      | varchar(100)     | Nome do aeroporto de origem.                         |
| origin_city              | varchar(50)      | Cidade do aeroporto de origem.                       |
| origin_state             | varchar(3)       | Estado do aeroporto de origem.                       |
| origin_latitude          | double precision | Latitude do aeroporto de origem.                     |
| origin_longitude         | double precision | Longitude do aeroporto de origem.                    |
| dest_airport_iata_code   | varchar(3)       | Código IATA do aeroporto de destino.                 |
| dest_airport_name        | varchar(100)     | Nome do aeroporto de destino.                        |
| dest_city                | varchar(50)      | Cidade do aeroporto de destino.                      |
| dest_state               | varchar(3)       | Estado do aeroporto de destino.                      |
| dest_latitude            | double precision | Latitude do aeroporto de destino.                    |
| dest_longitude           | double precision | Longitude do aeroporto de destino.                   |
| scheduled_departure      | timestamp        | Horário programado de partida.                       |
| departure_time           | timestamp        | Horário real de partida.                             |
| schedule_arrival         | timestamp        | Horário programado de chegada.                       |
| arrival_time             | timestamp        | Horário real de chegada.                             |
| wheels_off               | timestamp        | Horário que a aeronave saiu do solo.                 |
| whl_on                   | timestamp        | Horário que a aeronave tocou o solo.                 |
| taxi_out                 | double precision | Tempo gasto em solo antes da decolagem.              |
| taxi_in                  | double precision | Tempo gasto em solo após o pouso.                    |
| air_time                 | double precision | Tempo em voo.                                        |
| elapsed_time             | double precision | Tempo total do voo.                                  |
| scheduled_time           | double precision | Tempo de voo programado.                             |
| distance                 | double precision | Distência do voo.                                    |
| departure_delay          | double precision | Atraso na decolagem .                                |
| arrival_delay            | double precision | Atraso na chegada.                                   |
| air_system_delay         | double precision | Atraso devido ao sistema aéreo .                     |
| security_delay           | double precision | Atraso devido a questões de segurança .              |
| airline_delay            | double precision | Atraso devido à companhia aérea .                    |
| late_aircraft_delay      | double precision | Atraso causado por chegada tardia de outra aeronave. |
| weather_delay            | double precision | Atraso devido ao clima.                              |
| is_overnight_flight      | boolean          | Indica se um voo atravessou o dia.                   |
