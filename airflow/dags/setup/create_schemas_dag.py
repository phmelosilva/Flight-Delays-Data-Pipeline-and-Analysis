from airflow.decorators import dag, task
from pendulum import datetime, duration
from pipelines.setup.create_schemas import create_schemas


@dag(
    dag_id='create_schemas_dag',
    start_date=datetime(2025, 1, 1),
    schedule='@once',
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": duration(minutes=1),
    },
    tags=['setup', 'database', 'schemas']
)
def create_schemas_dag():

    @task
    def create_schemas_task():
        create_schemas()

    create_schemas_task()

create_schemas_dag()
