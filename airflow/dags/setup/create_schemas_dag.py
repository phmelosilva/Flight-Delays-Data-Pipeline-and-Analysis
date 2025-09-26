from airflow.decorators import dag, task
from pendulum import datetime
from pipelines.setup.create_schemas import create_schemas


@dag(
    dag_id='create_schemas_dag',
    start_date=datetime(2025, 1, 1),
    schedule='@once',
    catchup=False,
    tags=['setup', 'database', 'schemas']
)
def create_schemas_dag():

    @task
    def run_schemas_creation():
        create_schemas()

    run_schemas_creation()

create_schemas_dag()
