from airflow.decorators import dag, task
from pendulum import datetime, duration
from pipelines.utils.create_schemas import create_schemas


@dag(
    dag_id='create_schemas_dag',
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule='@once',
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": duration(seconds=30),
    },
    tags=['setup', 'database', 'schemas'],
    doc_md="""
    ###
    ---------------------------------[ INFO: ]---------------------------------

        DAG de Criação de Schemas

        Responsável por criar os schemas necessários no banco de dados antes do 
        início dos pipelines de ingestão.  
        Esta DAG deve ser executada apenas uma vez durante o setup inicial.

    ---------------------------------------------------------------------------
    """,
)
def create_schemas_dag():
    """
    DAG responsável pela criação inicial dos schemas no banco de dados.
    """

    @task
    def create_schemas_task():
        create_schemas()

    create_schemas_task()

create_schemas_dag()
