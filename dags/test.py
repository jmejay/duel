from logging import getLogger
import datetime 

from airflow.decorators import dag, task

logger = getLogger('airflow.task')

@dag(
    dag_id="jamie_test",
    schedule="@daily",
    catchup=False,
    start_date=datetime.datetime(2025, 5, 1)
)

def test_me():
    @task
    def print_log():
        logger.info("help!")

    (print_log())

test_me()