import json
import logging
from datetime import timedelta
from urllib.error import HTTPError, URLError

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from utils.jsonplaceholder_client import fetch_posts, format_posts


LOGGER = logging.getLogger(__name__)


@dag(
    dag_id="posts_api_publica",
    description="Consulta posts de uma API pública e apresenta os resultados no log.",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 7, 19, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["api", "jsonplaceholder"],
)
def public_api_posts_dag():
    @task(task_id="consultar_posts")
    def fetch_and_log_posts() -> None:
        try:
            posts = fetch_posts()
        except (HTTPError, URLError, TimeoutError) as error:
            raise AirflowException(
                f"Não foi possível consultar a API pública: {error}"
            ) from error
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise AirflowException(
                f"A API pública retornou uma resposta inesperada: {error}"
            ) from error

        LOGGER.info("\n%s", format_posts(posts))

    fetch_and_log_posts()


dag = public_api_posts_dag()
