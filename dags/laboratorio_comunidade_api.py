import json
import logging
from datetime import timedelta
from urllib.error import HTTPError, URLError

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup

from utils.community_lab import (
    build_ranking,
    calculate_engagement,
    calculate_productivity,
    fetch_resource,
    format_report,
    validate_records,
)


LOGGER = logging.getLogger(__name__)
RESOURCE_LIMITS = {
    "users": 10,
    "posts": 30,
    "comments": 100,
    "todos": 60,
}


@dag(
    dag_id="laboratorio_comunidade_api",
    description=(
        "Pipeline de teste que analisa engajamento e produtividade usando uma API pública."
    ),
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 19, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-lab",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["teste", "api", "jsonplaceholder", "taskflow"],
)
def community_lab_dag():
    @task(task_id="validar_configuracao")
    def validate_configuration() -> dict:
        if len(RESOURCE_LIMITS) < 4 or any(
            limit < 1 for limit in RESOURCE_LIMITS.values()
        ):
            raise AirflowException("A configuração dos recursos é inválida.")

        return {
            "base_url": "https://jsonplaceholder.typicode.com",
            "limits": RESOURCE_LIMITS,
        }

    @task
    def query_resource(resource: str, configuration: dict) -> list[dict]:
        try:
            return fetch_resource(
                resource=resource,
                limit=int(configuration["limits"][resource]),
                base_url=configuration["base_url"],
            )
        except (HTTPError, URLError, TimeoutError) as error:
            raise AirflowException(
                f"Falha ao consultar o recurso {resource}: {error}"
            ) from error
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise AirflowException(
                f"Resposta inesperada para o recurso {resource}: {error}"
            ) from error

    @task
    def validate_resource(resource: str, records: object) -> list[dict]:
        try:
            validated = validate_records(resource, records)
        except (TypeError, ValueError) as error:
            raise AirflowException(
                f"Falha de qualidade no recurso {resource}: {error}"
            ) from error

        LOGGER.info("%s: %d registros aprovados.", resource, len(validated))
        return validated

    @task(task_id="calcular_engajamento")
    def aggregate_engagement(
        users: list[dict],
        posts: list[dict],
        comments: list[dict],
    ) -> list[dict]:
        return calculate_engagement(users, posts, comments)

    @task(task_id="calcular_produtividade")
    def aggregate_productivity(
        users: list[dict],
        todos: list[dict],
    ) -> list[dict]:
        return calculate_productivity(users, todos)

    @task(task_id="selecionar_destaques")
    def select_highlights(
        engagement: list[dict],
        productivity: list[dict],
    ) -> list[dict]:
        return build_ranking(engagement, productivity)

    @task(task_id="gerar_relatorio")
    def generate_report(ranking: list[dict], datasets: dict) -> str:
        counts = {
            resource: len(records) for resource, records in datasets.items()
        }
        return format_report(ranking, counts)

    @task(task_id="publicar_relatorio")
    def publish_report(report: str) -> dict:
        LOGGER.info("\n%s", report)
        return {"published": True, "report_lines": len(report.splitlines())}

    @task(task_id="finalizar_execucao")
    def finish_execution(publication: dict) -> None:
        if not publication.get("published"):
            raise AirflowException("O relatório não foi publicado no log.")
        LOGGER.info(
            "Laboratório concluído com sucesso; relatório com %d linhas.",
            publication["report_lines"],
        )

    configuration = validate_configuration()
    raw_datasets = {}
    validated_datasets = {}

    with TaskGroup(group_id="extracao_api"):
        for resource in RESOURCE_LIMITS:
            raw_datasets[resource] = query_resource.override(
                task_id=f"consultar_{resource}"
            )(resource, configuration)

    with TaskGroup(group_id="qualidade_dados"):
        for resource, records in raw_datasets.items():
            validated_datasets[resource] = validate_resource.override(
                task_id=f"validar_{resource}"
            )(resource, records)

    engagement = aggregate_engagement(
        validated_datasets["users"],
        validated_datasets["posts"],
        validated_datasets["comments"],
    )
    productivity = aggregate_productivity(
        validated_datasets["users"],
        validated_datasets["todos"],
    )
    ranking = select_highlights(engagement, productivity)
    report = generate_report(ranking, validated_datasets)
    publication = publish_report(report)
    finish_execution(publication)


dag = community_lab_dag()
