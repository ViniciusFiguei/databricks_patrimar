import json
import logging
from datetime import timedelta
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException


API_URL = "https://open.er-api.com/v6/latest/BRL"
MOEDAS = {
    "USD": "Dólar americano",
    "GTQ": "Quetzal da Guatemala",
    "MXN": "Peso mexicano",
    "AUD": "Dólar australiano",
    "ARS": "Peso argentino",
    "COP": "Peso colombiano",
    "PEN": "Sol peruano",
    "CLP": "Peso chileno",
}
LARGURA_LOG = 66
LOGGER = logging.getLogger(__name__)


def buscar_cotacoes() -> dict:
    requisicao = Request(
        API_URL,
        headers={"User-Agent": "cotacoes-moedas-airflow/1.0"},
    )

    with urlopen(requisicao, timeout=10) as resposta:
        return json.load(resposta)


def formatar_cotacoes(dados: dict) -> str:
    taxas = dados["rates"]
    atualizado_em = pendulum.from_timestamp(
        int(dados["time_last_update_unix"]),
        tz="America/Sao_Paulo",
    )
    separador = "=" * LARGURA_LOG
    separador_secao = "-" * LARGURA_LOG
    linhas = [
        separador,
        "COTAÇÕES DE MOEDAS EM REAIS (BRL)".center(LARGURA_LOG),
        separador,
        f"{'MOEDA':<30} {'CÓDIGO':^10} {'VALOR':>24}",
        separador_secao,
    ]

    for codigo, nome in MOEDAS.items():
        valor = 1 / float(taxas[codigo])
        valor_formatado = f"R$ {valor:.4f}"
        linhas.append(f"{nome:<30} {codigo:^10} {valor_formatado:>24}")

    linhas.extend(
        [
            separador_secao,
            f"Atualização: {atualizado_em.format('DD/MM/YYYY HH:mm:ss')}",
            "Fonte: https://www.exchangerate-api.com",
            separador,
        ]
    )
    return "\n".join(linhas)


@dag(
    dag_id="cotacoes_moedas",
    description="Consulta cotações de moedas em reais.",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 7, 17, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["cotacoes", "moedas"],
)
def cotacoes_moedas():
    @task(task_id="consultar_cotacoes")
    def consultar_cotacoes() -> None:
        try:
            dados = buscar_cotacoes()
            if dados.get("result") != "success":
                raise ValueError(f"Resultado da API: {dados.get('result')}")

            LOGGER.info("\n%s", formatar_cotacoes(dados))
        except (HTTPError, URLError, TimeoutError) as erro:
            raise AirflowException(
                f"Não foi possível consultar as cotações: {erro}"
            ) from erro
        except (
            KeyError,
            TypeError,
            ValueError,
            ZeroDivisionError,
            json.JSONDecodeError,
        ) as erro:
            raise AirflowException(
                f"A API retornou uma resposta inesperada: {erro}"
            ) from erro

    consultar_cotacoes()


dag = cotacoes_moedas()
