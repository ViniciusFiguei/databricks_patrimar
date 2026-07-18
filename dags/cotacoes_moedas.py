import json
import logging
from datetime import timedelta
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException


API_URL = "https://open.er-api.com/v6/latest/BRL"
NOTICIAS_URL = "https://news.google.com/rss/search"
CONSULTA_NOTICIAS = "dólar mercado financeiro"
LIMITE_NOTICIAS = 2
MOEDAS = {
    "USD": "Dólar americano",
    "GTQ": "Quetzal da Guatemala",
    "MXN": "Peso mexicano",
    "AUD": "Dólar australiano",
    "ARS": "Peso argentino",
    "COP": "Peso colombiano",
    "PEN": "Sol peruano",
    "CLP": "Peso chileno",
    "AED": "Dirham dos Emirados Árabes",
    "THB": "Baht tailandês",
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


def buscar_noticias_dolar() -> list[dict[str, str]]:
    parametros = urlencode(
        {
            "q": CONSULTA_NOTICIAS,
            "hl": "pt-BR",
            "gl": "BR",
            "ceid": "BR:pt-419",
        }
    )
    requisicao = Request(
        f"{NOTICIAS_URL}?{parametros}",
        headers={"User-Agent": "cotacoes-moedas-airflow/1.0"},
    )

    with urlopen(requisicao, timeout=10) as resposta:
        raiz = ElementTree.parse(resposta).getroot()

    noticias = []
    for item in raiz.findall("./channel/item"):
        titulo = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        fonte = (item.findtext("source") or "Fonte não informada").strip()
        data_publicacao = (item.findtext("pubDate") or "").strip()

        if not titulo or not link:
            continue

        sufixo_fonte = f" - {fonte}"
        if titulo.endswith(sufixo_fonte):
            titulo = titulo[: -len(sufixo_fonte)]

        publicado_em = "Horário não informado"
        if data_publicacao:
            try:
                publicado_em = pendulum.instance(
                    parsedate_to_datetime(data_publicacao)
                ).in_timezone("America/Sao_Paulo").format("DD/MM/YYYY HH:mm")
            except (TypeError, ValueError, OverflowError):
                pass

        noticias.append(
            {
                "titulo": titulo,
                "fonte": fonte,
                "publicado_em": publicado_em,
                "link": link,
            }
        )
        if len(noticias) == LIMITE_NOTICIAS:
            break

    return noticias


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


def formatar_noticias(noticias: list[dict[str, str]]) -> str:
    separador = "=" * LARGURA_LOG
    separador_secao = "-" * LARGURA_LOG
    linhas = [
        separador,
        "NOTÍCIAS RECENTES SOBRE O DÓLAR".center(LARGURA_LOG),
        separador,
    ]

    if not noticias:
        linhas.append("Nenhuma notícia disponível no momento.")
    else:
        for indice, noticia in enumerate(noticias, start=1):
            if indice > 1:
                linhas.append(separador_secao)
            linhas.extend(
                [
                    f"{indice}. {noticia['titulo']}",
                    (
                        f"Fonte: {noticia['fonte']} | "
                        f"Publicação: {noticia['publicado_em']}"
                    ),
                    f"Link: {noticia['link']}",
                ]
            )

    linhas.append(separador)
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

        try:
            noticias = buscar_noticias_dolar()
        except (
            HTTPError,
            URLError,
            TimeoutError,
            ElementTree.ParseError,
        ) as erro:
            LOGGER.warning("Não foi possível consultar as notícias: %s", erro)
            noticias = []

        LOGGER.info(
            "\n%s\n%s",
            formatar_cotacoes(dados),
            formatar_noticias(noticias),
        )

    consultar_cotacoes()


dag = cotacoes_moedas()
