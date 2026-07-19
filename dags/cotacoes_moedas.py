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
NEWS_URL = "https://news.google.com/rss/search"
NEWS_QUERY = "dólar mercado financeiro"
NEWS_LIMIT = 2
CURRENCIES = {
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
LOG_WIDTH = 66
LOGGER = logging.getLogger(__name__)


def fetch_exchange_rates() -> dict:
    request = Request(
        API_URL,
        headers={"User-Agent": "cotacoes-moedas-airflow/1.0"},
    )

    with urlopen(request, timeout=10) as response:
        return json.load(response)


def fetch_dollar_news() -> list[dict[str, str]]:
    query_parameters = urlencode(
        {
            "q": NEWS_QUERY,
            "hl": "pt-BR",
            "gl": "BR",
            "ceid": "BR:pt-419",
        }
    )
    request = Request(
        f"{NEWS_URL}?{query_parameters}",
        headers={"User-Agent": "cotacoes-moedas-airflow/1.0"},
    )

    with urlopen(request, timeout=10) as response:
        root = ElementTree.parse(response).getroot()

    news_items = []
    for item in root.findall("./channel/item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        source = (item.findtext("source") or "Fonte não informada").strip()
        publication_date = (item.findtext("pubDate") or "").strip()

        if not title or not link:
            continue

        source_suffix = f" - {source}"
        if title.endswith(source_suffix):
            title = title[: -len(source_suffix)]

        published_at = "Horário não informado"
        if publication_date:
            try:
                published_at = pendulum.instance(
                    parsedate_to_datetime(publication_date)
                ).in_timezone("America/Sao_Paulo").format("DD/MM/YYYY HH:mm")
            except (TypeError, ValueError, OverflowError):
                pass

        news_items.append(
            {
                "title": title,
                "source": source,
                "published_at": published_at,
                "link": link,
            }
        )
        if len(news_items) == NEWS_LIMIT:
            break

    return news_items


def format_exchange_rates(data: dict) -> str:
    rates = data["rates"]
    updated_at = pendulum.from_timestamp(
        int(data["time_last_update_unix"]),
        tz="America/Sao_Paulo",
    )
    separator = "=" * LOG_WIDTH
    section_separator = "-" * LOG_WIDTH
    lines = [
        separator,
        "COTAÇÕES DE MOEDAS EM REAIS (BRL)".center(LOG_WIDTH),
        separator,
        f"{'MOEDA':<30} {'CÓDIGO':^10} {'VALOR':>24}",
        section_separator,
    ]

    for currency_code, currency_name in CURRENCIES.items():
        value_in_brl = 1 / float(rates[currency_code])
        formatted_value = f"R$ {value_in_brl:.4f}"
        lines.append(
            f"{currency_name:<30} {currency_code:^10} {formatted_value:>24}"
        )

    lines.extend(
        [
            section_separator,
            f"Atualização: {updated_at.format('DD/MM/YYYY HH:mm:ss')}",
            "Fonte: https://www.exchangerate-api.com",
            separator,
        ]
    )
    return "\n".join(lines)


def format_news(news_items: list[dict[str, str]]) -> str:
    separator = "=" * LOG_WIDTH
    section_separator = "-" * LOG_WIDTH
    lines = [
        separator,
        "NOTÍCIAS RECENTES SOBRE O DÓLAR".center(LOG_WIDTH),
        separator,
    ]

    if not news_items:
        lines.append("Nenhuma notícia disponível no momento.")
    else:
        for index, news_item in enumerate(news_items, start=1):
            if index > 1:
                lines.append(section_separator)
            lines.extend(
                [
                    f"{index}. {news_item['title']}",
                    (
                        f"Fonte: {news_item['source']} | "
                        f"Publicação: {news_item['published_at']}"
                    ),
                    f"Link: {news_item['link']}",
                ]
            )

    lines.append(separator)
    return "\n".join(lines)


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
def currency_quotes_dag():
    @task(task_id="consultar_cotacoes")
    def fetch_and_log_exchange_rates() -> None:
        try:
            exchange_rate_data = fetch_exchange_rates()
            if exchange_rate_data.get("result") != "success":
                raise ValueError(
                    f"Resultado da API: {exchange_rate_data.get('result')}"
                )
        except (HTTPError, URLError, TimeoutError) as error:
            raise AirflowException(
                f"Não foi possível consultar as cotações: {error}"
            ) from error
        except (
            KeyError,
            TypeError,
            ValueError,
            ZeroDivisionError,
            json.JSONDecodeError,
        ) as error:
            raise AirflowException(
                f"A API retornou uma resposta inesperada: {error}"
            ) from error

        try:
            news_items = fetch_dollar_news()
        except (
            HTTPError,
            URLError,
            TimeoutError,
            ElementTree.ParseError,
        ) as error:
            LOGGER.warning("Não foi possível consultar as notícias: %s", error)
            news_items = []

        LOGGER.info(
            "\n%s\n%s",
            format_exchange_rates(exchange_rate_data),
            format_news(news_items),
        )

    fetch_and_log_exchange_rates()


dag = currency_quotes_dag()
