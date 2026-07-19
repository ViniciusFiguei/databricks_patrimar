import json
from collections import defaultdict
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_API_URL = "https://jsonplaceholder.typicode.com"
REQUIRED_FIELDS = {
    "users": {"id", "name", "username", "email"},
    "posts": {"id", "userId", "title", "body"},
    "comments": {"id", "postId", "name", "email", "body"},
    "todos": {"id", "userId", "title", "completed"},
}


def fetch_resource(
    resource: str,
    limit: int,
    base_url: str = DEFAULT_API_URL,
) -> list[dict]:
    if resource not in REQUIRED_FIELDS:
        raise ValueError(f"Recurso não suportado: {resource}.")
    if limit < 1:
        raise ValueError("O limite da consulta deve ser maior que zero.")

    query = urlencode({"_limit": limit})
    request = Request(
        f"{base_url.rstrip('/')}/{resource}?{query}",
        headers={"User-Agent": "airflow-community-lab/1.0"},
    )

    with urlopen(request, timeout=15) as response:
        payload = json.load(response)

    if not isinstance(payload, list):
        raise ValueError(f"A API não retornou uma lista para {resource}.")
    return payload


def validate_records(resource: str, records: object) -> list[dict]:
    if resource not in REQUIRED_FIELDS:
        raise ValueError(f"Recurso não suportado: {resource}.")
    if not isinstance(records, list) or not records:
        raise ValueError(f"O recurso {resource} está vazio ou inválido.")

    required_fields = REQUIRED_FIELDS[resource]
    validated_records = []
    for index, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ValueError(f"O registro {index} de {resource} não é um objeto.")

        missing_fields = required_fields.difference(record)
        if missing_fields:
            fields = ", ".join(sorted(missing_fields))
            raise ValueError(
                f"O registro {index} de {resource} não possui: {fields}."
            )
        validated_records.append(record)

    return validated_records


def calculate_engagement(
    users: list[dict],
    posts: list[dict],
    comments: list[dict],
) -> list[dict]:
    posts_by_user: dict[int, int] = defaultdict(int)
    comments_by_user: dict[int, int] = defaultdict(int)
    post_owners = {}

    for post in posts:
        user_id = int(post["userId"])
        post_id = int(post["id"])
        posts_by_user[user_id] += 1
        post_owners[post_id] = user_id

    for comment in comments:
        user_id = post_owners.get(int(comment["postId"]))
        if user_id is not None:
            comments_by_user[user_id] += 1

    metrics = []
    for user in users:
        user_id = int(user["id"])
        post_count = posts_by_user[user_id]
        comment_count = comments_by_user[user_id]
        metrics.append(
            {
                "user_id": user_id,
                "name": user["name"],
                "username": user["username"],
                "posts": post_count,
                "comments_received": comment_count,
                "engagement_score": (post_count * 3) + comment_count,
            }
        )

    return sorted(
        metrics,
        key=lambda item: (-item["engagement_score"], item["user_id"]),
    )


def calculate_productivity(users: list[dict], todos: list[dict]) -> list[dict]:
    totals: dict[int, int] = defaultdict(int)
    completed: dict[int, int] = defaultdict(int)

    for todo in todos:
        user_id = int(todo["userId"])
        totals[user_id] += 1
        completed[user_id] += int(bool(todo["completed"]))

    metrics = []
    for user in users:
        user_id = int(user["id"])
        total = totals[user_id]
        done = completed[user_id]
        completion_rate = round((done / total) * 100, 1) if total else 0.0
        metrics.append(
            {
                "user_id": user_id,
                "todos": total,
                "completed_todos": done,
                "completion_rate": completion_rate,
            }
        )

    return sorted(
        metrics,
        key=lambda item: (-item["completion_rate"], item["user_id"]),
    )


def build_ranking(
    engagement: list[dict],
    productivity: list[dict],
) -> list[dict]:
    productivity_by_user = {
        item["user_id"]: item for item in productivity
    }
    ranking = []

    for engagement_item in engagement:
        productivity_item = productivity_by_user.get(
            engagement_item["user_id"],
            {
                "todos": 0,
                "completed_todos": 0,
                "completion_rate": 0.0,
            },
        )
        item = {**engagement_item, **productivity_item}
        item["community_score"] = (
            item["engagement_score"] + (item["completed_todos"] * 2)
        )
        ranking.append(item)

    return sorted(
        ranking,
        key=lambda item: (-item["community_score"], item["user_id"]),
    )


def format_report(ranking: list[dict], resource_counts: dict[str, int]) -> str:
    if not ranking:
        raise ValueError("Não há dados suficientes para gerar o relatório.")

    separator = "=" * 78
    lines = [
        separator,
        "LABORATÓRIO DE COMUNIDADE - JSONPLACEHOLDER".center(78),
        separator,
        (
            "Amostra: "
            f"{resource_counts['users']} usuários | "
            f"{resource_counts['posts']} posts | "
            f"{resource_counts['comments']} comentários | "
            f"{resource_counts['todos']} tarefas"
        ),
        "-" * 78,
        f"{'#':<3} {'USUÁRIO':<24} {'POSTS':>6} {'COMENT.':>8} "
        f"{'CONCLUÍDAS':>11} {'SCORE':>7}",
        "-" * 78,
    ]

    for position, item in enumerate(ranking[:5], start=1):
        lines.append(
            f"{position:<3} {item['username']:<24} {item['posts']:>6} "
            f"{item['comments_received']:>8} {item['completed_todos']:>11} "
            f"{item['community_score']:>7}"
        )

    champion = ranking[0]
    lines.extend(
        [
            "-" * 78,
            (
                f"Destaque: {champion['name']} (@{champion['username']}) "
                f"com {champion['community_score']} pontos."
            ),
            separator,
        ]
    )
    return "\n".join(lines)
