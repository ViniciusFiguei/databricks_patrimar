import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_URL = "https://jsonplaceholder.typicode.com/posts"
DEFAULT_POST_LIMIT = 5


def fetch_posts(limit: int = DEFAULT_POST_LIMIT) -> list[dict]:
    if limit < 1:
        raise ValueError("O limite de posts deve ser maior que zero.")

    query_parameters = urlencode({"_limit": limit})
    request = Request(
        f"{API_URL}?{query_parameters}",
        headers={"User-Agent": "airflow-public-api/1.0"},
    )

    with urlopen(request, timeout=10) as response:
        posts = json.load(response)

    validate_posts(posts)
    return posts


def validate_posts(posts: object) -> None:
    if not isinstance(posts, list):
        raise ValueError("A API não retornou uma lista de posts.")

    required_fields = {"id", "userId", "title", "body"}
    for index, post in enumerate(posts, start=1):
        if not isinstance(post, dict):
            raise ValueError(f"O post {index} não é um objeto válido.")

        missing_fields = required_fields.difference(post)
        if missing_fields:
            fields = ", ".join(sorted(missing_fields))
            raise ValueError(f"O post {index} não possui os campos: {fields}.")


def format_posts(posts: list[dict]) -> str:
    separator = "=" * 70
    lines = [
        separator,
        "POSTS DA API JSONPLACEHOLDER".center(70),
        separator,
    ]

    for post in posts:
        lines.extend(
            [
                f"Post: {post['id']} | Usuário: {post['userId']}",
                f"Título: {post['title']}",
                f"Conteúdo: {post['body'].replace(chr(10), ' ')}",
                "-" * 70,
            ]
        )

    lines.append(f"Total de posts: {len(posts)}")
    lines.append(separator)
    return "\n".join(lines)
