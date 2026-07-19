import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

from dags.utils.jsonplaceholder_client import fetch_posts, format_posts


def main() -> None:
    posts = fetch_posts()
    print(format_posts(posts))
    print("\nTeste concluído: resposta recebida e validada com sucesso.")


if __name__ == "__main__":
    main()
