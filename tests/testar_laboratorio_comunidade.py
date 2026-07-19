import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "dags"))

from utils.community_lab import (
    DEFAULT_API_URL,
    build_ranking,
    calculate_engagement,
    calculate_productivity,
    fetch_resource,
    format_report,
    validate_records,
)


RESOURCE_LIMITS = {
    "users": 10,
    "posts": 30,
    "comments": 100,
    "todos": 60,
}


def main() -> None:
    datasets = {}
    for resource, limit in RESOURCE_LIMITS.items():
        records = fetch_resource(resource, limit, DEFAULT_API_URL)
        datasets[resource] = validate_records(resource, records)
        print(f"{resource}: {len(records)} registros recebidos e validados")

    engagement = calculate_engagement(
        datasets["users"],
        datasets["posts"],
        datasets["comments"],
    )
    productivity = calculate_productivity(
        datasets["users"],
        datasets["todos"],
    )
    ranking = build_ranking(engagement, productivity)
    counts = {
        resource: len(records) for resource, records in datasets.items()
    }

    print()
    print(format_report(ranking, counts))
    print("\nTeste concluído: API, validações e cálculos executados com sucesso.")


if __name__ == "__main__":
    main()
