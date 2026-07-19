import sys
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "dags"))

from utils.community_lab import (
    build_ranking,
    calculate_engagement,
    calculate_productivity,
    format_report,
    validate_records,
)


class CommunityLabTest(unittest.TestCase):
    def setUp(self) -> None:
        self.users = [
            {"id": 1, "name": "Ana", "username": "ana", "email": "a@test.dev"},
            {"id": 2, "name": "Beto", "username": "beto", "email": "b@test.dev"},
        ]
        self.posts = [
            {"id": 10, "userId": 1, "title": "A", "body": "Texto"},
            {"id": 20, "userId": 2, "title": "B", "body": "Texto"},
        ]
        self.comments = [
            {
                "id": 100,
                "postId": 10,
                "name": "C",
                "email": "c@test.dev",
                "body": "Comentário",
            },
            {
                "id": 101,
                "postId": 10,
                "name": "D",
                "email": "d@test.dev",
                "body": "Comentário",
            },
        ]
        self.todos = [
            {"id": 1, "userId": 1, "title": "T1", "completed": True},
            {"id": 2, "userId": 1, "title": "T2", "completed": False},
            {"id": 3, "userId": 2, "title": "T3", "completed": True},
        ]

    def test_builds_ranking_and_report(self) -> None:
        engagement = calculate_engagement(
            self.users, self.posts, self.comments
        )
        productivity = calculate_productivity(self.users, self.todos)
        ranking = build_ranking(engagement, productivity)

        self.assertEqual(ranking[0]["username"], "ana")
        self.assertEqual(ranking[0]["community_score"], 7)

        report = format_report(
            ranking,
            {"users": 2, "posts": 2, "comments": 2, "todos": 3},
        )
        self.assertIn("LABORATÓRIO DE COMUNIDADE", report)
        self.assertIn("Destaque: Ana", report)

    def test_rejects_record_with_missing_field(self) -> None:
        with self.assertRaisesRegex(ValueError, "não possui"):
            validate_records("users", [{"id": 1}])


if __name__ == "__main__":
    unittest.main()
