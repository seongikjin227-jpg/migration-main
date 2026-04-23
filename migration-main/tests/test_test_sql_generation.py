"""Tests for deterministic TEST SQL generation."""

from types import SimpleNamespace
import unittest

from app.services.llm_service import generate_test_sql_no_bind
from app.services.validation_service import _prepare_runtime_sql


class TestSqlGenerationTests(unittest.TestCase):
    def test_generate_test_sql_no_bind_strips_trailing_semicolons_from_nested_queries(self) -> None:
        job = SimpleNamespace(source_sql="SELECT empno FROM emp;")

        test_sql = generate_test_sql_no_bind(
            job=job,
            tobe_sql="SELECT empno FROM emp;",
        )

        self.assertNotIn("(SELECT empno FROM emp;) f", test_sql)
        self.assertNotIn("(SELECT empno FROM emp;) t", test_sql)
        self.assertIn("(SELECT empno FROM emp) f", test_sql)
        self.assertIn("(SELECT empno FROM emp) t", test_sql)
        self.assertEqual(_prepare_runtime_sql(test_sql, stage="EXECUTE_TEST_SQL"), test_sql)

    def test_generate_test_sql_no_bind_removes_sqlplus_slash_terminator_lines(self) -> None:
        job = SimpleNamespace(source_sql="SELECT empno FROM emp\n/\n")

        test_sql = generate_test_sql_no_bind(
            job=job,
            tobe_sql="SELECT empno FROM emp\n/\n",
        )

        self.assertNotIn("/)", test_sql)
        self.assertIn("(SELECT empno FROM emp) f", test_sql)
        self.assertIn("(SELECT empno FROM emp) t", test_sql)


if __name__ == "__main__":
    unittest.main()
