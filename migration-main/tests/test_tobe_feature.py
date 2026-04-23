"""Tests for TOBE feature soft-validation return contracts."""

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from app.features.tobe.tobe_feature import generate_tobe_sql_with_soft_validation


class TobeFeatureTests(unittest.TestCase):
    def test_soft_validation_success_still_returns_block_rag_context(self) -> None:
        job = SimpleNamespace(space_nm="view.merging", sql_id="TuningTest1", source_sql="SELECT 1 FROM DUAL")

        with (
            patch("app.features.tobe.tobe_feature.build_tobe_block_rag_context", return_value='{"flow_kind":"TEST"}'),
            patch("app.features.tobe.tobe_feature.generate_tobe_sql", return_value="SELECT 1 FROM DUAL"),
            patch("app.features.tobe.tobe_feature.collect_tobe_sql_column_coverage_issues", return_value=[]),
        ):
            sql, block_rag_context, warning_message = generate_tobe_sql_with_soft_validation(
                job=job,
                mapping_rules=[],
            )

        self.assertEqual(sql, "SELECT 1 FROM DUAL")
        self.assertEqual(block_rag_context, '{"flow_kind":"TEST"}')
        self.assertIsNone(warning_message)


if __name__ == "__main__":
    unittest.main()
