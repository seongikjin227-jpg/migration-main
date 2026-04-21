"""Lightweight unit tests for tuning pipeline utilities."""

import unittest

from app.features.sql_tuning.rule_detector import detect_tuning_rules
from app.features.sql_tuning.sql_normalizer import normalize_sql_for_tuning


class TuningPipelineUtilityTests(unittest.TestCase):
    def test_normalizer_collapses_extra_whitespace(self) -> None:
        normalized_sql, notes = normalize_sql_for_tuning("SELECT   *  FROM  EMPLOYEES ; ")
        self.assertEqual(normalized_sql, "SELECT * FROM EMPLOYEES")
        self.assertIn("collapsed_whitespace", notes)

    def test_rule_detector_finds_pagination_without_order_by(self) -> None:
        detected = detect_tuning_rules("SELECT * FROM EMPLOYEES FETCH FIRST 10 ROWS ONLY")
        self.assertTrue(any(item.rule.rule_id == "RULE_P001" for item in detected))

if __name__ == "__main__":
    unittest.main()
