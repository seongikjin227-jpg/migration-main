"""Tests for the simplified TOBE block RAG flow."""

from pathlib import Path
import shutil
import tempfile
import time
import unittest

from app.features.tobe.tobe_block_rag_flow import analyze_tobe_block_rag
from app.features.tobe.tobe_rule_vector_service import tobe_rule_vector_service


SOURCE_SQL = """
SELECT e.first_name,
       e.last_name,
       dept_locs_v.street_address,
       dept_locs_v.postal_code
FROM employee e,
     (
         SELECT d.department_id,
                d.department_name,
                l.street_address,
                l.postal_code
         FROM departments d,
              locations l
         WHERE d.location_id = l.location_id
     ) dept_locs_v
WHERE dept_locs_v.department_id = e.department_id
  AND e.department_id IN (
      SELECT d2.department_id
      FROM departments d2
      WHERE d2.department_name LIKE 'S%'
  )
  AND e.last_name = 'Smith'
"""


class TobeBlockRagFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = Path(tempfile.mkdtemp())
        self._original_db_path = tobe_rule_vector_service.db_path
        tobe_rule_vector_service.db_path = str(self._temp_dir / "rule_catalog.db")
        tobe_rule_vector_service._ensure_schema()

    def tearDown(self) -> None:
        db_path = Path(tobe_rule_vector_service.db_path)
        tobe_rule_vector_service.db_path = self._original_db_path
        for _ in range(5):
            try:
                if db_path.exists():
                    db_path.unlink()
                break
            except PermissionError:
                time.sleep(0.1)
        shutil.rmtree(self._temp_dir, ignore_errors=True)

    def test_example_sql_splits_into_three_blocks(self) -> None:
        result = analyze_tobe_block_rag(SOURCE_SQL)
        block_ids = [block.block_id for block in result.blocks]
        self.assertEqual(block_ids, ["MAIN", "SUBQUERY_1", "SUBQUERY_2"])

    def test_example_sql_captures_expected_rule_signals(self) -> None:
        result = analyze_tobe_block_rag(SOURCE_SQL)
        by_id = {block.block_id: block for block in result.blocks}
        main_rules = {item.rule_id for item in by_id["MAIN"].top_rule_matches}
        subquery_1_rules = {item.rule_id for item in by_id["SUBQUERY_1"].top_rule_matches}
        subquery_2_rules = {item.rule_id for item in by_id["SUBQUERY_2"].top_rule_matches}
        self.assertIn("RULE_VM001", subquery_1_rules)
        self.assertTrue({"RULE_VM004", "RULE_F005"}.intersection(main_rules | subquery_2_rules))

    def test_rewrite_steps_reference_merge_and_unnest(self) -> None:
        result = analyze_tobe_block_rag(SOURCE_SQL)
        text = " ".join(result.rewrite_steps)
        self.assertIn("inline-view merge candidate", text)
        self.assertIn("IN-subquery unnest candidate", text)

    def test_rule_catalog_vectors_are_stored_in_rule_catalog_db(self) -> None:
        analyze_tobe_block_rag(SOURCE_SQL)
        db_path = Path(tobe_rule_vector_service.db_path)
        self.assertEqual(db_path.name, "rule_catalog.db")
        self.assertTrue(db_path.exists())


if __name__ == "__main__":
    unittest.main()
