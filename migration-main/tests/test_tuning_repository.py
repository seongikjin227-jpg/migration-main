"""Tests for tuning repository defensive logging behavior."""

import json
import unittest
from unittest.mock import patch

from app.features.sql_tuning.tuning_repository import persist_tuning_log


class TuningRepositoryTests(unittest.TestCase):
    def test_persist_tuning_log_handles_none_retrieved_cases(self) -> None:
        with patch("app.features.sql_tuning.tuning_repository.insert_tuning_log") as mock_insert:
            persist_tuning_log(
                execution_id="exec-1",
                row_id="row-1",
                space_nm="view.merging",
                sql_id="TuningTest1",
                tag_kind="SELECT",
                tuning_status="TUNING_FAILED",
                job_status="FAIL",
                final_stage="PERSIST_FAILURE",
                retry_count=4,
                llm_used_yn="N",
                applied_rule_ids=[],
                diff_summary=None,
                error_message="boom",
                tobe_sql="SELECT 1 FROM DUAL",
                tobe_rag_debug={
                    "query_case": {"source_sql_raw": "SELECT 1 FROM DUAL"},
                    "retrieved_rule_ids": ["RULE_VM001"],
                    "retrieved_cases": None,
                },
            )

        kwargs = mock_insert.call_args.kwargs
        self.assertEqual(json.loads(kwargs["retrieved_rule_ids_json"]), ["RULE_VM001"])
        self.assertEqual(json.loads(kwargs["retrieved_case_ids_json"]), [])


if __name__ == "__main__":
    unittest.main()
