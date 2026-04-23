"""Tests for mapping-rule selection robustness."""

from types import SimpleNamespace
import unittest

from app.common import MappingRuleItem
from app.services.llm_service import select_mapping_rules_for_job


class MappingRuleSelectionTests(unittest.TestCase):
    def test_select_mapping_rules_handles_malformed_target_table_and_unions_source_sql_matches(self) -> None:
        job = SimpleNamespace(
            target_table='["DEPARTMENTS","LOCATION",\'EMPLOYEES"]',
            source_sql="""
                SELECT e.first_name, dept_locs_v.street_address
                FROM HR.EMPLOYEES e,
                     (SELECT d.department_id, l.street_address
                        FROM HR.DEPARTMENTS d, HR.LOCATIONS l
                       WHERE d.location_id = l.location_id) dept_locs_v
                WHERE dept_locs_v.department_id = e.department_id
            """,
        )
        mapping_rules = [
            MappingRuleItem(map_type="", fr_table="HR.EMPLOYEES", fr_col="FIRST_NAME", to_table="TGT_EMP", to_col="FIRST_NAME", map_id="1"),
            MappingRuleItem(map_type="", fr_table="HR.DEPARTMENTS", fr_col="DEPARTMENT_ID", to_table="TGT_DEP", to_col="DEPARTMENT_ID", map_id="2"),
            MappingRuleItem(map_type="", fr_table="HR.LOCATION", fr_col="STREET_ADDRESS", to_table="TGT_LOC", to_col="NEW_STREET_ADDRESS", map_id="4"),
        ]

        selected = select_mapping_rules_for_job(job=job, mapping_rules=mapping_rules, fallback_to_all=False)
        selected_fr_tables = {rule.fr_table for rule in selected}

        self.assertEqual(selected_fr_tables, {"HR.EMPLOYEES", "HR.DEPARTMENTS", "HR.LOCATION"})


if __name__ == "__main__":
    unittest.main()
