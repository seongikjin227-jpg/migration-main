# HR Complex Mapping Rules

기준 버전: Oracle Database 19c

## Files

- `hr_complex_mapping_rules_seed.sql`
  - 소스(From) SQL 정의
  - RULE 6개
- `hr_complex_mapping_rules_table.sql`
  - 매핑 메타데이터 (`FROM_TABLE`, `FROM_COL`, `TO_TABLE`, `TO_COL`)
  - RULE 6개
- `HrComplexMapper.xml`
  - Mapper SQL (`select id` 6개)
  - RULE 6개
- `hr_complex_mapping_rules.md`
  - 위 SQL과 동일한 매핑 요약 문서

## Rule List (6)

1. RULE 01 - COMPLEX_JOIN - `HR_EMP_DEPT_JOB_SNAP`
2. RULE 02 - COMPLEX_JOIN - `HR_DEPT_PAYROLL_ROLLUP_F`
3. RULE 03 - COMPLEX_RESHAPE - `HR_EMP_CAREER_EVENT_F`
4. RULE 04 - COMPLEX_RESHAPE - `HR_EMP_HISTORY_STACKED_F`
5. RULE 05 - COMPLEX_TYPE_CAST - `HR_EMP_TYPECAST_AUDIT`
6. RULE 06 - COMPLEX_TYPE_CAST - `HR_DEPT_TYPECAST_AUDIT`

## Notes

- `LOCATIONS` 미사용
- `FROM_COL`은 alias 컬럼명만 사용
- 문법은 Oracle 19c 기준으로 작성
