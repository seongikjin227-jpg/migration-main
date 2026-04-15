# Complex Mapping Rules (Oracle 19c)

기준 파일: `hr_complex_mapping_rules_seed.sql`

- 총 RULE: 6개 (COMPLEX_JOIN 2, COMPLEX_RESHAPE 2, COMPLEX_TYPE_CAST 2)
- FROM_COL은 alias 이름만 사용
- LOCATIONS 미사용

## RULE 01 - COMPLEX_JOIN (HR_EMP_DEPT_JOB_SNAP)

| FROM_TABLE | FROM_COL | TO_TABLE | TO_COL |
|---|---|---|---|
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |

## RULE 02 - COMPLEX_JOIN (HR_DEPT_PAYROLL_ROLLUP_F)

| FROM_TABLE | FROM_COL | TO_TABLE | TO_COL |
|---|---|---|---|
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |

## RULE 03 - COMPLEX_RESHAPE (HR_EMP_CAREER_EVENT_F)

| FROM_TABLE | FROM_COL | TO_TABLE | TO_COL |
|---|---|---|---|
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |

## RULE 04 - COMPLEX_RESHAPE (HR_EMP_HISTORY_STACKED_F)

| FROM_TABLE | FROM_COL | TO_TABLE | TO_COL |
|---|---|---|---|
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |

## RULE 05 - COMPLEX_TYPE_CAST (HR_EMP_TYPECAST_AUDIT)

| FROM_TABLE | FROM_COL | TO_TABLE | TO_COL |
|---|---|---|---|
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |

## RULE 06 - COMPLEX_TYPE_CAST (HR_DEPT_TYPECAST_AUDIT)

| FROM_TABLE | FROM_COL | TO_TABLE | TO_COL |
|---|---|---|---|
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |
| $fromTable | $c | $toTable | $c |


