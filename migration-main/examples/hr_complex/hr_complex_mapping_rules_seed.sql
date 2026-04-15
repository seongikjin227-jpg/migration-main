-- =============================================================
-- hr_complex_mapping_rules_seed.sql
-- 목표:
-- 1) 룰 개수는 총 6개로 유지
-- 2) 각 룰은 충분히 복합적(CTE, CASE, WINDOW, UNION ALL, 형변환)
-- 3) LOCATIONS 테이블은 사용하지 않음
-- 4) CREATE/MERGE 없이 "소스 SQL 정의"에 집중
-- =============================================================

-- -----------------------------------------------------------------
-- RULE 01 : COMPLEX_JOIN
-- TO_TABLE : HR_EMP_DEPT_JOB_SNAP
-- 설명:
-- - 직원 + 부서 + 직무 조인
-- - 급여 밴드, 직무 레벨, 부서 내 급여 순위, 매니저 존재 여부 파생
-- -----------------------------------------------------------------
WITH
emp_base AS (
    SELECT
        e.EMPLOYEE_ID,
        e.FIRST_NAME,
        e.LAST_NAME,
        e.EMAIL,
        e.PHONE_NUMBER,
        e.HIRE_DATE,
        e.JOB_ID,
        e.SALARY,
        e.COMMISSION_PCT,
        e.MANAGER_ID,
        e.DEPARTMENT_ID
    FROM HR.EMPLOYEES e
),
dept_base AS (
    SELECT
        d.DEPARTMENT_ID,
        d.DEPARTMENT_NAME,
        d.MANAGER_ID AS DEPT_MANAGER_ID
    FROM HR.DEPARTMENTS d
),
job_base AS (
    SELECT
        j.JOB_ID,
        j.JOB_TITLE,
        j.MIN_SALARY,
        j.MAX_SALARY
    FROM HR.JOBS j
),
joined AS (
    SELECT
        eb.EMPLOYEE_ID                                  AS EMPLOYEE_ID,
        eb.FIRST_NAME                                   AS FIRST_NAME,
        eb.LAST_NAME                                    AS LAST_NAME,
        eb.EMAIL                                        AS EMAIL,
        eb.PHONE_NUMBER                                 AS PHONE_NUMBER,
        eb.HIRE_DATE                                    AS HIRE_DATE,
        eb.DEPARTMENT_ID                                AS DEPARTMENT_ID,
        db.DEPARTMENT_NAME                              AS DEPARTMENT_NAME,
        eb.JOB_ID                                       AS JOB_ID,
        jb.JOB_TITLE                                    AS JOB_TITLE,
        eb.SALARY                                       AS CURRENT_SALARY,
        eb.COMMISSION_PCT                               AS COMMISSION_PCT,
        eb.MANAGER_ID                                   AS MANAGER_ID,
        db.DEPT_MANAGER_ID                              AS DEPT_MANAGER_ID,
        CASE
            WHEN eb.SALARY >= 20000 THEN 'EXEC'
            WHEN eb.SALARY >= 12000 THEN 'HIGH'
            WHEN eb.SALARY >= 7000 THEN 'MID'
            ELSE 'LOW'
        END                                             AS SALARY_BAND,
        CASE
            WHEN jb.MAX_SALARY IS NULL THEN 'NO_JOB_RANGE'
            WHEN eb.SALARY > jb.MAX_SALARY THEN 'ABOVE_JOB_MAX'
            WHEN eb.SALARY < jb.MIN_SALARY THEN 'BELOW_JOB_MIN'
            ELSE 'IN_JOB_RANGE'
        END                                             AS JOB_RANGE_STATUS,
        CASE
            WHEN eb.MANAGER_ID IS NULL THEN 'NO_MANAGER'
            ELSE 'HAS_MANAGER'
        END                                             AS MANAGER_EXISTS_YN,
        ROW_NUMBER() OVER (
            PARTITION BY eb.DEPARTMENT_ID
            ORDER BY eb.SALARY DESC, eb.EMPLOYEE_ID
        )                                               AS DEPT_SALARY_RANK,
        DENSE_RANK() OVER (
            PARTITION BY eb.JOB_ID
            ORDER BY eb.SALARY DESC
        )                                               AS JOB_SALARY_RANK,
        COUNT(*) OVER (
            PARTITION BY eb.DEPARTMENT_ID
        )                                               AS DEPT_EMP_COUNT
    FROM emp_base eb
    LEFT JOIN dept_base db ON db.DEPARTMENT_ID = eb.DEPARTMENT_ID
    LEFT JOIN job_base jb ON jb.JOB_ID = eb.JOB_ID
)
SELECT *
FROM joined;


-- -----------------------------------------------------------------
-- RULE 02 : COMPLEX_JOIN
-- TO_TABLE : HR_DEPT_PAYROLL_ROLLUP_F
-- 설명:
-- - 부서 단위 집계 + 직무 다양성 + 급여분포 + 최근 채용일
-- - 집계 결과에 밴드/상태 컬럼 추가
-- -----------------------------------------------------------------
WITH
emp_dept AS (
    SELECT
        e.EMPLOYEE_ID,
        e.DEPARTMENT_ID,
        e.JOB_ID,
        e.SALARY,
        e.HIRE_DATE
    FROM HR.EMPLOYEES e
),
dept_master AS (
    SELECT
        d.DEPARTMENT_ID,
        d.DEPARTMENT_NAME,
        d.MANAGER_ID AS DEPT_MANAGER_ID
    FROM HR.DEPARTMENTS d
),
agg AS (
    SELECT
        dm.DEPARTMENT_ID                                 AS DEPARTMENT_ID,
        dm.DEPARTMENT_NAME                               AS DEPARTMENT_NAME,
        dm.DEPT_MANAGER_ID                               AS DEPT_MANAGER_ID,
        COUNT(ed.EMPLOYEE_ID)                            AS EMP_COUNT,
        COUNT(DISTINCT ed.JOB_ID)                        AS DISTINCT_JOB_COUNT,
        SUM(ed.SALARY)                                   AS TOTAL_SALARY,
        ROUND(AVG(ed.SALARY), 2)                         AS AVG_SALARY,
        MIN(ed.SALARY)                                   AS MIN_SALARY,
        MAX(ed.SALARY)                                   AS MAX_SALARY,
        MEDIAN(ed.SALARY)                                AS MEDIAN_SALARY,
        STDDEV(ed.SALARY)                                AS SALARY_STDDEV,
        MIN(ed.HIRE_DATE)                                AS FIRST_HIRE_DATE,
        MAX(ed.HIRE_DATE)                                AS LAST_HIRE_DATE,
        SUM(CASE WHEN ed.SALARY >= 12000 THEN 1 ELSE 0 END) AS HIGH_SALARY_COUNT,
        SUM(CASE WHEN ed.SALARY < 7000 THEN 1 ELSE 0 END)   AS LOW_SALARY_COUNT
    FROM dept_master dm
    LEFT JOIN emp_dept ed ON ed.DEPARTMENT_ID = dm.DEPARTMENT_ID
    GROUP BY dm.DEPARTMENT_ID, dm.DEPARTMENT_NAME, dm.DEPT_MANAGER_ID
),
finalized AS (
    SELECT
        a.DEPARTMENT_ID,
        a.DEPARTMENT_NAME,
        a.DEPT_MANAGER_ID,
        a.EMP_COUNT,
        a.DISTINCT_JOB_COUNT,
        a.TOTAL_SALARY,
        a.AVG_SALARY,
        a.MIN_SALARY,
        a.MAX_SALARY,
        a.MEDIAN_SALARY,
        a.SALARY_STDDEV,
        a.FIRST_HIRE_DATE,
        a.LAST_HIRE_DATE,
        a.HIGH_SALARY_COUNT,
        a.LOW_SALARY_COUNT,
        CASE
            WHEN a.EMP_COUNT = 0 THEN 'EMPTY'
            WHEN a.EMP_COUNT >= 10 THEN 'LARGE'
            WHEN a.EMP_COUNT >= 5 THEN 'MEDIUM'
            ELSE 'SMALL'
        END                                              AS DEPT_SIZE_BAND,
        CASE
            WHEN a.AVG_SALARY IS NULL THEN 'N/A'
            WHEN a.AVG_SALARY >= 12000 THEN 'HIGH_PAY'
            WHEN a.AVG_SALARY >= 7000 THEN 'MID_PAY'
            ELSE 'LOW_PAY'
        END                                              AS DEPT_PAY_BAND,
        CASE
            WHEN a.DISTINCT_JOB_COUNT >= 4 THEN 'DIVERSE'
            WHEN a.DISTINCT_JOB_COUNT >= 2 THEN 'BALANCED'
            ELSE 'NARROW'
        END                                              AS JOB_DIVERSITY_BAND
    FROM agg a
)
SELECT *
FROM finalized;


-- -----------------------------------------------------------------
-- RULE 03 : COMPLEX_RESHAPE
-- TO_TABLE : HR_EMP_CAREER_EVENT_F
-- 설명:
-- - 현재 스냅샷 + 이력 레코드를 이벤트로 정규화
-- - 이벤트 유형, 기간, 경력순번, 상태 플래그 생성
-- -----------------------------------------------------------------
WITH
current_events AS (
    SELECT
        e.EMPLOYEE_ID                                   AS EMPLOYEE_ID,
        'CURRENT'                                       AS EVENT_SOURCE,
        e.HIRE_DATE                                     AS EVENT_DATE,
        CAST(NULL AS DATE)                              AS EVENT_END_DATE,
        e.JOB_ID                                        AS JOB_ID,
        e.DEPARTMENT_ID                                 AS DEPARTMENT_ID,
        e.SALARY                                        AS CURRENT_SALARY,
        CAST(NULL AS NUMBER)                            AS HISTORY_GAP_DAYS,
        'CURRENT_SNAPSHOT'                              AS EVENT_TYPE
    FROM HR.EMPLOYEES e
),
history_events AS (
    SELECT
        jh.EMPLOYEE_ID                                  AS EMPLOYEE_ID,
        'HISTORY'                                       AS EVENT_SOURCE,
        jh.START_DATE                                   AS EVENT_DATE,
        jh.END_DATE                                     AS EVENT_END_DATE,
        jh.JOB_ID                                       AS JOB_ID,
        jh.DEPARTMENT_ID                                AS DEPARTMENT_ID,
        CAST(NULL AS NUMBER)                            AS CURRENT_SALARY,
        TRUNC(NVL(jh.END_DATE, SYSDATE) - jh.START_DATE) AS HISTORY_GAP_DAYS,
        'JOB_HISTORY'                                   AS EVENT_TYPE
    FROM HR.JOB_HISTORY jh
),
stacked AS (
    SELECT * FROM current_events
    UNION ALL
    SELECT * FROM history_events
),
enriched AS (
    SELECT
        s.EMPLOYEE_ID,
        s.EVENT_SOURCE,
        s.EVENT_DATE,
        s.EVENT_END_DATE,
        s.JOB_ID,
        s.DEPARTMENT_ID,
        s.CURRENT_SALARY,
        s.HISTORY_GAP_DAYS,
        s.EVENT_TYPE,
        CASE
            WHEN s.EVENT_END_DATE IS NULL THEN 'OPEN'
            ELSE 'CLOSED'
        END                                             AS EVENT_STATUS,
        ROW_NUMBER() OVER (
            PARTITION BY s.EMPLOYEE_ID
            ORDER BY s.EVENT_DATE, s.EVENT_SOURCE
        )                                               AS EMP_EVENT_SEQ,
        LAG(s.JOB_ID) OVER (
            PARTITION BY s.EMPLOYEE_ID
            ORDER BY s.EVENT_DATE, s.EVENT_SOURCE
        )                                               AS PREV_JOB_ID,
        LAG(s.DEPARTMENT_ID) OVER (
            PARTITION BY s.EMPLOYEE_ID
            ORDER BY s.EVENT_DATE, s.EVENT_SOURCE
        )                                               AS PREV_DEPARTMENT_ID
    FROM stacked s
)
SELECT *
FROM enriched;


-- -----------------------------------------------------------------
-- RULE 04 : COMPLEX_RESHAPE
-- TO_TABLE : HR_EMP_HISTORY_STACKED_F
-- 설명:
-- - 요청 패턴 강화: CASE WHEN AA AND BB THEN CC, UNION ALL, NULL AS
-- - 소스별 표준화 + 조건 기반 신호 컬럼 생성
-- -----------------------------------------------------------------
WITH
emp_part AS (
    SELECT
        e.EMPLOYEE_ID                                   AS EMPLOYEE_ID,
        e.HIRE_DATE                                     AS EVENT_DATE,
        CAST(NULL AS DATE)                              AS EVENT_END_DATE,
        NVL(e.FIRST_NAME, '$')                          AS AA,
        NVL(e.LAST_NAME, '$')                           AS BB,
        TO_CHAR(e.SALARY)                               AS CC,
        CASE
            WHEN e.JOB_ID LIKE 'IT_%' AND e.SALARY >= 8000 THEN '$'
            WHEN e.JOB_ID IS NULL THEN '$'
            ELSE e.JOB_ID
        END                                             AS EE,
        'EMPLOYEES'                                     AS SOURCE_TAG,
        e.SALARY                                        AS CURRENT_SALARY,
        CAST(NULL AS NUMBER)                            AS HISTORY_GAP_DAYS,
        e.JOB_ID                                        AS JOB_ID,
        e.DEPARTMENT_ID                                 AS DEPARTMENT_ID
    FROM HR.EMPLOYEES e
),
hist_part AS (
    SELECT
        jh.EMPLOYEE_ID                                  AS EMPLOYEE_ID,
        jh.START_DATE                                   AS EVENT_DATE,
        jh.END_DATE                                     AS EVENT_END_DATE,
        NVL(jh.JOB_ID, '$')                             AS AA,
        NVL(TO_CHAR(jh.DEPARTMENT_ID), '$')             AS BB,
        TO_CHAR(jh.DEPARTMENT_ID)                       AS CC,
        CASE
            WHEN jh.JOB_ID = 'AC_ACCOUNT' AND jh.DEPARTMENT_ID = 110 THEN '$'
            WHEN jh.JOB_ID IS NULL THEN '$'
            ELSE jh.JOB_ID
        END                                             AS EE,
        'JOB_HISTORY'                                   AS SOURCE_TAG,
        CAST(NULL AS NUMBER)                            AS CURRENT_SALARY,
        TRUNC(NVL(jh.END_DATE, SYSDATE) - jh.START_DATE) AS HISTORY_GAP_DAYS,
        jh.JOB_ID                                       AS JOB_ID,
        jh.DEPARTMENT_ID                                AS DEPARTMENT_ID
    FROM HR.JOB_HISTORY jh
),
unioned AS (
    SELECT * FROM emp_part
    UNION ALL
    SELECT * FROM hist_part
),
projected AS (
    SELECT
        u.EMPLOYEE_ID,
        u.EVENT_DATE,
        u.EVENT_END_DATE,
        CASE WHEN u.AA = '$' AND u.BB = '$' THEN u.CC END AS DD,
        CASE WHEN u.EE = '$' AND u.EE = '$' THEN 'DOUBLE_DOLLAR' ELSE u.EE END AS EE_SIGNAL,
        u.SOURCE_TAG,
        u.CURRENT_SALARY,
        u.HISTORY_GAP_DAYS,
        u.JOB_ID,
        u.DEPARTMENT_ID,
        CASE
            WHEN u.SOURCE_TAG = 'EMPLOYEES' THEN 'CURRENT_TRACK'
            ELSE 'HISTORY_TRACK'
        END                                             AS TRACK_TYPE,
        ROW_NUMBER() OVER (
            PARTITION BY u.EMPLOYEE_ID, u.SOURCE_TAG
            ORDER BY u.EVENT_DATE
        )                                               AS TRACK_SEQ
    FROM unioned u
)
SELECT *
FROM projected;


-- -----------------------------------------------------------------
-- RULE 05 : COMPLEX_TYPE_CAST
-- TO_TABLE : HR_EMP_TYPECAST_AUDIT
-- 설명:
-- - 직원 중심 문자열/숫자/날짜 캐스팅
-- - 라운드트립 변환 + 해시키 + 품질검증 플래그
-- -----------------------------------------------------------------
WITH
source_rows AS (
    SELECT
        e.EMPLOYEE_ID,
        e.HIRE_DATE,
        e.SALARY,
        e.COMMISSION_PCT,
        e.DEPARTMENT_ID,
        e.JOB_ID,
        j.MIN_SALARY,
        j.MAX_SALARY
    FROM HR.EMPLOYEES e
    LEFT JOIN HR.JOBS j ON j.JOB_ID = e.JOB_ID
),
casted AS (
    SELECT
        TO_CHAR(s.EMPLOYEE_ID)                          AS EMPLOYEE_ID_TXT,
        TO_CHAR(s.HIRE_DATE, 'YYYYMMDD')                AS HIRE_DATE_YYYYMMDD,
        TO_CHAR(s.SALARY, 'FM9999990D00', 'NLS_NUMERIC_CHARACTERS=.,') AS SALARY_TXT,
        TO_NUMBER(TO_CHAR(s.SALARY, 'FM9999990D00', 'NLS_NUMERIC_CHARACTERS=.,'), '9999990D00', 'NLS_NUMERIC_CHARACTERS=.,') AS SALARY_NUM,
        NVL(TO_CHAR(s.COMMISSION_PCT, 'FM0D99', 'NLS_NUMERIC_CHARACTERS=.,'), '0.00') AS COMMISSION_TXT,
        TO_CHAR(s.DEPARTMENT_ID)                        AS DEPARTMENT_ID_TXT,
        TO_CHAR(s.JOB_ID)                               AS JOB_ID_TXT,
        TO_CHAR(s.MIN_SALARY)                           AS MIN_SALARY_TXT,
        TO_CHAR(s.MAX_SALARY)                           AS MAX_SALARY_TXT,
        CAST(s.MIN_SALARY AS NUMBER(12,2))              AS MIN_SALARY_DECIMAL,
        CAST(s.MAX_SALARY AS NUMBER(12,2))              AS MAX_SALARY_DECIMAL,
        TO_DATE(TO_CHAR(s.HIRE_DATE, 'YYYYMMDD'), 'YYYYMMDD') AS HIRE_DATE_ROUNDTRIP,
        STANDARD_HASH(TO_CHAR(s.EMPLOYEE_ID) || ':' || TO_CHAR(s.HIRE_DATE, 'YYYYMMDD') || ':' || TO_CHAR(s.JOB_ID), 'MD5') AS EMP_HASH_KEY
    FROM source_rows s
),
quality AS (
    SELECT
        c.*,
        CASE
            WHEN c.SALARY_NUM IS NULL THEN 'NUM_CAST_FAIL'
            WHEN c.HIRE_DATE_ROUNDTRIP IS NULL THEN 'DATE_CAST_FAIL'
            ELSE 'OK'
        END                                             AS CAST_QUALITY_STATUS,
        CASE
            WHEN c.MIN_SALARY_DECIMAL IS NULL OR c.MAX_SALARY_DECIMAL IS NULL THEN 'JOB_RANGE_MISSING'
            WHEN c.MIN_SALARY_DECIMAL > c.MAX_SALARY_DECIMAL THEN 'JOB_RANGE_INVALID'
            ELSE 'JOB_RANGE_OK'
        END                                             AS JOB_RANGE_CHECK
    FROM casted c
)
SELECT *
FROM quality;


-- -----------------------------------------------------------------
-- RULE 06 : COMPLEX_TYPE_CAST
-- TO_TABLE : HR_DEPT_TYPECAST_AUDIT
-- 설명:
-- - 부서 집계 타입캐스트 + 문자열/숫자 변환 + 상태판정
-- - 해시키/스냅샷 컬럼 포함
-- -----------------------------------------------------------------
WITH
dept_agg AS (
    SELECT
        d.DEPARTMENT_ID,
        d.DEPARTMENT_NAME,
        COUNT(e.EMPLOYEE_ID)                            AS EMP_COUNT,
        SUM(e.SALARY)                                   AS TOTAL_SALARY,
        AVG(e.SALARY)                                   AS AVG_SALARY,
        MIN(e.SALARY)                                   AS MIN_SALARY,
        MAX(e.SALARY)                                   AS MAX_SALARY
    FROM HR.DEPARTMENTS d
    LEFT JOIN HR.EMPLOYEES e ON e.DEPARTMENT_ID = d.DEPARTMENT_ID
    GROUP BY d.DEPARTMENT_ID, d.DEPARTMENT_NAME
),
casted AS (
    SELECT
        TO_CHAR(a.DEPARTMENT_ID)                        AS DEPARTMENT_ID_TXT,
        LPAD(TO_CHAR(a.DEPARTMENT_ID), 6, '0')          AS DEPARTMENT_ID_LPAD,
        a.DEPARTMENT_NAME                               AS DEPARTMENT_NAME,
        TO_CHAR(a.EMP_COUNT)                            AS EMP_COUNT_TXT,
        CAST(a.EMP_COUNT AS NUMBER(10,0))               AS EMP_COUNT_NUM,
        TO_CHAR(a.TOTAL_SALARY, 'FM9999999990D00', 'NLS_NUMERIC_CHARACTERS=.,') AS TOTAL_SALARY_TXT,
        TO_CHAR(a.AVG_SALARY, 'FM9999990D00', 'NLS_NUMERIC_CHARACTERS=.,')       AS AVG_SALARY_TXT,
        TO_CHAR(a.MIN_SALARY, 'FM9999990D00', 'NLS_NUMERIC_CHARACTERS=.,')       AS MIN_SALARY_TXT,
        TO_CHAR(a.MAX_SALARY, 'FM9999990D00', 'NLS_NUMERIC_CHARACTERS=.,')       AS MAX_SALARY_TXT,
        CAST(a.AVG_SALARY AS NUMBER(12,2))              AS AVG_SALARY_NUM,
        TO_CHAR(SYSDATE, 'YYYYMMDD')                    AS SNAP_DATE_YYYYMMDD,
        TO_DATE(TO_CHAR(SYSDATE, 'YYYYMMDD'), 'YYYYMMDD') AS SNAP_DATE,
        STANDARD_HASH(TO_CHAR(a.DEPARTMENT_ID) || ':' || TO_CHAR(a.EMP_COUNT) || ':' || TO_CHAR(NVL(a.TOTAL_SALARY,0)), 'MD5') AS DEPT_HASH_KEY
    FROM dept_agg a
),
status_labeled AS (
    SELECT
        c.*,
        CASE
            WHEN c.EMP_COUNT_NUM = 0 THEN 'EMPTY'
            WHEN c.EMP_COUNT_NUM >= 10 THEN 'LARGE'
            WHEN c.EMP_COUNT_NUM >= 5 THEN 'MEDIUM'
            ELSE 'SMALL'
        END                                             AS DEPT_SIZE_BAND,
        CASE
            WHEN c.AVG_SALARY_NUM IS NULL THEN 'N/A'
            WHEN c.AVG_SALARY_NUM >= 12000 THEN 'HIGH_PAY'
            WHEN c.AVG_SALARY_NUM >= 7000 THEN 'MID_PAY'
            ELSE 'LOW_PAY'
        END                                             AS DEPT_PAY_BAND
    FROM casted c
)
SELECT *
FROM status_labeled;
