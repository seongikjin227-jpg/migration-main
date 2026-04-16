MERGE INTO HR.TGT_EMP tgt
USING (
    SELECT
        e.EMPLOYEE_ID AS EMP_ID,
        e.FIRST_NAME || ' ' || e.LAST_NAME AS NAME,
        e.EMAIL,
        e.HIRE_DATE,
        e.JOB_ID,
        e.SALARY
    FROM HR.EMPLOYEES e
) src
ON (tgt.EMP_ID = src.EMP_ID)
WHEN MATCHED THEN
    UPDATE SET
        tgt.NAME = src.NAME,
        tgt.EMAIL = src.EMAIL,
        tgt.HIRE_DATE = src.HIRE_DATE,
        tgt.JOB_ID = src.JOB_ID,
        tgt.SALARY = src.SALARY
WHEN NOT MATCHED THEN
    INSERT (
        EMP_ID,
        NAME,
        EMAIL,
        HIRE_DATE,
        JOB_ID,
        SALARY
    )
    VALUES (
        src.EMP_ID,
        src.NAME,
        src.EMAIL,
        src.HIRE_DATE,
        src.JOB_ID,
        src.SALARY
    );
