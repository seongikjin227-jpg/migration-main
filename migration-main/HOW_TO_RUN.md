# Oracle Mapper SQL Migration Agent 실행 가이드

## 1. 목적

배치는 다음 순서로 동작합니다.

1. `NEXT_MIG_INFO` / `NEXT_MIG_INFO_DTL`에서 매핑 룰 조회
2. `NEXT_SQL_INFO`에서 `STATUS='FAIL'` 행만 조회
3. `EDIT_FR_SQL`이 있으면 우선 사용, 없으면 `FR_SQL_TEXT` 사용
4. LLM 호출로 TO-BE SQL / BIND SQL / TEST SQL 생성
5. 검증 후 `NEXT_SQL_INFO` 결과 컬럼 업데이트

## 2. 필수 환경변수

프로젝트 루트 `.env`:

```env
ORACLE_USER=
ORACLE_PASSWORD=
ORACLE_DSN=
LLM_API_KEY=
LLM_MODEL=
LLM_BASE_URL=
```

## 3. 설치

```powershell
pip install -r requirements.txt
```

## 4. DB 연결 확인

```powershell
py init_db.py
```

## 5. 배치 실행

```powershell
py app\main.py
```

동작 규칙:
- 10초마다 폴링
- `STATUS='FAIL'` 행만 처리
- 처리 결과는 `STATUS`에 `PASS` 또는 `FAIL`만 기록
- 처리 시 `TO_SQL_TEXT`, `BIND_SQL`, `BIND_SET`, `TEST_SQL`, `LOG`, `UPD_TS` 업데이트

## 6. NEXT_MIG_INFO / NEXT_MIG_INFO_DTL 필수 컬럼

- `FR_TABLE`
- `FR_COL`
- `TO_TABLE`
- `TO_COL`

## 7. NEXT_SQL_INFO 필수 컬럼

- `TAG_KIND`
- `SPACE_NM`
- `SQL_ID`
- `FR_SQL_TEXT`
- `EDIT_FR_SQL`
- `TO_SQL_TEXT`
- `BIND_SQL`
- `BIND_SET`
- `TEST_SQL`
- `STATUS`
- `LOG`
- `UPD_TS`
- `EDITED_YN`
- `CORRECT_SQL`

## 8. 종료

배치 중단: `Ctrl + C`
