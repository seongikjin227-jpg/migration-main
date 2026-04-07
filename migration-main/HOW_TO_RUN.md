# Oracle Mapper SQL Migration Agent 실행 가이드

## 1. 목적

이 배치는 다음 순서로 동작합니다.

1. `NEXT_MIG_INFO`에서 매핑 룰을 조회
2. `NEXT_SQL_INFO`에서 대상 SQL 행을 조회
3. `EDIT_FR_SQL`이 있으면 우선 사용, 없으면 `FR_SQL_TEXT` 사용
4. LLM 호출로 TO-BE SQL 생성
5. 생성 결과를 `NEXT_SQL_INFO.TO_SQL_TEXT`에 저장

## 2. 필수 환경변수

프로젝트 루트에 `.env` 파일을 생성합니다.

```env
ORACLE_USER=
ORACLE_PASSWORD=
ORACLE_DSN=
LLM_API_KEY=
LLM_MODEL=
LLM_BASE_URL=
```

참고:
- 매핑 테이블은 코드에서 고정: `NEXT_MIG_INFO`
- SQL 테이블은 코드에서 고정: `NEXT_SQL_INFO`

## 3. 설치

```powershell
pip install -r requirements.txt
```


## 4. DB 연결 확인

```powershell
py init_db.py
```

위 명령은 `NEXT_MIG_INFO`, `NEXT_SQL_INFO` 두 테이블 연결 가능 여부를 확인합니다.

## 5. 배치 실행

```powershell
py app\main.py
```

동작 규칙:
- 10초마다 폴링
- `USE_YN='Y'` 이고 `TARGET_YN='Y'` 인 행만 대상
- 잠금 처리: `TARGET_YN`을 `Y -> R`로 변경
- SQL 생성 후 `TO_SQL_TEXT` 업데이트
- 완료 처리: `TARGET_YN`을 `R -> N`으로 변경
- 반복 실패 시 오류를 애플리케이션 로그에 기록하고 `TARGET_YN='N'`으로 종료

## 6. NEXT_MIG_INFO 필수 컬럼

- `MAP_TYPE`
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
- `USE_YN`
- `TARGET_YN`
- `UPD_TS`
- `USER_EDITED`
- `CORRECT_SQL`

## 8. 현재 프롬프트 입력값

LLM 프롬프트에는 아래 값이 들어갑니다.

- `TAG_KIND`
- `SPACE_NM`
- `SQL_ID`
- `source_sql` (`EDIT_FR_SQL` 우선, 없으면 `FR_SQL_TEXT`)
- `NEXT_MIG_INFO` 전체 매핑 룰(JSON)
- `NEXT_SQL_INFO.USER_EDITED`, `CORRECT_SQL`, 기존 `TO_SQL_TEXT` 기반 피드백 예시
- 재시도 시 마지막 오류(`last_error`)

## 9. 종료

배치 중단은 `Ctrl + C`로 합니다.
