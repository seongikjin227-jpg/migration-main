# Migration Agent

Oracle 기반 SQL Migration 배치 에이전트입니다.  
`NEXT_SQL_INFO`의 실패 건(`STATUS='FAIL'`)을 주기적으로 재처리하면서:

1. AS-IS SQL(`FR_SQL_TEXT` 또는 `EDIT_FR_SQL`)을 TO-BE SQL로 생성
2. 검증용 bind 추출 SQL 생성/실행
3. 테스트 SQL 생성/실행
4. 결과를 같은 row에 업데이트

핵심은 "SQL 생성"만이 아니라 "실행 가능한지까지 검증"하는 것입니다.

## 1) 프로젝트 구조

```text
migration-main/
  app/
    main.py                      # 배치 엔트리포인트 (APScheduler 1분 주기)
    batch/
      runner.py                  # poll_database(), Job 루프
    agent/
      mapper_sql_agent.py        # MigrationOrchestrator 핵심 파이프라인
    repositories/
      mapper_repository.py       # 매핑룰 조회 (NEXT_MIG_INFO + DTL)
      result_repository.py       # 대상 Job 조회/결과 업데이트/피드백 조회
    services/
      llm_service.py             # 프롬프트 렌더 + LLM 호출 + SQL 정규화
      binding_service.py         # bind 파라미터명 추출, bind_set 구성
      feedback_rag_service.py    # CORRECT_SQL 임베딩/RAG 검색
      validation_service.py      # bind/test SQL 실행 및 PASS/FAIL 판정
      prompt_service.py          # 템플릿 로더
      xml_parser_service.py      # mapper XML -> NEXT_SQL_INFO 적재 유틸
      case_service.py            # (현재 비어 있음)
    prompts/
      tobe_sql_prompt.txt
      bind_sql_prompt.txt
      test_sql_prompt.txt
      test_sql_no_bind_prompt.txt
    config.py                    # .env 로딩, Oracle 연결
    models.py                    # SqlInfoJob, MappingRuleItem
    exceptions.py                # LLMRateLimitError, DBSqlError
    runtime.py                   # graceful stop 이벤트
    logger.py                    # 공통 로거
  init_db.py                     # 필수 테이블 접근 점검
  list_mapping_rules.py          # 매핑룰 조회 CLI
  sync_feedback_rag.py           # 피드백 RAG 벡터 수동 동기화
  requirements.txt
```

## 2) 런타임 실행 흐름

### 2.1 스케줄러

- `app/main.py`
- APScheduler `interval=1 minute`
- 첫 실행 `next_run_time=datetime.now()`
- SIGINT/SIGTERM 처리:
  - 1회: graceful shutdown 요청
  - 2회: 강제 종료

### 2.2 Job 조회 기준

- `app/repositories/result_repository.py:get_pending_jobs`
- 현재 처리 대상은 **`STATUS='FAIL'`** 레코드만입니다.
- 정렬: `UPD_TS NULLS FIRST, SPACE_NM, SQL_ID`

### 2.3 오케스트레이션 단계

- `app/agent/mapper_sql_agent.py:MigrationOrchestrator.process_job`

처리 순서:

1. 매핑 룰 로딩 (`get_all_mapping_rules`)  
   - 프롬프트 주입 전 `TARGET_TABLE`(없으면 `FROM SQL` 참조 테이블) 기준으로 관련 FR_TABLE 매핑만 선별
2. 피드백 예시 로딩 (`feedback_rag_service.retrieve_feedback_examples`)  
3. TO-BE SQL 생성 (`generate_tobe_sql`)  
4. `TAG_KIND != SELECT` 이면 TO-BE만 업데이트하고 `PASS` 종료  
5. bind 파라미터 유무 판정
   - 없으면 bind/test를 no-bind 경로로 처리
   - 있으면 bind SQL 생성/실행 후 bind_set 생성
6. test SQL 생성 (`generate_test_sql` 또는 `generate_test_sql_no_bind`)  
7. test SQL 실행 후 결과 판정 (`evaluate_status_from_test_rows`)  
8. 결과 업데이트 (`update_cycle_result`)

재시도 정책:

- 최대 3회 재시도 (`retry_count <= 3`)
- `LLMRateLimitError`, 일반 예외 모두 재시도 대상
- 테스트 판정 결과가 `FAIL`인 경우도 재시도 대상
  - `CASE_NO/FROM_COUNT/TO_COUNT` 요약을 `last_error`로 전달해 다음 생성 시 교정 유도
- 최종 실패 시 `STATUS='FAIL'` 및 마지막 산출물/에러 로그 저장

## 3) LLM/SQL 처리 규칙

- `app/services/llm_service.py`
- 프롬프트는 `app/prompts/*.txt`를 템플릿으로 렌더링
- LLM 응답 정규화:
  - 코드블록 추출
  - SQL 시작 키워드 검증
  - trailing `LIMIT n` -> `FETCH FIRST n ROWS ONLY` 치환
  - 세미콜론 다중문 금지
- 실행 직전 검증(`validation_service.py`):
  - `<if>`, `<choose>`, `#{}`, `${}` 등 MyBatis 런타임 토큰 포함 시 실패
  - 다중 SQL 금지
  - Oracle 11g 호환 row limit 래핑 (`ROWNUM`)

## 4) PASS/FAIL 판정 규칙

- `evaluate_status_from_test_rows(rows)`
- 테스트 SQL은 반드시 컬럼을 포함해야 함:
  - `CASE_NO`
  - `FROM_COUNT`
  - `TO_COUNT`
- 판정:
  - 모든 케이스에서 `FROM_COUNT == TO_COUNT`
  - 단, `0 == 0`은 실패로 취급 (양쪽 모두 데이터 미발견)
  - 위 조건 불만족 또는 결과 없음 -> `FAIL`

## 5) DB 계약 (코드 기준)

### 5.1 조회/갱신 대상 테이블

- 결과 테이블: `NEXT_SQL_INFO`
- 매핑 마스터: `NEXT_MIG_INFO`
- 매핑 상세: `NEXT_MIG_INFO_DTL`

### 5.2 NEXT_SQL_INFO 사용 컬럼

- 읽기:
  - `TAG_KIND`, `SPACE_NM`, `SQL_ID`, `FR_SQL_TEXT`, `TARGET_TABLE`, `EDIT_FR_SQL`
  - `TO_SQL_TEXT`, `BIND_SQL`, `BIND_SET`, `TEST_SQL`
  - `STATUS`, `LOG`, `UPD_TS`, `EDITED_YN`, `CORRECT_SQL`
- 쓰기:
  - `TO_SQL_TEXT`, `BIND_SQL`, `BIND_SET`, `TEST_SQL`
  - `STATUS`, `LOG`, `UPD_TS`

`result_repository`는 `USER_TAB_COLUMNS`를 조회해 컬럼 길이를 확인하고, 비 CLOB 컬럼은 UTF-8 byte 기준으로 자동 truncate 후 저장합니다.

## 6) 환경변수

`.env`는 프로젝트 루트(`migration-main/.env`)에 둡니다.

필수:

- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN`
- `LLM_API_KEY`
- `LLM_MODEL`
- `LLM_BASE_URL`

선택:

- `ORACLE_CLIENT_LIB_DIR` (기본값: `C:\oracle\instantclient_21c`)
- `MAPPER_XML_SOURCE_DIR`
- `XML_PARSER_DATA_DIR`
- `ACTIVE_SQL_ID_TABLE`
- `ACTIVE_SQL_ID_COLUMN` (기본값: `SQL_ID`)
- `TEST_MAPPING_TABLES`
- `RAG_EMBED_BASE_URL` (임베딩 API URL)
- `RAG_EMBED_MODEL` (기본: `BAAI/bge-m3`)
- `RAG_EMBED_API_KEY` (선택)
- `RAG_EMBED_TIMEOUT_SEC` (기본: `30`)
- `RAG_VECTOR_DB_PATH` (기본: `migration.db`)
- `RAG_VECTOR_TABLE` (기본: `feedback_rag_index`)
- `RAG_TOP_K` (기본: `5`)
- `RAG_CORPUS_LIMIT` (기본: `2000`)

## 7) 실행 방법

Windows 환경에서는 `python` 대신 `py` 실행을 기준으로 사용합니다.

의존성 설치:

```bash
pip install -r requirements.txt
```

통합 연결 점검(Oracle/LLM/Embedding):

```bash
py init_db.py
```

배치 실행:

```bash
py app/main.py
```

RAG 벡터 수동 동기화:

```bash
py sync_feedback_rag.py
py sync_feedback_rag.py --limit 500
```

매핑룰 조회 유틸:

```bash
py list_mapping_rules.py --format table
py list_mapping_rules.py --fr-table TB_A --to-table TB_B --format json --out rules.json
```

RAG 벡터 인덱스 확인:

```bash
py inspect_rag_index.py
py inspect_rag_index.py --limit 20
py inspect_rag_index.py --show-vector
```

XML parser 유틸:

```bash
py -m app.services.xml_parser_service stage1 --source-dir <mapper_dir> --output-dir <out_dir>
py -m app.services.xml_parser_service stage2 --output-dir <out_dir>
py -m app.services.xml_parser_service stage3
py -m app.services.xml_parser_service stage4
py -m app.services.xml_parser_service all --source-dir <mapper_dir> --output-dir <out_dir>
```

## 8) 운영 시 주의사항

- 현재 설계상 신규 건 자동 처리보다 "실패 건 재처리 루프"에 최적화되어 있습니다.
- `TAG_KIND != SELECT`는 실행 검증 없이 TO-BE 생성만 하고 `PASS`로 종료됩니다.
- LLM 응답 품질은 프롬프트와 `feedback_examples`(`EDITED_YN`, `CORRECT_SQL`)에 강하게 의존합니다.

## 9) README 동기화 원칙

코드 변경 시 아래 항목이 영향받으면 README를 같은 PR/커밋에서 같이 업데이트합니다.

- 패키지 구조/파일 역할 변경
- 오케스트레이션 단계/재시도 정책 변경
- PASS/FAIL 판정 규칙 변경
- ENV/테이블/컬럼 계약 변경
- 실행 명령 변경
