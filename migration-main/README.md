# Migration Agent

Oracle SQL Migration Agent 프로젝트입니다.  
이 프로젝트는 Oracle의 `NEXT_SQL_INFO`를 배치로 읽어 SQL 마이그레이션 작업을 수행하고, TOBE SQL 생성 이후 BIND/TEST/GOOD SQL 튜닝까지 순차적으로 처리합니다.

현재 구조는 `flows + features` 기준으로 정리되어 있습니다.

- `flows`: 실제 배치/잡 단계 흐름을 표현하는 계층
- `features`: 각 단계에서 사용하는 기능 구현 계층
- `repositories`: Oracle/SQLite 조회 및 저장 계층
- `runtime`: 프로세스 시작과 스케줄러 실행 래퍼
- `services`: 공통 유틸과 외부 연동 서비스

## 1. 전체 실행 흐름

프로세스 시작 순서는 아래와 같습니다.

1. `app/main.py`
2. `app/runtime/main_flow.py`
3. `app/flows/runtime_flow.py`
4. `app/flows/job_flow.py`
5. 각 단계별 `app/features/*`

실행 순서만 보면 다음과 같습니다.

```text
main.py
  -> bootstrap_runtime()
  -> run_startup_sync()
  -> start_scheduler()
```

배치 1사이클은 아래 순서로 동작합니다.

```text
init_cycle
  -> startup_rag_sync      (startup 시에만)
  -> load_pending_jobs
  -> process_jobs
  -> finish_cycle
```

개별 row는 아래 순서로 동작합니다.

```text
INIT
  -> LOAD_RULES
  -> LOAD_TOBE_FEEDBACK
  -> GENERATE_TOBE_SQL
  -> DETECT_BIND_PARAMS
      -> LOAD_BIND_FEEDBACK
      -> GENERATE_BIND_SQL
      -> EXECUTE_BIND_SQL
      -> BUILD_BIND_SET
  -> GENERATE_TEST_SQL
  -> EXECUTE_TEST_SQL
  -> EVALUATE_STATUS
  -> LOAD_TUNING_CONTEXT
  -> GENERATE_GOOD_SQL
  -> GENERATE_GOOD_TEST_SQL
  -> EXECUTE_GOOD_TEST_SQL
  -> EVALUATE_TUNING_STATUS
  -> PERSIST_SUCCESS / PERSIST_TUNING_RESULT / PERSIST_FAILURE
```

## 2. 현재 패키지 구조

```text
app/
  main.py
  common.py
  db.py
  batch/
    app.py
    poller.py
  runtime/
    main_flow.py
    batch_runtime.py
  flows/
    runtime_flow.py
    job_flow.py
    tobe_flow.py
    bind_flow.py
    validation_flow.py
    tuning_flow.py
  features/
    tobe/
      tobe_feature.py
    bind/
      bind_feature.py
    validation/
      validation_feature.py
    rag/
      feedback_rag_service.py
      tobe_rag_indexer.py
      tobe_rag_models.py
      tobe_rag_service.py
    sql_tuning/
      sql_normalizer.py
      rule_catalog.py
      rule_detector.py
      tuning_context_builder.py
      llm_proposer.py
      good_test_sql_generator.py
      tuning_verifier.py
      tuning_repository.py
      tuning_models.py
      tuning_pipeline.py
  repositories/
    mapper_repository.py
    migration_log_repository.py
    result_repository.py
  prompts/
    tobe_sql_prompt.txt
    bind_sql_prompt.txt
    tobe_rag_rerank_prompt.txt
    tuning_sql_prompt.txt
  services/
    binding_service.py
    llm_service.py
    prompt_service.py
    validation_service.py
    xml_parser_service.py
```

핵심 읽기 순서는 아래가 맞습니다.

1. `app/main.py`
2. `app/runtime/main_flow.py`
3. `app/flows/runtime_flow.py`
4. `app/flows/job_flow.py`
5. 필요한 단계의 `app/flows/*`
6. 필요한 기능의 `app/features/*`

## 3. 각 폴더 책임

### `app/runtime`

- 프로세스 부트스트랩
- startup sync 실행
- 스케줄러 실행 래퍼

### `app/flows`

- LangGraph 노드/엣지 구성
- 단계 간 라우팅
- stage orchestration
- job/batch 단위 상태 관리

### `app/features`

- TOBE SQL 생성
- BIND SQL 생성 및 bind set 생성
- TEST SQL 생성 및 검증
- TOBE/BIND RAG
- SQL 튜닝 2차 파이프라인

### `app/repositories`

- Oracle에서 입력 row 조회
- 결과 저장
- 로그 저장

### `app/services`

현재는 공통 서비스 계층으로 유지합니다.

- LLM 호출
- 프롬프트 렌더링
- XML 파싱
- bind 추출/생성
- validation 실행

## 4. DB 기준 핵심 컬럼

주요 입력 테이블:

- `NEXT_SQL_INFO`
- `NEXT_MIG_INFO`
- `NEXT_MIG_INFO_DTL`

`NEXT_SQL_INFO`에서 자주 보는 컬럼:

입력 계열:

- `TAG_KIND`
- `SPACE_NM`
- `SQL_ID`
- `FR_SQL_TEXT`
- `EDIT_FR_SQL`
- `TARGET_TABLE`

1차 생성 결과:

- `TO_SQL_TEXT`
- `BIND_SQL`
- `BIND_SET`
- `TEST_SQL`
- `STATUS`
- `LOG`

2차 튜닝 결과:

- `GOOD_SQL`
- `GOOD_TEST_SQL`
- `TUNING_STATUS`
- `TUNING_UPD_TS`

정답/학습 계열:

- `TOBE_CORRECT_SQL`
- `BIND_CORRECT_SQL`
- `TEST_CORRECT_SQL`

## 5. RAG 구조

현재 RAG는 stage별로 분리되어 있습니다.

### TOBE RAG

위치:

- `app/features/rag/tobe_rag_service.py`
- `app/features/rag/tobe_rag_indexer.py`

특징:

- case 기반 RAG
- SQLite metadata + Faiss dense index
- BM25 sparse retrieval
- rule tag / feature tag 기반 필터
- LLM rerank

주요 파일:

- `migration.db`
- `migration_tobe_rag.faiss`

### BIND RAG

위치:

- `app/features/rag/feedback_rag_service.py`

특징:

- legacy feedback RAG 유지
- 현재는 BIND stage에서만 사용

### TEST

- runtime에서 별도 RAG retrieval 없이 deterministic builder 사용

## 6. SQL 튜닝 구조

SQL 튜닝은 TOBE 검증 이후의 2차 파이프라인입니다.

흐름:

```text
TO_SQL_TEXT
  -> normalize
  -> rule detect
  -> tuning context build
  -> LLM proposal
  -> GOOD_TEST_SQL 생성
  -> TO_SQL_TEXT vs GOOD_SQL 검증
  -> 통과 시 GOOD_SQL 승격
```

위치:

- `app/flows/tuning_flow.py`
- `app/features/sql_tuning/*`

현재 원칙:

- 직접 auto-fix는 하지 않음
- LLM proposal + 검증 중심
- 검증 실패 시 GOOD_SQL 승격 안 함
- 결과는 `NEXT_SQL_INFO`와 `NEXT_SQL_TUNING_LOG`에 저장

## 7. XML Parser 사용법

`xml_parser_service`는 프로젝트 루트에서 module mode로 실행해야 합니다.

권장 실행:

```powershell
py -m app.services.xml_parser_service stage1 --source-dir sample_mappers --output-dir app/services/DATA
py -m app.services.xml_parser_service stage2 --output-dir app/services/DATA
py -m app.services.xml_parser_service stage3
py -m app.services.xml_parser_service stage4
```

단계 설명:

- `stage1`: XML을 읽어 JSON payload 생성
- `stage2`: JSON payload를 `NEXT_SQL_INFO`에 upsert
- `stage3`: include 확장 및 후처리
- `stage4`: active table 기준 cleanup

전체 실행:

```powershell
py -m app.services.xml_parser_service all --source-dir sample_mappers --output-dir app/services/DATA
```

주의:

- `No module named 'app'` 오류가 나면 repo root에서 실행 중인지 먼저 확인
- 직접 파일 실행보다 `py -m ...` 방식 사용 권장

## 8. 실행 방법

의존성 설치:

```powershell
py -m pip install -r requirements.txt
```

배치 실행:

```powershell
py app/main.py
```

TOBE RAG 동기화:

```powershell
py tools/sync_tobe_rag.py
py tools/sync_tobe_rag.py --limit 500
```

BIND RAG 동기화:

```powershell
py tools/sync_feedback_rag.py
py tools/sync_feedback_rag.py --limit 500
```

TOBE RAG 디버그:

```powershell
py tools/debug_tobe_rag.py --from-file sample.sql
```

RAG 인덱스 조회:

```powershell
py tools/inspect_tobe_rag.py
py tools/inspect_rag_index.py
```

매핑룰 조회:

```powershell
py tools/list_mapping_rules.py --format table
py tools/list_mapping_rules.py --format json --out rules.json
```

## 9. 운영 점검 포인트

- `.env`의 Oracle/LLM/RAG 설정이 현재 운영 환경과 맞는지
- `TOBE_CORRECT_SQL`, `BIND_CORRECT_SQL`, `TEST_CORRECT_SQL` 데이터가 실제로 쌓이고 있는지
- startup sync 로그에서 `source_rows`, `upserted`, `deleted` 수치가 정상인지
- `migration.db`, `migration_tobe_rag.faiss` 경로가 현재 실행 위치 기준으로 맞는지
- `NEXT_SQL_INFO`의 `STATUS`, `TUNING_STATUS`가 기대한 단계로 움직이는지

## 10. 테스트

현재 자주 쓰는 확인 명령:

```powershell
py -m unittest tests.test_graph_runtime_routes tests.test_orchestrator_routes tests.test_tuning_pipeline_utils
py -m compileall app tests tools
```

## 11. 메모

- 현재 구조에서 읽기 시작점은 항상 `main -> runtime -> flows -> features` 순서입니다.
- `flows`는 orchestration, `features`는 구현이라는 경계를 유지하는 것이 가장 중요합니다.
- `services`는 당분간 공통 유틸 계층으로 유지합니다.
