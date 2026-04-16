# Migration Agent

Oracle SQL Migration Agent입니다.
이 프로그램은 `NEXT_SQL_INFO` 테이블에서 `STATUS='FAIL'` 인 행을 읽어 아래 순서로 재시도합니다.

1. AS-IS SQL(`FR_SQL_TEXT`, 필요 시 `EDIT_FR_SQL`)을 기반으로 TO-BE SQL 생성
2. 바인드 후보를 뽑기 위한 BIND SQL 생성 및 실행
3. 바인드 세트(`BIND_SET`) 구성
4. FROM / TO 결과 건수 비교용 TEST SQL 생성 및 실행
5. 검증 결과를 `NEXT_SQL_INFO`에 반영

또한 정답 SQL을 별도 컬럼으로 보관하고, SQLite 기반 벡터 인덱스를 통해 stage별 RAG 예시를 불러옵니다.

## 1. 현재 패키지 구조

```text
migration-main/
  app/
    common.py
    db.py
    main.py
    orchestrator.py
    batch/
      app.py
      poller.py
    prompts/
      bind_sql_prompt.txt
      test_sql_no_bind_prompt.txt
      test_sql_prompt.txt
      tobe_sql_prompt.txt
    repositories/
      mapper_repository.py
      result_repository.py
    services/
      binding_service.py
      feedback_rag_service.py
      llm_service.py
      prompt_service.py
      validation_service.py
      xml_parser_service.py
  tools/
    init_db.py
    inspect_rag_index.py
    list_mapping_rules.py
    sync_feedback_rag.py
    _bootstrap.py
  migration.db
  requirements.txt
  README.md
```

설명:
- `app/main.py`: 배치 실행 엔트리포인트
- `app/batch/app.py`: 스케줄러 시작, 시그널 처리, startup sync
- `app/batch/poller.py`: Oracle polling 루프
- `app/orchestrator.py`: 한 건의 migration job 전체 orchestration
- `app/repositories/*`: Oracle 테이블 조회/갱신
- `app/services/*`: LLM, RAG, prompt, validation, binding 등 도메인 로직
- `tools/*`: 점검/동기화/조회용 유틸리티 스크립트

## 2. 배치 실행 흐름

### 2.1 시작 시점

배치 시작 파일:
- [main.py](/c:/Users/11824/Downloads/migration-main/migration-main/app/main.py)
- 실제 실행 로직: [app.py](/c:/Users/11824/Downloads/migration-main/migration-main/app/batch/app.py)

시작 시 수행하는 일:
- `.env` 로드
- Oracle 연결 준비
- RAG startup sync 수행
- APScheduler 시작
- 1분 주기로 `NEXT_SQL_INFO` polling

### 2.2 polling 대상

조회 repository:
- [result_repository.py](/c:/Users/11824/Downloads/migration-main/migration-main/app/repositories/result_repository.py)

기본 대상:
- `NEXT_SQL_INFO`
- 조건: `STATUS='FAIL'`

### 2.3 job orchestration

주 오케스트레이터:
- [orchestrator.py](/c:/Users/11824/Downloads/migration-main/migration-main/app/orchestrator.py)

한 건 처리 순서:
1. mapping rule 조회
2. TOBE stage RAG 예시 조회
3. TOBE SQL 생성
4. 바인드 필요 여부 판단
5. BIND stage RAG 예시 조회
6. BIND SQL 생성
7. BIND SQL 실행
8. `BIND_SET` 생성
9. TEST stage RAG 예시 조회
10. TEST SQL 생성
11. TEST SQL 실행
12. FROM / TO 결과 비교
13. PASS / FAIL 결정
14. `NEXT_SQL_INFO` 업데이트

## 3. 정답 SQL 컬럼 구조

기존 `CORRECT_SQL` 단일 컬럼 대신 stage별 컬럼을 사용합니다.

- `TOBE_CORRECT_SQL`
- `BIND_CORRECT_SQL`
- `TEST_CORRECT_SQL`

의미:
- TOBE 프롬프트에는 `TOBE_CORRECT_SQL` 기반 예시를 사용
- BIND 프롬프트에는 `BIND_CORRECT_SQL` 기반 예시를 사용
- TEST 프롬프트에는 `TEST_CORRECT_SQL` 기반 예시를 사용

즉, RAG도 stage별로 분리됩니다.

## 4. RAG / 임베딩 / 벡터DB

RAG 저장소는 외부 벡터DB가 아니라 SQLite 파일입니다.
기본 파일은 루트의 `migration.db` 입니다.

### 4.1 저장 위치

환경변수:

```env
RAG_VECTOR_DB_PATH=migration.db
RAG_VECTOR_TABLE=feedback_rag_index
RAG_TOP_K=5
RAG_CORPUS_LIMIT=2000
```

기본값:
- DB 파일: `migration.db`
- 테이블: `feedback_rag_index`

### 4.2 언제 저장되는가

저장 진입점:
- [feedback_rag_service.py](/c:/Users/11824/Downloads/migration-main/migration-main/app/services/feedback_rag_service.py)
- sync 실행 스크립트: [sync_feedback_rag.py](/c:/Users/11824/Downloads/migration-main/migration-main/tools/sync_feedback_rag.py)

저장 흐름:
1. Oracle `NEXT_SQL_INFO`에서 정답 SQL 행 읽기
2. stage별(`TOBE`, `BIND`, `TEST`)로 corpus 구성
3. 문서 텍스트 생성
4. 임베딩 API 호출
5. SQLite에 upsert

### 4.3 어떤 데이터가 저장되는가

`feedback_rag_index` 주요 컬럼:
- `doc_id`
- `correct_kind`
- `space_nm`
- `sql_id`
- `source_sql`
- `generated_sql`
- `correct_sql`
- `edited_yn`
- `upd_ts`
- `pattern_tags_json`
- `text_hash`
- `embedding_json`

중요 포인트:
- `correct_kind`: `TOBE`, `BIND`, `TEST`
- `correct_sql`: 각 stage의 정답 SQL
- `embedding_json`: 실제 벡터 값

### 4.4 로그 전체를 저장하나

아니요.
현재 벡터DB에는 `NEXT_SQL_INFO.LOG` 전체를 저장하지 않습니다.

저장하는 것은 주로 다음입니다.
- source SQL
- generated SQL
- correct SQL
- edited 여부
- update 시각
- pattern tag
- embedding

저장하지 않는 것:
- 전체 실행 로그
- retry 로그 전체
- bind case 실행 로그
- test row 결과 전체

### 4.5 retrieval 방식

한 job 처리 시 stage별로 retrieval 합니다.

예:
- TOBE 생성 전: `correct_kind='TOBE'`
- BIND 생성 전: `correct_kind='BIND'`
- TEST 생성 전: `correct_kind='TEST'`

정렬 기준:
1. 벡터 유사도
2. pattern tag overlap bonus
3. namespace / sql_id 조건

`RAG_TOP_K=5` 라면 stage별 최대 5건입니다.
즉 “전체에서 5건”이 아니라 “각 stage에서 최대 5건”입니다.

### 4.6 retrieval 예시가 LLM에 들어가는 방식

retrieval 결과는 Python dict list로 구성되고, prompt 렌더링 시 JSON 문자열로 삽입됩니다.
프롬프트 변수명은 `feedback_examples_json` 입니다.

### 4.7 언제 비어 보일 수 있나

다음 경우 RAG가 비어 보일 수 있습니다.
- `NEXT_SQL_INFO` 에 stage별 정답 SQL이 실제로 없음
- 해당 컬럼이 NULL만 들어 있음
- Oracle 스키마가 현재 코드가 기대하는 컬럼 구조와 다름
- sync를 아직 실행하지 않음
- `migration.db` 를 다른 경로로 보고 있음
- 임베딩 API가 실패해서 upsert가 안 됨

## 5. 현재 RAG 소스 판정 조건

RAG source 조회 함수:
- [result_repository.py](/c:/Users/11824/Downloads/migration-main/migration-main/app/repositories/result_repository.py)
- 함수명: `get_feedback_corpus_rows(correct_kind=...)`

의도상 동작:
- `TOBE_CORRECT_SQL` 이 있으면 TOBE corpus 대상
- `BIND_CORRECT_SQL` 이 있으면 BIND corpus 대상
- `TEST_CORRECT_SQL` 이 있으면 TEST corpus 대상

즉 3개 중 하나라도 값이 있으면, 해당 stage에서는 인식되어야 합니다.

다만 실제 DB 컬럼 타입이나 메타데이터 조회 방식에 따라 누락될 수 있으므로, 이상하면 `tools/list_mapping_rules.py` 가 아니라 실제 Oracle 컬럼 메타와 `tools/sync_feedback_rag.py` 결과를 함께 확인해야 합니다.

## 6. LLM provider 구조

현재는 provider 하드코딩이 아니라 `.env` 기준으로 분기합니다.

지원 provider:
- `anthropic`
- `openai`

구현 파일:
- [llm_service.py](/c:/Users/11824/Downloads/migration-main/migration-main/app/services/llm_service.py)

### 6.1 Anthropic 직통 예시

```env
LLM_PROVIDER=anthropic
LLM_BASE_URL=https://api.anthropic.com
LLM_API_KEY=your_anthropic_key
LLM_MODEL=claude-haiku-4-5-20251001
```

허용 형태:
- `https://api.anthropic.com`
- `https://api.anthropic.com/v1`
- `https://api.anthropic.com/v1/messages`

코드에서 base root로 정규화합니다.

### 6.2 OpenAI 호환 사내 gateway 예시

```env
LLM_PROVIDER=openai
LLM_BASE_URL=https://your-company-gateway/v1
LLM_API_KEY=your_gateway_key
LLM_MODEL=gpt-4.1
```

권장:
- `LLM_BASE_URL` 은 `/v1` 까지만 넣기
- `/chat/completions` 전체 경로를 넣는 것은 비권장

코드에서 일부 endpoint suffix는 정규화하지만, 루트 + `/v1` 형태가 가장 안전합니다.

### 6.3 Oracle 스키마 분리 예시

로그인 계정과 실제 테이블 소유 스키마가 다를 수 있으면 `.env` 에 아래도 명시합니다.

```env
ORACLE_USER=...
ORACLE_PASSWORD=...
ORACLE_DSN=...
ORACLE_SCHEMA=HR
```

설명:
- `ORACLE_USER`: 로그인 계정
- `ORACLE_SCHEMA`: `NEXT_SQL_INFO`, `NEXT_MIG_INFO`, `NEXT_MIG_INFO_DTL` 를 조회할 스키마
- `ORACLE_SCHEMA` 를 비우면 기존처럼 스키마 미지정 테이블명을 사용합니다.

## 7. TEST SQL 정책

TEST SQL은 현재 deterministic builder를 사용합니다.
즉 LLM이 아니라 코드가 직접 SQL을 조립합니다.

구현 파일:
- [llm_service.py](/c:/Users/11824/Downloads/migration-main/migration-main/app/services/llm_service.py)

규칙:
- 각 bind case마다 `SELECT ... FROM DUAL` 한 줄 생성
- 여러 케이스는 `UNION ALL` 로 결합
- Oracle에서 `UNION` / `UNION ALL` 사용 시 각 `SELECT` 끝에 반드시 `FROM DUAL` 이 오도록 유지

## 8. 주요 DB 컬럼

### 8.1 대상 테이블

- `NEXT_SQL_INFO`
- `NEXT_MIG_INFO`
- `NEXT_MIG_INFO_DTL`

### 8.2 `NEXT_SQL_INFO` 주요 컬럼

입력 계열:
- `TAG_KIND`
- `SPACE_NM`
- `SQL_ID`
- `FR_SQL_TEXT`
- `TARGET_TABLE`
- `EDIT_FR_SQL`

생성 결과:
- `TO_SQL_TEXT`
- `BIND_SQL`
- `BIND_SET`
- `TEST_SQL`

상태 / 로그:
- `STATUS`
- `LOG`
- `UPD_TS`
- `EDITED_YN`

정답 SQL:
- `TOBE_CORRECT_SQL`
- `BIND_CORRECT_SQL`
- `TEST_CORRECT_SQL`

## 9. 실행 방법

의존성 설치:

```bash
py -m pip install -r requirements.txt --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org
```

연결 점검:

```bash
py tools/init_db.py
```

배치 실행:

```bash
py app/main.py
```

RAG 동기화:

```bash
py tools/sync_feedback_rag.py
py tools/sync_feedback_rag.py --limit 500
```

RAG 인덱스 조회:

```bash
py tools/inspect_rag_index.py
py tools/inspect_rag_index.py --limit 20
py tools/inspect_rag_index.py --show-vector
```

mapping rule 조회:

```bash
py tools/list_mapping_rules.py --format table
py tools/list_mapping_rules.py --fr-table TB_A --to-table TB_B --format json --out rules.json
```

## 10. 운영 시 점검 포인트

1. `.env` 의 provider / endpoint / key / model이 실제 환경과 맞는지
2. Oracle 스키마에 `TOBE_CORRECT_SQL`, `BIND_CORRECT_SQL`, `TEST_CORRECT_SQL` 이 실제로 존재하는지
3. `migration.db` 경로가 기대한 파일을 보고 있는지
4. 임베딩 API(`RAG_EMBED_BASE_URL`)가 실제로 설정되어 있는지
5. startup sync 로그에서 `source_rows`, `upserted`, `deleted` 값이 정상인지

## 11. 발표/운영 관점 요약

이 프로젝트의 핵심은 다음입니다.
- Oracle migration 실패 건을 배치로 다시 생성 / 검증 / 반영
- stage별 정답 SQL을 별도 관리
- stage별 RAG 예시를 이용해 LLM 품질 개선
- 벡터DB는 외부 서비스가 아니라 로컬 SQLite로 관리
- provider는 Anthropic / OpenAI 호환 gateway 둘 다 수용 가능

