# Oracle Mapper SQL Migration Agent 실행 가이드

이 문서는 현재 커밋 기준 실행 절차를 정리한 가이드입니다.

## 1. 사전 준비

### 1.1 Python 설치 확인

아래 중 하나가 동작해야 합니다.

```powershell
python --version
```

```powershell
py --version
```

### 1.2 의존성 설치

```powershell
pip install -r requirements.txt
```

## 2. 환경변수 설정

루트의 `.env`를 채웁니다.  
템플릿은 `.env.example`를 참고하세요.

### 2.1 Oracle 필수

- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN`
- `ORACLE_CLIENT_LIB_DIR` (환경에 맞게)

### 2.2 LLM 필수

- `LLM_API_KEY`
- `LLM_MODEL`
- `LLM_BASE_URL`

### 2.3 Embedding/RAG 필수

- `RAG_EMBED_BASE_URL`
- `RAG_EMBED_MODEL` (권장: `BAAI/bge-m3`)
- `RAG_EMBED_API_KEY` (필요 시)

### 2.4 XML Parser/운영 옵션

- `MAPPER_XML_SOURCE_DIR`
- `XML_PARSER_DATA_DIR`
- `ACTIVE_SQL_ID_TABLE`
- `ACTIVE_SQL_ID_COLUMN`
- `TEST_MAPPING_TABLES`

주의: `ACTIVE_SQL_ID_COLUMN` 값은 반드시 `NAMESPACE.SQL_ID` 형식이어야 합니다.

## 3. 연결 점검

Oracle/LLM/Embedding을 한 번에 점검합니다.

```powershell
python init_db.py
```

성공 시 `[OK] ORACLE`, `[OK] LLM`, `[OK] EMBEDDING` 로그가 출력됩니다.

## 4. XML 파서 실행 (필요 시)

`NEXT_SQL_INFO`를 XML 기준으로 구성/정리하려면 아래를 사용합니다.

### 4.1 전체 단계 한 번에 실행

```powershell
python -m app.services.xml_parser_service all --source-dir <mapper_xml_dir> --output-dir <json_out_dir>
```

### 4.2 단계별 실행

```powershell
python -m app.services.xml_parser_service stage1 --source-dir <mapper_xml_dir> --output-dir <json_out_dir>
python -m app.services.xml_parser_service stage2 --output-dir <json_out_dir>
python -m app.services.xml_parser_service stage3
python -m app.services.xml_parser_service stage4
```

## 5. 배치 실행

```powershell
python app\main.py
```

동작:
- 1분 주기로 `NEXT_SQL_INFO` 폴링
- `STATUS='FAIL'` 건 재처리
- TO-BE SQL 생성 + bind/test 검증 + 상태 업데이트

주의:
- 배치 실행 중에는 벡터 인덱스를 자동 저장하지 않습니다.
- 배치에서는 저장된 벡터 인덱스를 조회만 합니다.

종료:
- 1회 `Ctrl + C`: 안전 종료
- 2회 `Ctrl + C`: 강제 종료

## 6. RAG 벡터 인덱스 확인

### 6.1 수동 동기화(저장)

```powershell
py sync_feedback_rag.py
py sync_feedback_rag.py --limit 500
```

### 6.2 인덱스 확인(조회)

벡터 저장 상태를 확인합니다.

```powershell
python inspect_rag_index.py
python inspect_rag_index.py --limit 20
python inspect_rag_index.py --show-vector
```

## 7. 자주 발생하는 문제

1. `python` 명령이 안 됨  
- `py`로 실행하거나 Python 설치 경로/alias 확인

2. Oracle 연결 실패  
- `ORACLE_*` 값과 Instant Client 경로 확인

3. 임베딩 연결 실패  
- `RAG_EMBED_BASE_URL`, `RAG_EMBED_MODEL`, 인증 헤더(`RAG_EMBED_API_KEY`) 확인

4. Stage4에서 예외 발생 (`NAMESPACE.SQL_ID` 관련)  
- `ACTIVE_SQL_ID_COLUMN` 값이 `SQL_ID` 단독이 아닌 `NAMESPACE.SQL_ID` 형식인지 확인
