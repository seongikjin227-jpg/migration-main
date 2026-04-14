# Complex SQL 대응 설계 정리

## 1) 목표
- 단순 컬럼 치환이 아닌 복합형 SQL(중첩 서브쿼리, 다중 JOIN, 페이징, MyBatis 동적 태그) 변환 정확도 향상
- FROM SQL 재현이 아니라 TOBE 스키마 기준의 동등 기능 SQL 생성

## 2) 현재 파이프라인에서 복합형을 푸는 방식

### 2.1 오케스트레이션 루프
- 파일: `app/agent/mapper_sql_agent.py`
- 흐름:
  1. 매핑 룰 로드
  2. 피드백 RAG 예시 로드
  3. TO-BE SQL 생성
  4. bind SQL 생성/실행(필요 시)
  5. test SQL 생성/실행
  6. FROM_COUNT vs TO_COUNT 검증
- 특징:
  - 검증 결과가 `FAIL`이어도 재시도 루프로 재진입
  - `CASE_NO/FROM_COUNT/TO_COUNT`를 `last_error`로 전달해 다음 생성 시 교정 유도

### 2.2 매핑 스코프 축소
- 파일: `app/services/llm_service.py`
- 방식:
  - 전체 매핑룰을 그대로 주입하지 않음
  - `TARGET_TABLE`에 해당하는 FR_TABLE 매핑룰만 선별 주입
  - `TARGET_TABLE` 부재 시 FROM SQL 참조 테이블 기준으로 fallback

### 2.3 프롬프트 규칙(복합형)
- 파일: `app/prompts/tobe_sql_prompt.txt`
- 강화 포인트:
  - 중첩 서브쿼리 평탄화(가능 시)
  - 스칼라 서브쿼리의 LEFT JOIN 변환 우선(단, 건수/중복 위험 시 안전 우선)
  - ANSI JOIN 표준화
  - MyBatis 해소 중 중복 FROM/WHERE 발생 시 단일 논리 절로 병합
  - 페이징은 Oracle 표준 패턴(`ROW_NUMBER`/`FETCH FIRST`)만 허용
  - `LIMIT/OFFSET` 직접 사용 금지

### 2.4 실행 전 안전 검증
- 파일: `app/services/validation_service.py`
- 수행:
  - MyBatis 런타임 토큰 잔존 검사
  - 다중 SQL 차단
  - LIMIT/FETCH -> Oracle 11g 호환 보정(ROWNUM 래핑)

## 3) RAG 보강(복합형 중심)

### 3.1 패턴 태그 스키마
- 파일: `app/services/feedback_rag_service.py`
- 인덱스에 `pattern_tags_json` 컬럼 저장
- 자동 추출 태그 예:
  - `NESTED_SUBQUERY`
  - `SUBQUERY`
  - `SCALAR_SUBQUERY`
  - `MULTI_JOIN`
  - `PAGING`
  - `NON_ORACLE_PAGING`
  - `MULTI_FROM_CLAUSE`
  - `MULTI_WHERE_CLAUSE`
  - `MYBATIS_DYNAMIC_TAG`
  - `MYBATIS_PLACEHOLDER`

### 3.2 검색 점수
- 코사인 유사도 + 태그 일치 보너스 혼합 점수 사용
- 복합형 태그가 겹치는 예제가 상위로 오도록 가중

### 3.3 기대 효과
- 쉬운 케이스는 기존처럼 처리
- 복합형 케이스는 복합형 정답 예제를 우선 참조해 구조 변환 실패율 감소

## 4) 발표용 핵심 메시지
- “규칙 기반 + 예시 기반(RAG) + 실행 검증 + 재시도”의 4중 안전장치 구조
- 매핑은 `정확도`, 프롬프트는 `구조 변환`, 검증은 `실행 가능성`, RAG는 `복합형 일반화`를 담당
- 복합형 실패를 단발성으로 끝내지 않고, 실패 신호를 다시 프롬프트로 환류하는 폐루프 구조
