"""오케스트레이션/DB 검증 단계에서 공통으로 쓰는 예외 계층."""


class AgentBaseException(Exception):
    """에이전트 공통 예외의 최상위 클래스."""


class LLMRateLimitError(AgentBaseException):
    """LLM 일시 장애(429/timeout 등)로 재시도 가능한 예외."""


class DBSqlError(AgentBaseException):
    """DB SQL 실행 오류(문법/객체/런타임) 예외."""
