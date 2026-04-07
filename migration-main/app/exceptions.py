class AgentBaseException(Exception):
    """모든 에이전트 예외의 상위 예외"""
    pass

class LLMRateLimitError(AgentBaseException):
    """LLM API Rate Limit 또는 토큰 제한 초과 시 발생하는 에러 (재시도용)"""
    pass

class DBSqlError(AgentBaseException):
    """생성된 SQL 실행 중 문법/런타임 DB 오류 발생 시 에러 (재시도용)"""
    pass

class VerificationFailError(AgentBaseException):
    """검증(Verification) 실패 시 에러 (데이터 불일치 등)"""
    pass
