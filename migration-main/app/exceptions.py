class AgentBaseException(Exception):
    """Base exception for migration agent."""
    pass


class LLMRateLimitError(AgentBaseException):
    """Retryable LLM API rate-limit/timeout error."""
    pass


class DBSqlError(AgentBaseException):
    """Retryable DB SQL execution error (syntax/object/runtime)."""
    pass
