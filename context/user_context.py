
from contextvars import ContextVar
from typing import Any, Dict

user_context: ContextVar[Dict[str, Any]] = ContextVar("user_context", default={})
