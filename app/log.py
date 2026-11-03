"""JSON logs for the gateway logger.

Uvicorn's access line stays in its own format. Lines from this logger
carry ``trace_id`` even when the message text does not. A line with no
request in scope has ``trace_id`` null.
"""

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone

trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)

logger = logging.getLogger("gateway")


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": trace_id.get(),
        }
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging() -> None:
    """Attach one JSON handler. A second call in the same process does nothing."""
    if logger.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
