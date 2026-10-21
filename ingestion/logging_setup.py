"""Structured logging configuration for PakFuelWatch."""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict


class StructuredJsonFormatter(logging.Formatter):
    """JSON log formatter producing structured logs with timestamp, level, and context."""

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a structured JSON string.

        Args:
            record: Standard library LogRecord instance.

        Returns:
            JSON-formatted string representation of the log entry.
        """
        log_payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }

        # Include exception info if available
        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)

        # Include custom extra fields if attached to record
        standard_attrs = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "taskName",
        }
        extras = {k: v for k, v in record.__dict__.items() if k not in standard_attrs}
        if extras:
            log_payload["extra"] = extras

        return json.dumps(log_payload, default=str)


def setup_logging(
    logger_name: str = "pakfuelwatch",
    log_level: str = "INFO",
    as_json: bool = True,
) -> logging.Logger:
    """Configure and return a structured logger.

    Args:
        logger_name: Name of the logger to configure.
        log_level: Log level threshold (e.g., 'DEBUG', 'INFO', 'WARNING', 'ERROR').
        as_json: Whether to format output as JSON (True) or human-readable text (False).

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(logger_name)
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(numeric_level)

    # Avoid duplicate handlers if setup_logging is invoked repeatedly
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(numeric_level)

        if as_json:
            handler.setFormatter(StructuredJsonFormatter())
        else:
            handler.setFormatter(
                logging.Formatter(
                    fmt="%(asctime)s [%(levelname)s] %(name)s (%(module)s:%(lineno)d): %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )

        logger.addHandler(handler)

    logger.propagate = False
    return logger
