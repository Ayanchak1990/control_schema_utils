"""
Shared internal helpers for quper_control_schema_utils.

This module is private — not exported via __init__.py.
"""

import logging
import traceback
from typing import Type


def _escape_sql(value: str) -> str:
    """
    Escape single quotes for safe SQL string interpolation.

    Args:
        value: Raw string value.

    Returns:
        String with single quotes doubled.
    """
    return str(value).replace("'", "''")


def _raise_error(
    log: logging.Logger,
    context: str,
    message: str,
    exc: Exception,
    error_class: Type[Exception],
) -> None:
    """
    Log at ERROR level and raise error_class.

    Must be called from within an except block so that
    traceback.format_exc() captures the active exception.

    Args:
        log:         Module-level logger of the caller.
        context:     Context string, e.g. "pipeline=foo, object=bar".
        message:     Short description of what failed, e.g. "Failed to read watermark".
        exc:         The caught exception.
        error_class: Exception subclass to raise.
    """
    log.error(f"[{context}] {message}: {exc}\n{traceback.format_exc()}")
    raise error_class(f"{message} for {context}: {exc}") from exc


def _swallow_error(
    log: logging.Logger,
    context: str,
    message: str,
    exc: Exception,
) -> None:
    """
    Log at WARNING level and swallow the exception.

    Used in must-never-raise functions where a logging failure
    must not crash the pipeline.

    Must be called from within an except block so that
    traceback.format_exc() captures the active exception.

    Args:
        log:     Module-level logger of the caller.
        context: Context string, e.g. "pipeline=foo, object=bar".
        message: Short description of what failed.
        exc:     The caught exception.
    """
    log.warning(f"[{context}] {message}: {exc}\n{traceback.format_exc()}")
