"""
Shared internal helpers for quper_control_schema_utils.

This module is private — not exported via __init__.py.
"""


def _escape_sql(value: str) -> str:
    """
    Escape single quotes for safe SQL string interpolation.

    Args:
        value: Raw string value.

    Returns:
        String with single quotes doubled.
    """
    return str(value).replace("'", "''")
