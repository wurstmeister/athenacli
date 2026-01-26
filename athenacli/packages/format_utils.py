# -*- coding: utf-8 -*-


def format_status(rows_length=None, cursor=None, backend=None):
    """Format query status including row count and backend-specific statistics.

    Args:
        rows_length: Number of rows returned
        cursor: Database cursor after query execution
        backend: DatabaseBackend instance for backend-specific formatting

    Returns:
        str: Formatted status message
    """
    return rows_status(rows_length) + statistics(cursor, backend)


def rows_status(rows_length):
    """Format row count message.

    Args:
        rows_length: Number of rows returned

    Returns:
        str: Row count message
    """
    if rows_length:
        return '%d row%s in set' % (rows_length, '' if rows_length == 1 else 's')
    else:
        return 'Query OK'


def statistics(cursor, backend=None):
    """Format backend-specific execution statistics.

    Args:
        cursor: Database cursor after query execution
        backend: DatabaseBackend instance for backend-specific formatting

    Returns:
        str: Formatted statistics (may be empty for some backends)
    """
    if cursor and backend:
        return backend.format_statistics(cursor)
    else:
        return ''
