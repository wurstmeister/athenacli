# encoding: utf-8
"""Database backend abstraction layer."""

from athenacli.backends.base import DatabaseBackend
from athenacli.backends.athena import AthenaBackend
from athenacli.backends.redshift import RedshiftBackend

__all__ = ['DatabaseBackend', 'AthenaBackend', 'RedshiftBackend']


def create_backend(backend_type, **config):
    """Factory function to create appropriate backend instance.

    Args:
        backend_type: 'athena' or 'redshift'
        **config: Backend-specific configuration parameters

    Returns:
        DatabaseBackend instance

    Raises:
        ValueError: If backend_type is not supported
    """
    backends = {
        'athena': AthenaBackend,
        'redshift': RedshiftBackend,
    }

    backend_class = backends.get(backend_type.lower())
    if not backend_class:
        raise ValueError(
            f"Unsupported backend type: {backend_type}. "
            f"Supported types: {', '.join(backends.keys())}"
        )

    return backend_class(**config)
