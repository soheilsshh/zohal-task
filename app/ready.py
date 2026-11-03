"""Readiness. Redis and the broker only.

A provider being down is not this process being unable to serve. That
shows up as a circuit and a provider status, not as a failed readiness check.
"""

import logging

logger = logging.getLogger("gateway")


def broker_ready(url: str) -> bool:
    """Open one AMQP connection and close it. Do not call a provider."""
    from kombu import Connection

    try:
        with Connection(url, connect_timeout=1) as conn:
            conn.ensure_connection(max_retries=0)
    except Exception as exc:
        logger.warning("broker_unavailable error=%s", type(exc).__name__)
        return False
    return True
