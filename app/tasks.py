"""Audit insert. This task does not call a provider.

The message is acknowledged after the database commit because
``acks_late`` is on. A crash before the ack delivers the message again.
Five failures is the end: Celery logs the error and does not requeue it.
"""

import logging

from celery.signals import worker_process_init, worker_process_shutdown

from app.audit import create_sync_engine, insert_audit_row
from app.celery_app import celery
from app.settings import get_settings

logger = logging.getLogger("gateway")

_engine = None


@worker_process_init.connect
def _open_database(**_kwargs) -> None:
    global _engine
    _engine = create_sync_engine(get_settings().database_url_sync)
    logger.info("audit worker database opened")


@worker_process_shutdown.connect
def _close_database(**_kwargs) -> None:
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None


@celery.task(
    bind=True,
    acks_late=True,
    max_retries=5,
    autoretry_for=(Exception,),
    retry_backoff=1,
    retry_backoff_max=8,
    retry_jitter=False,
)
def write_audit(self, message: dict) -> None:
    """Insert the audit row. Raise to retry. Do not call a provider."""
    if _engine is None:
        raise RuntimeError("audit worker database is not open")
    logger.info(
        "audit_task trace_id=%s retry=%s",
        message.get("trace_id"),
        self.request.retries,
    )
    insert_audit_row(_engine, message)


def publish_audit(message: dict) -> None:
    """Hand the message to RabbitMQ. Return without waiting for the worker."""
    write_audit.apply_async(args=[message])
    logger.info("audit_publish trace_id=%s", message["trace_id"])
