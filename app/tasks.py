"""Audit insert. This task does not call a provider.

The message is acknowledged after both writes because ``acks_late`` is on.
PostgreSQL is first. MongoDB is second. A crash between them redelivers the
message: the row is already there, and the document insert runs again.
Five failures is the end: Celery logs the error and does not requeue it.
"""

import logging

from celery.signals import worker_process_init, worker_process_shutdown

from app.audit import create_sync_engine, insert_audit_row
from app.bodies import insert_body, open_bodies
from app.celery_app import celery
from app.log import configure_logging, trace_id
from app.settings import get_settings

logger = logging.getLogger("gateway")

_engine = None
_mongo = None
_bodies = None


@worker_process_init.connect
def _open_database(**_kwargs) -> None:
    global _engine, _mongo, _bodies
    configure_logging()
    settings = get_settings()
    _engine = create_sync_engine(settings.database_url_sync)
    _mongo, _bodies = open_bodies(settings.mongo_url)
    logger.info("audit worker database opened")


@worker_process_shutdown.connect
def _close_database(**_kwargs) -> None:
    global _engine, _mongo, _bodies
    if _engine is not None:
        _engine.dispose()
        _engine = None
    if _mongo is not None:
        _mongo.close()
        _mongo = None
        _bodies = None


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
    """Insert the row, then the bodies. Raise to retry. Do not call a provider."""
    if _engine is None or _bodies is None:
        raise RuntimeError("audit worker database is not open")
    token = trace_id.set(message.get("trace_id"))
    try:
        logger.info(
            "audit_task trace_id=%s retry=%s",
            message.get("trace_id"),
            self.request.retries,
        )
        insert_audit_row(_engine, message)
        insert_body(_bodies, message)
    finally:
        trace_id.reset(token)


def publish_audit(message: dict) -> None:
    """Hand the message to RabbitMQ. Return without waiting for the worker."""
    write_audit.apply_async(args=[message])
    logger.info("audit_publish trace_id=%s", message["trace_id"])
