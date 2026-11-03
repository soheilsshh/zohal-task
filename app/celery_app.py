"""Celery application.

The worker process is this app. The API imports it only to publish.
There is no result backend: the request does not wait for the insert.
"""

from celery import Celery

from app.settings import get_settings

settings = get_settings()

celery = Celery("zohal", broker=settings.celery_broker_url)
celery.conf.update(
    task_ignore_result=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # One unacked message per process. A stuck insert must not hide the rest of the queue.
    worker_prefetch_multiplier=1,
    task_serializer="json",
    accept_content=["json"],
    broker_connection_timeout=1,
    broker_connection_retry_on_startup=True,
    broker_transport_options={"socket_timeout": 1, "connect_timeout": 1},
)
celery.autodiscover_tasks(["app"])
