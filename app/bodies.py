"""Raw request and response bodies for one trace id.

The worker is a synchronous Celery process. pymongo is the sync driver.
motor is an asyncio driver, and an event loop inside a prefork worker
is a good way to hang a process, so it is not used here.
"""

import logging

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger("gateway")


def open_bodies(url: str) -> tuple[MongoClient, Collection]:
    """Open the client and make ``trace_id`` unique. A second document with that id is rejected."""
    client: MongoClient = MongoClient(
        url,
        serverSelectionTimeoutMS=1000,
        connectTimeoutMS=1000,
        socketTimeoutMS=1000,
    )
    collection = client["zohal"]["audit_bodies"]
    collection.create_index("trace_id", unique=True, name="trace_id_unique")
    client.admin.command("ping")
    logger.info("mongo opened")
    return client, collection


def insert_body(collection: Collection, message: dict) -> None:
    """Insert one document. A duplicate ``trace_id`` is success, not a second document.

    Called after the PostgreSQL commit. If this raises, the task is retried:
    the row already exists, and this insert runs again.
    """
    document = {
        "trace_id": message["trace_id"],
        "request": message["request"],
        "response": message["response"],
    }
    try:
        collection.insert_one(document)
    except DuplicateKeyError:
        logger.info("audit_body_exists trace_id=%s", message["trace_id"])
        return
    request = message["request"]
    response = message["response"]
    logger.info(
        "audit_body trace_id=%s request_bytes=%s response_bytes=%s request_dropped=%s response_dropped=%s",
        message["trace_id"],
        request.get("bytes"),
        response.get("bytes"),
        request.get("dropped"),
        response.get("dropped"),
    )
