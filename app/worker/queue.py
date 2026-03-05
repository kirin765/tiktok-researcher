from __future__ import annotations

import uuid

from app.settings import get_settings


class SyncResult:
    def __init__(self, job_id: str) -> None:
        self.id = job_id


def enqueue(func, *args, **kwargs):
    settings = get_settings()
    if settings.worker_sync:
        func(*args, **kwargs)
        return SyncResult(str(uuid.uuid4()))

    try:
        from redis import Redis
        from rq import Queue

        conn = Redis.from_url(settings.redis_url)
        q = Queue("viral-factory", connection=conn)
        return q.enqueue(func, args=args, kwargs=kwargs)
    except Exception:
        func(*args, **kwargs)
        return SyncResult(str(uuid.uuid4()))
