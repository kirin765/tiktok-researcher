from __future__ import annotations

import time
from datetime import datetime, timezone

from sqlalchemy import select

from app.db.models import Job
from app.db.models import ScheduledTask
from app.db.session import get_db
from app.settings import get_settings
from app.worker.tasks import task_fetch_metrics_snapshot_with_task
from app.worker.queue import enqueue


def run_once() -> int:
    now = datetime.now(tz=timezone.utc)
    provider = get_settings().provider_default
    enqueued = 0
    with get_db() as db:
        rows = (
            db.execute(
                select(ScheduledTask)
                .where(ScheduledTask.status == "pending")
                .where(ScheduledTask.due_at <= now)
                .order_by(ScheduledTask.due_at)
                .limit(500)
            )
            .scalars()
            .all()
        )

        for row in rows:
            job = Job(type="snapshot", status="queued", video_id=row.video_id)
            db.add(job)
            db.flush()
            row.status = "enqueued"
            row.updated_at = now
            enqueue(task_fetch_metrics_snapshot_with_task, str(row.video_id), provider, None, str(row.id), str(job.id))
            enqueued += 1

    return enqueued


def main() -> None:
    while True:
        run_once()
        time.sleep(60)


if __name__ == "__main__":
    main()
