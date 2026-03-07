from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from app.core.ids import extract_tiktok_video_id
from sqlalchemy import func, select
from sqlalchemy import delete as sa_delete

from app.db.session import create_schema
from app.db.models import Job, JobLog, ScheduledTask, Video
from app.db.session import get_db
from app.worker.tasks import task_import_csv


def run_import_csv(path: str) -> None:
    with get_db() as db:
        task_import_csv(db, "manual", path)


def cleanup_stale_queued_jobs(
    max_age_minutes: int = 30,
    dry_run: bool = False,
    job_types: list[str] | None = None,
    video_id: str | None = None,
) -> tuple[int, int]:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=max_age_minutes)
    with get_db() as db:
        q = select(Job).where(Job.status == "queued", Job.updated_at <= cutoff)
        if job_types:
            q = q.where(Job.type.in_(job_types))
        if video_id:
            q = q.where(Job.video_id == video_id)

        rows = db.execute(q).scalars().all()
        total = len(rows)
        if dry_run:
            return total, 0

        changed = 0
        for job in rows:
            job.status = "failed"
            job.error = f"cleaned as stale queued job (older than {max_age_minutes} minutes)"
            db.add(
                JobLog(
                    job_id=job.id,
                    level="warn",
                    message="stale queued job cleaned",
                    meta={"max_age_minutes": max_age_minutes, "cutoff": cutoff.isoformat()},
                )
            )
            changed += 1
        return total, changed


def cleanup_invalid_tiktok_videos(*, dry_run: bool = False) -> tuple[int, int, int]:
    with get_db() as db:
        invalid_videos = [
            row
            for row in db.execute(select(Video)).scalars().all()
            if not extract_tiktok_video_id(row.url or "")
        ]
        invalid_ids = [v.id for v in invalid_videos]
        invalid_count = len(invalid_ids)

        if not invalid_ids:
            return 0, 0, 0

        scheduled_pending = db.scalar(
            select(func.count(ScheduledTask.id)).where(ScheduledTask.video_id.in_(invalid_ids))
        )
        job_total = db.scalar(
            select(func.count(Job.id)).where(Job.video_id.in_(invalid_ids))
        ) or 0

        if dry_run:
            return invalid_count, int(scheduled_pending or 0), int(job_total)

        db.execute(
            sa_delete(Job)
            .where(Job.video_id.in_(invalid_ids))
            .where(Job.status.in_(["queued", "running"]))
        )
        for vid in invalid_videos:
            db.delete(vid)

        return invalid_count, int(scheduled_pending or 0), int(job_total)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["upgrade", "import-csv", "cleanup-queued", "cleanup-invalid-videos"])
    parser.add_argument("--minutes", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--type", dest="job_types", action="append", help="filter by job type; repeatable, e.g. --type snapshot")
    parser.add_argument("--video-id", dest="video_id", help="filter by video_id")
    parser.add_argument("path", nargs="?")
    args = parser.parse_args()

    if args.command == "upgrade":
        create_schema()
        return

    if args.command == "import-csv" and args.path:
        run_import_csv(args.path)
        return

    if args.command == "cleanup-queued":
        total, changed = cleanup_stale_queued_jobs(
            max_age_minutes=args.minutes,
            dry_run=args.dry_run,
            job_types=args.job_types,
            video_id=args.video_id,
        )
        action = "dry-run" if args.dry_run else "updated"
        print(f"stale queued jobs [{action}]: total={total}, changed={changed}")
        return

    if args.command == "cleanup-invalid-videos":
        invalid_videos, affected_tasks, affected_jobs = cleanup_invalid_tiktok_videos(dry_run=args.dry_run)
        action = "dry-run" if args.dry_run else "cleaned"
        print(
            f"invalid tiktok videos [{action}]: videos={invalid_videos}, "
            f"scheduled_tasks={affected_tasks}, jobs_with_video={affected_jobs}"
        )
        return


if __name__ == "__main__":
    main()
