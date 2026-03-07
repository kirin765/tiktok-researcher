from __future__ import annotations

import time
import traceback
from datetime import datetime, timezone
from datetime import timedelta
import logging

from sqlalchemy import func, select

from app.db.models import Job, JobLog, ScheduledTask, Video
from app.db.session import get_db
from app.settings import get_settings
from app.worker.tasks import _log
from app.worker.tasks import task_discover_videos
from app.worker.tasks import task_generate_brief
from app.worker.tasks import task_fetch_metrics_snapshot_with_task
from app.worker.queue import enqueue
from app.monitoring.telegram_notifier import send_telegram_message


logger = logging.getLogger(__name__)
_LAST_HEALTH_ALERT_SIGNATURE: tuple[int, int, int, int] | None = None


def _collect_health_metrics() -> tuple[dict[str, object], str]:
    settings = get_settings()
    now = datetime.now(tz=timezone.utc)
    window_start = now - timedelta(hours=settings.scheduler_health_check_interval_hours)
    stale_cutoff = now - timedelta(minutes=settings.scheduler_running_stale_minutes)

    with get_db() as db:
        failed_jobs = db.scalar(
            select(func.count(Job.id))
            .where(Job.status == "failed")
            .where(Job.updated_at >= window_start)
        )
        error_logs = db.scalar(
            select(func.count(JobLog.id))
            .where(JobLog.level == "error")
            .where(JobLog.ts >= window_start)
        )
        overdue_scheduled = db.scalar(
            select(func.count(ScheduledTask.id))
            .where(ScheduledTask.status == "pending")
            .where(ScheduledTask.due_at <= now)
        )
        stale_running = db.scalar(
            select(func.count(Job.id))
            .where(Job.status == "running")
            .where(Job.updated_at <= stale_cutoff)
        )

        failed_jobs = int(failed_jobs or 0)
        error_logs = int(error_logs or 0)
        overdue_scheduled = int(overdue_scheduled or 0)
        stale_running = int(stale_running or 0)

        recent_errors = db.execute(
            select(JobLog)
            .where(JobLog.level == "error")
            .where(JobLog.ts >= window_start)
            .order_by(JobLog.ts.desc())
            .limit(10)
        ).scalars().all()

        recent_failed_jobs = db.execute(
            select(Job)
            .where(Job.status == "failed")
            .order_by(Job.updated_at.desc())
            .limit(5)
        ).scalars().all()
        recent_errors = [
            (
                entry.ts.isoformat(),
                str(entry.job_id),
                (str(entry.message)[:157] + "...") if entry.message and len(str(entry.message)) > 160 else str(entry.message),
            )
            for entry in recent_errors
        ]
        recent_failed_jobs = [
            (str(row.id), row.type, str(row.error or "")[:140])
            for row in recent_failed_jobs
        ]

    signature = (failed_jobs, error_logs, overdue_scheduled, stale_running)

    body_parts: list[str] = []
    if failed_jobs:
        body_parts.append(f"- failed jobs (last {settings.scheduler_health_check_interval_hours}h): {failed_jobs}")
    if error_logs:
        body_parts.append(f"- error logs (last {settings.scheduler_health_check_interval_hours}h): {error_logs}")
    if overdue_scheduled:
        body_parts.append(f"- overdue pending scheduled tasks: {overdue_scheduled}")
    if stale_running:
        body_parts.append(f"- stale running jobs (>={settings.scheduler_running_stale_minutes}m): {stale_running}")

    if not body_parts:
        return (
            {
                "signature": signature,
                "failed_jobs": failed_jobs,
                "error_logs": error_logs,
                "overdue_scheduled": overdue_scheduled,
                "stale_running": stale_running,
                "recent_errors": [],
                "recent_failed_jobs": [],
                "window_start": window_start,
            },
            "",
        )

    body = [f"[viral-factory] scheduler health issues at {now:%Y-%m-%d %H:%M:%S UTC}", *body_parts, ""]

    if recent_errors:
        body.append("Recent error logs:")
        for ts, job_id, msg in recent_errors:
            body.append(f"- {ts} job={job_id} msg={msg}")

    if recent_failed_jobs:
        body.append("")
        body.append("Recent failed jobs:")
        for row_id, row_type, row_error in recent_failed_jobs:
            body.append(f"- id={row_id} type={row_type} error={row_error}")

    return (
        {
            "signature": signature,
            "failed_jobs": failed_jobs,
            "error_logs": error_logs,
            "overdue_scheduled": overdue_scheduled,
            "stale_running": stale_running,
            "recent_errors": recent_errors,
            "recent_failed_jobs": recent_failed_jobs,
            "window_start": window_start,
        },
        "\n".join(body),
    )


def _run_health_check() -> bool:
    global _LAST_HEALTH_ALERT_SIGNATURE
    settings = get_settings()
    if settings.telegram_enabled is False:
        logger.info("scheduler health check skipped: telegram disabled")
        return True

    metrics, message = _collect_health_metrics()
    signature = metrics["signature"]

    if not message:
        _LAST_HEALTH_ALERT_SIGNATURE = None
        logger.info(
            "scheduler health check passed (interval=%sh, no anomalies)",
            settings.scheduler_health_check_interval_hours,
        )
        return True

    if signature == _LAST_HEALTH_ALERT_SIGNATURE:
        logger.warning("scheduler health check skipped duplicate alert: %s", signature)
        return True

    if send_telegram_message(message):
        logger.warning("scheduler health alert sent: %s", signature)
        _LAST_HEALTH_ALERT_SIGNATURE = signature
        return True

    logger.error("failed to send scheduler health alert: telegram request failed")
    return False


def _cleanup_stale_discover_jobs(db, settings, now) -> int:
    cutoff = now - timedelta(minutes=settings.scheduler_running_stale_minutes)
    stale_jobs = (
        db.execute(
            select(Job)
            .where(Job.type == "discover")
            .where(Job.status.in_(("queued", "running")))
            .where(Job.updated_at <= cutoff)
        )
        .scalars()
        .all()
    )
    if not stale_jobs:
        return 0

    for stale_job in stale_jobs:
        previous_status = stale_job.status
        stale_job.status = "failed"
        stale_job.error = f"stale discover job cleaned ({settings.scheduler_running_stale_minutes}m)"
        _log(
            db,
            stale_job.id,
            "error",
            "scheduled discover job cleaned as stale",
            {
                "reason": f"stale discover job older than {settings.scheduler_running_stale_minutes} minutes",
                "previous_status": previous_status,
                "updated_at": str(stale_job.updated_at),
            },
        )

    return len(stale_jobs)


def _count_videos_for_pool(db, settings) -> int:
    q = select(func.count(Video.id))
    if settings.scheduled_discovery_region:
        q = q.where(Video.region == settings.scheduled_discovery_region)
    if settings.scheduled_discovery_language:
        q = q.where(Video.language == settings.scheduled_discovery_language)
    return int(db.scalar(q) or 0)


def _run_discover_once(
    *,
    max_results: int | None = None,
    reason: str = "scheduled",
    enforce_scheduled_flag: bool = True,
) -> bool:
    settings = get_settings()
    if enforce_scheduled_flag and not settings.scheduled_discovery_enabled:
        logger.info("scheduled discovery skipped: disabled")
        return False

    requested_max_results = settings.scheduled_discovery_max_results if max_results is None else max(1, min(max_results, settings.scheduled_discovery_max_results))
    if requested_max_results <= 0:
        logger.info("scheduled discover skipped: max_results is zero")
        return False

    payload = {
        "provider": settings.scheduled_discovery_provider,
        "query": settings.scheduled_discovery_query,
        "region": settings.scheduled_discovery_region,
        "language": settings.scheduled_discovery_language,
        "sort": settings.scheduled_discovery_sort,
        "max_results": requested_max_results,
        "reason": reason,
    }

    with get_db() as db:
        stale_cleaned = _cleanup_stale_discover_jobs(db, settings, datetime.now(tz=timezone.utc))
        if stale_cleaned:
            logger.info("scheduled discover stale jobs cleaned: %s", stale_cleaned)

        existing_job = (
            db.execute(
                select(Job)
                .where(Job.type == "discover")
                .where(Job.status.in_(("queued", "running")))
                .order_by(Job.created_at.desc())
            )
            .scalars()
            .first()
        )
        if existing_job is not None:
            logger.info(
                "scheduled discover skipped: existing job in progress (id=%s, status=%s)",
                existing_job.id,
                existing_job.status,
            )
            _log(
                db,
                existing_job.id,
                "info",
                "scheduled discover skipped",
                {
                    "reason": "existing discover job is queued or running",
                    "trigger": reason,
                },
            )
            return False

        job = Job(type="discover", status="queued")
        db.add(job)
        db.flush()
        db.commit()
        _log(
            db,
            job.id,
            "info",
            "scheduled discover enqueued",
            {**payload, "trigger": reason},
        )
        try:
            enqueue(
                task_discover_videos,
                settings.scheduled_discovery_provider,
                settings.scheduled_discovery_query,
                None,
                None,
                None,
                settings.scheduled_discovery_region,
                settings.scheduled_discovery_language,
                settings.scheduled_discovery_sort,
                None,
                None,
                requested_max_results,
                None,
                str(job.id),
            )
            logger.info("scheduled discover job enqueued: id=%s, payload=%s", job.id, payload)
            return True
        except Exception:
            job.status = "failed"
            job.error = "scheduled discover enqueue failed"
            _log(db, job.id, "error", "scheduled discover enqueue failed", {"error": traceback.format_exc(), "payload": payload})
            db.add(job)
            db.commit()
            raise


def _run_brief_once() -> bool:
    settings = get_settings()
    if not settings.scheduled_brief_enabled:
        logger.info("scheduled brief skipped: disabled")
        return False

    payload = {
        "region": settings.scheduled_brief_region,
        "language": settings.scheduled_brief_language,
        "niche": settings.scheduled_brief_niche,
        "window_days": settings.scheduled_brief_window_days,
        "analysis_level": settings.scheduled_brief_analysis_level,
        "active_video_target": settings.scheduled_brief_active_video_target,
        "analysis_min_final_score": settings.scheduled_brief_analysis_min_final_score,
        "trigger": "scheduled",
    }

    with get_db() as db:
        existing_job = (
            db.execute(
                select(Job)
                .where(Job.type == "brief")
                .where(Job.status.in_(("queued", "running")))
                .order_by(Job.created_at.desc())
            )
            .scalars()
            .first()
        )
        if existing_job is not None:
            logger.info(
                "scheduled brief skipped: existing brief job in progress (id=%s, status=%s)",
                existing_job.id,
                existing_job.status,
            )
            _log(
                db,
                existing_job.id,
                "info",
                "scheduled brief skipped",
                {"reason": "existing brief job is queued or running", "trigger": "scheduled"},
            )
            return False

        job = Job(type="brief", status="queued")
        db.add(job)
        db.flush()
        db.commit()
        _log(db, job.id, "info", "scheduled brief enqueued", payload)
        try:
            enqueue(
                task_generate_brief,
                str(job.id),
                settings.scheduled_brief_region,
                settings.scheduled_brief_language,
                settings.scheduled_brief_niche,
                settings.scheduled_brief_window_days,
                settings.scheduled_brief_analysis_level,
                settings.scheduled_brief_active_video_target,
                settings.scheduled_brief_analysis_min_final_score,
            )
            logger.info("scheduled brief job enqueued: id=%s, payload=%s", job.id, payload)
            return True
        except Exception:
            job.status = "failed"
            job.error = "scheduled brief enqueue failed"
            _log(
                db,
                job.id,
                "error",
                "scheduled brief enqueue failed",
                {"error": traceback.format_exc(), "payload": payload},
            )
            db.add(job)
            db.commit()
            raise


def _maintain_video_pool() -> bool:
    settings = get_settings()
    if not settings.video_pool_maintenance_enabled:
        logger.info("video pool maintenance skipped: disabled")
        return False

    with get_db() as db:
        current_count = _count_videos_for_pool(db, settings)
    target_count = settings.video_pool_target
    if current_count >= target_count:
        logger.info("video pool sufficient: current=%s target=%s", current_count, target_count)
        return False

    missing = target_count - current_count
    requested = min(settings.scheduled_discovery_max_results, missing)
    logger.warning(
        "video pool maintenance triggered: current=%d target=%d missing=%d request=%d",
        current_count,
        target_count,
        missing,
        requested,
    )
    return _run_discover_once(
        max_results=requested,
        reason=f"pool_maintenance_shortage_{missing}",
        enforce_scheduled_flag=False,
    )


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
            job = None
            try:
                job = Job(type="snapshot", status="queued", video_id=row.video_id)
                db.add(job)
                db.flush()
                row.status = "enqueued"
                row.updated_at = now
                db.commit()
                _log(
                    db,
                    job.id,
                    "info",
                    "snapshot task dequeued",
                    {"scheduled_task_id": str(row.id), "provider": provider, "video_id": str(row.video_id)},
                )
                enqueue(task_fetch_metrics_snapshot_with_task, str(row.video_id), provider, None, str(row.id), str(job.id))
                _log(
                    db,
                    job.id,
                    "info",
                    "snapshot task enqueued",
                    {"scheduled_task_id": str(row.id), "provider": provider, "video_id": str(row.video_id)},
                )
                enqueued += 1
            except Exception as exc:  # noqa: BLE001
                if job is not None:
                    db.add(job)
                    job.status = "failed"
                    job.error = f"{type(exc).__name__}: {exc}"
                    _log(db, job.id, "error", f"{type(exc).__name__}: {exc}", {"trace": traceback.format_exc(), "scheduled_task_id": str(row.id)})
                row.status = "pending"
                row.last_error = f"{type(exc).__name__}: {exc}"
                row.updated_at = datetime.now(tz=timezone.utc)
                db.add(row)
                db.commit()

    return enqueued


def main() -> None:
    settings = get_settings()
    health_interval_seconds = max(60, settings.scheduler_health_check_interval_hours * 3600)
    discovery_interval_seconds = max(60, settings.scheduled_discovery_interval_hours * 3600)
    brief_interval_seconds = max(60, settings.scheduled_brief_interval_hours * 3600)
    pool_maintenance_interval_seconds = max(60, settings.video_pool_maintenance_interval_minutes * 60)
    next_health_check_at = time.time()
    next_discovery_at = time.time()
    next_pool_maintenance_at = time.time()
    next_brief_at = time.time()
    while True:
        try:
            run_once()
        except Exception:
            logger.exception("scheduler iteration failed")

        now = time.time()
        if now >= next_health_check_at:
            try:
                _run_health_check()
            except Exception:
                logger.exception("health check failed")
            next_health_check_at = now + health_interval_seconds

        if settings.scheduled_discovery_enabled and now >= next_discovery_at:
            try:
                _run_discover_once()
            except Exception:
                logger.exception("scheduled discovery failed")
            next_discovery_at = now + discovery_interval_seconds

        if now >= next_brief_at:
            try:
                _run_brief_once()
            except Exception:
                logger.exception("scheduled brief failed")
            next_brief_at = now + brief_interval_seconds

        if now >= next_pool_maintenance_at:
            try:
                _maintain_video_pool()
            except Exception:
                logger.exception("video pool maintenance failed")
            next_pool_maintenance_at = now + pool_maintenance_interval_seconds
        time.sleep(60)


if __name__ == "__main__":
    main()
