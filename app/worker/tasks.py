from __future__ import annotations

import logging
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select

from app.analysis.content_features import build_content_tokens
from app.core.ids import extract_tiktok_video_id, coerce_tiktok_video_url
from app.core.brief_builder import build_brief_json
from app.core.scoring import compute_scores_for_videos
from app.core.storage import brief_filename, write_export
from app.db.models import ContentToken, CreativeBrief, Job, JobLog, MetricSnapshot, ScheduledTask, Video
from app.db.session import get_db
from app.providers.apify_provider import ApifyProvider
from app.providers.base import BaseProvider, normalize_provider, ProviderDisabledError
from app.providers.csv_provider import CsvProvider
from app.settings import get_settings


def _provider(name: str) -> BaseProvider:
    normalized = normalize_provider(name, default=get_settings().provider_default)
    if normalized == "apify":
        return ApifyProvider()
    if normalized == "csv":
        return CsvProvider()
    if normalized == "official":
        raise ProviderDisabledError("provider 'official' is not enabled in phase-1")
    raise ValueError(f"unsupported provider '{name}'")


def _safe_uuid(value: object, field: str) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"invalid {field}: {value}") from exc


def _log(db, job_id: uuid.UUID | str, level: str, message: str, meta: dict | None = None) -> None:
    try:
        job_uuid = _safe_uuid(job_id, "job_id")
    except ValueError:
        return
    if db.get(Job, job_uuid) is None:
        return
    db.add(JobLog(job_id=job_uuid, level=level, message=message, meta=meta))


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _fmt_job_error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _is_valid_tiktok_video_url(raw_url: str) -> bool:
    return extract_tiktok_video_id(raw_url or "") is not None


def _safe_parse_discovered_payload(raw: object) -> dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    return None


def _apply_discovered_metadata(video: Video, payload: dict[str, Any]) -> None:
    published_at = payload.get("published_at")
    if published_at is not None and video.published_at is None:
        video.published_at = published_at
    duration_sec = payload.get("duration_sec")
    if duration_sec is not None and video.duration_sec is None:
        video.duration_sec = duration_sec
    caption_keywords = payload.get("caption_keywords")
    if caption_keywords is not None and not video.caption_keywords:
        video.caption_keywords = caption_keywords
    hashtags = payload.get("hashtags")
    if hashtags is not None and not video.hashtags:
        video.hashtags = hashtags
    author_id = payload.get("author_id")
    if author_id is not None and video.author_id is None:
        video.author_id = author_id
    author_handle = payload.get("author_handle")
    if author_handle is not None and video.author_handle is None:
        video.author_handle = author_handle
    sound_id = payload.get("sound_id")
    if sound_id is not None and video.sound_id is None:
        video.sound_id = sound_id
    sound_title = payload.get("sound_title")
    if sound_title is not None and video.sound_title is None:
        video.sound_title = sound_title
    sound_is_original = payload.get("sound_is_original")
    if sound_is_original is not None and video.sound_is_original is None:
        video.sound_is_original = sound_is_original
    width = payload.get("width")
    if width is not None and video.width is None:
        video.width = width
    height = payload.get("height")
    if height is not None and video.height is None:
        video.height = height
    has_audio = payload.get("has_audio")
    if has_audio is not None and video.has_audio is None:
        video.has_audio = has_audio


def task_discover_videos(
    provider_name: str,
    query: str | None = None,
    creator_handle: str | None = None,
    hashtag: str | None = None,
    challenge: str | None = None,
    region: str | None = "KR",
    language: str | None = "ko",
    sort: str | None = None,
    time_window_start: str | None = None,
    time_window_end: str | None = None,
    max_results: int = 120,
    cursor: str | None = None,
    job_id: str | None = None,
) -> tuple[int, int, int, int, list[str], str | None]:
    with get_db() as db:
        job: Job | None = None
        if job_id is not None:
            try:
                job = db.get(Job, _safe_uuid(job_id, "job_id"))
            except ValueError:
                job = None
        if job is not None:
            job.status = "running"

        try:
            prov = _provider(provider_name)
            if job is not None:
                _log(
                    db,
                    job.id,
                    "info",
                    "video discovery started",
                    {"provider": provider_name, "query": query, "creator_handle": creator_handle, "hashtag": hashtag, "challenge": challenge, "max_results": max_results},
                )
            (
                discovered_count,
                imported_count,
                skipped_count,
                scheduled_count,
                imported_video_ids,
            ), discovered_cursor = _discover_videos(
                db,
                prov=prov,
                provider_name=provider_name,
                query=query,
                creator_handle=creator_handle,
                hashtag=hashtag,
                challenge=challenge,
                region=region,
                language=language,
                sort=sort,
                time_window_start=time_window_start,
                time_window_end=time_window_end,
                max_results=max_results,
                cursor=cursor,
            )
            if job is not None:
                job.status = "done"
                job.progress = 100
                _log(
                    db,
                    job.id,
                    "info",
                    "video discovery done",
                    {"provider": provider_name, "count": discovered_count, "next_cursor": discovered_cursor},
                )
            return discovered_count, imported_count, skipped_count, scheduled_count, imported_video_ids, discovered_cursor
        except ProviderDisabledError as exc:
            detail = _fmt_job_error(exc)
            if job is not None:
                job.status = "failed"
                job.error = detail
                _log(db, job.id, "error", detail)
            raise
        except Exception as exc:  # noqa: BLE001
            detail = _fmt_job_error(exc)
            if job is not None:
                job.status = "failed"
                job.error = detail
                _log(
                    db,
                    job.id,
                    "error",
                    detail,
                    {"trace": traceback.format_exc(), "provider": provider_name},
                )
            raise


def discover_videos_sync(
    db,
    prov: BaseProvider,
    provider_name: str,
    query: str | None = None,
    creator_handle: str | None = None,
    hashtag: str | None = None,
    challenge: str | None = None,
    region: str | None = None,
    language: str | None = None,
    sort: str | None = None,
    time_window_start: str | None = None,
    time_window_end: str | None = None,
    max_results: int = 120,
    cursor: str | None = None,
) -> tuple[tuple[int, int, int, int, list[str]], str | None]:
    return _discover_videos(
        db,
        prov=prov,
        provider_name=provider_name,
        query=query,
        creator_handle=creator_handle,
        hashtag=hashtag,
        challenge=challenge,
        region=region,
        language=language,
        sort=sort,
        time_window_start=time_window_start,
        time_window_end=time_window_end,
        max_results=max_results,
        cursor=cursor,
    )


def _discover_videos(
    db,
    prov: BaseProvider,
    provider_name: str,
    query: str | None = None,
    creator_handle: str | None = None,
    hashtag: str | None = None,
    challenge: str | None = None,
    region: str | None = None,
    language: str | None = None,
    sort: str | None = None,
    time_window_start: str | None = None,
    time_window_end: str | None = None,
    max_results: int = 120,
    cursor: str | None = None,
) -> tuple[tuple[int, int, int, int, list[str]], str | None]:
    if max_results <= 0:
        return (0, 0, 0, 0, []), cursor

    discovered, next_cursor = prov.discover_videos(
        db,
        query=query,
        creator_handle=creator_handle,
        hashtag=hashtag,
        challenge=challenge,
        region=region,
        language=language,
        sort=sort,
        time_window_start=time_window_start,
        time_window_end=time_window_end,
        max_results=max_results,
        cursor=cursor,
    )
    discovered_count = len(discovered)
    imported = 0
    skipped = 0
    scheduled = 0
    skip_missing_url = 0
    skip_invalid_url = 0
    skip_missing_payload = 0
    skip_other = 0
    imported_video_ids: list[str] = []
    for item in discovered:
        payload = _safe_parse_discovered_payload(item)
        if not payload:
            skipped += 1
            skip_missing_payload += 1
            continue
        url = payload.get("url")
        platform_video_id = payload.get("platform_video_id")
        if not isinstance(url, str) or not url.strip():
            skipped += 1
            skip_missing_url += 1
            continue
        normalized_url = coerce_tiktok_video_url(url, str(platform_video_id) if platform_video_id is not None else None)
        if not normalized_url:
            skipped += 1
            skip_invalid_url += 1
            continue

        try:
            video = prov.upsert_video_from_url(db, normalized_url, region=region, language=language)
        except Exception:
            skipped += 1
            skip_other += 1
            continue
        imported += 1
        imported_video_ids.append(str(video.id))
        _apply_discovered_metadata(video, payload)
        scheduled += schedule_snapshot_tasks(db, video)

    if skip_missing_payload or skip_missing_url or skip_invalid_url or skip_other:
        logger = logging.getLogger(__name__)
        logger.warning(
            "discovery import summary: discovered=%d, imported=%d, skipped=%d, skip_missing_payload=%d, skip_missing_url=%d, skip_invalid_url=%d, skip_other=%d, scheduled=%d",
            discovered_count,
            imported,
            skipped,
            skip_missing_payload,
            skip_missing_url,
            skip_invalid_url,
            skip_other,
            scheduled,
        )

    return (discovered_count, imported, skipped, scheduled, imported_video_ids), next_cursor


def upsert_snapshot(
    db,
    video: Video,
    captured_at,
    source: str,
    view_count: int | None,
    like_count: int | None,
    comment_count: int | None,
    share_count: int | None,
    bookmark_count: int | None,
    raw: dict | None = None,
) -> MetricSnapshot:
    row = db.execute(
        select(MetricSnapshot).where(
            MetricSnapshot.video_id == video.id,
            MetricSnapshot.captured_at == captured_at,
        )
    ).scalar_one_or_none()

    if row is not None:
        row.view_count = view_count if view_count is not None else row.view_count
        row.like_count = like_count if like_count is not None else row.like_count
        row.comment_count = comment_count if comment_count is not None else row.comment_count
        row.share_count = share_count if share_count is not None else row.share_count
        row.bookmark_count = bookmark_count if bookmark_count is not None else row.bookmark_count
        if raw is not None:
            row.raw = raw
        row.source = source
        return row

    row = MetricSnapshot(
        video_id=video.id,
        captured_at=captured_at,
        source=source,
        view_count=view_count,
        like_count=like_count,
        comment_count=comment_count,
        share_count=share_count,
        bookmark_count=bookmark_count,
        raw=raw,
    )
    db.add(row)
    return row


def ensure_video_with_provider(db, provider_name: str, url: str, region: str | None, language: str | None) -> Video:
    prov = _provider(provider_name)
    return prov.upsert_video_from_url(db, url, region, language)


def task_import_csv(db, batch_id: str, file_path: str) -> None:
    from pathlib import Path
    from app.providers.csv_provider import CsvProvider, _parse_ts, _to_int

    provider = CsvProvider()
    provider_data = provider.parse_csv(Path(file_path).read_bytes())
    scheduled_total = 0
    skipped = 0
    imported = 0

    for row in provider_data:
        raw_url = (row.get("url") or "").strip()
        if not raw_url:
            skipped += 1
            continue
        normalized_url = coerce_tiktok_video_url(raw_url, row.get("platform_video_id"))
        if not normalized_url:
            skipped += 1
            continue
        try:
            vid = provider.upsert_video_from_url(db, normalized_url, row.get("region"), row.get("language"))
        except Exception:
            skipped += 1
            continue
        if vid is None:
            continue
        view_count = _to_int(row.get("viewCount"))
        like_count = _to_int(row.get("likeCount"))
        comment_count = _to_int(row.get("commentCount"))
        share_count = _to_int(row.get("shareCount"))
        bookmark_count = _to_int(row.get("bookmarkCount"))
        if any(v is not None for v in [view_count, like_count, comment_count, share_count, bookmark_count]):
            captured_at = _parse_ts(row.get("capturedAt"))
            upsert_snapshot(
                db,
                video=vid,
                captured_at=captured_at,
                source="csv",
                view_count=view_count,
                like_count=like_count,
                comment_count=comment_count,
                share_count=share_count,
                bookmark_count=bookmark_count,
                raw={"provider": "csv", "batch_id": batch_id},
            )
        scheduled_total += schedule_snapshot_tasks(db, vid)
        imported += 1

    if skipped:
        logging.getLogger(__name__).warning(
            "task_import_csv summary: batch_id=%s imported=%d skipped=%d scheduled_snapshots=%d",
            batch_id,
            imported,
            skipped,
            scheduled_total,
        )
    return None


def schedule_snapshot_tasks(db, video: Video) -> int:
    if not _is_valid_tiktok_video_url(video.url):
        return 0
    now = _now()
    offsets = [0, 3600, 21600, 86400, 259200]
    count = 0
    for sec in offsets:
        due = now + timedelta(seconds=sec)
        dup = db.execute(
            select(ScheduledTask).where(
                ScheduledTask.video_id == video.id,
                ScheduledTask.task_type == "metrics_snapshot",
                ScheduledTask.due_at == due,
            )
        ).scalar_one_or_none()
        if dup:
            continue
        db.add(
            ScheduledTask(
                task_type="metrics_snapshot",
                video_id=video.id,
                due_at=due,
                status="pending",
            )
        )
        count += 1
    return count


def _active_snapshot_job(db, video_id: uuid.UUID) -> Job | None:
    return (
        db.execute(
            select(Job)
            .where(Job.type == "snapshot")
            .where(Job.video_id == video_id)
            .where(Job.status.in_(["queued", "running"]))
            .order_by(Job.created_at.desc())
        )
        .scalars()
        .first()
    )


def _get_scheduled_task(db, scheduled_task_id: str) -> ScheduledTask | None:
    try:
        parsed = _safe_uuid(scheduled_task_id, "scheduled_task_id")
    except Exception:
        return None
    return db.get(ScheduledTask, parsed)


def _is_retriable_snapshot_error(error: str) -> bool:
    normalized = (error or "").lower()
    if "invalid tiktok video url" in normalized:
        return False
    if "apify actor call failed (400" in normalized:
        if (
            "timeout" in normalized
            or "run-timeout-exceeded" in normalized
            or "too many requests" in normalized
            or "rate limit" in normalized
        ):
            return True
        return False
    if "apify actor call failed (404" in normalized:
        return False
    if "run-failed" in normalized:
        return False
    if "apify actor call failed (408" in normalized:
        return True
    if "apify actor call failed (502" in normalized:
        return True
    if "timeout" in normalized or "run-timeout-exceeded" in normalized:
        return True
    return True


def _plan_next_retry(s: ScheduledTask, error: str) -> None:
    settings = get_settings()
    s.attempts += 1
    s.last_error = error
    if not _is_retriable_snapshot_error(error):
        s.status = "failed"
        s.updated_at = _now()
        return
    if s.attempts >= settings.max_snapshot_attempts:
        s.status = "failed"
        s.updated_at = _now()
        return

    delay = max(settings.retry_base_seconds, settings.retry_base_seconds * (2 ** (s.attempts - 1)))
    delay = min(delay, settings.retry_cap_seconds)
    s.status = "pending"
    s.due_at = _now() + timedelta(seconds=delay)
    s.updated_at = _now()


def _finalize_scheduled_task(s: ScheduledTask | None, success: bool = True, error: str | None = None) -> None:
    if not s:
        return
    s.updated_at = _now()
    if success:
        s.status = "done"
        s.last_error = None
        return
    s.last_error = error
    _plan_next_retry(s, error or "unknown")


def _resolve_snapshot_job(db, job_id: str | None, video_id: uuid.UUID | None) -> Job | None:
    if job_id:
        try:
            parsed = _safe_uuid(job_id, "job_id")
        except ValueError:
            parsed = None
        if parsed is not None:
            row = db.get(Job, parsed)
            if row is not None:
                return row

    if video_id is None:
        return None
    return _active_snapshot_job(db, video_id)


def task_fetch_metrics_snapshot(
    video_id: str,
    provider_name: str,
    captured_at: datetime | None = None,
    scheduled_task_id: str | None = None,
    job_id: str | None = None,
) -> str | None:
    return _fetch_metrics_snapshot_impl(video_id, provider_name, captured_at, scheduled_task_id, job_id)


def task_fetch_metrics_snapshot_with_task(
    video_id: str,
    provider_name: str,
    captured_at: datetime | None = None,
    scheduled_task_id: str | None = None,
    job_id: str | None = None,
) -> str | None:
    return _fetch_metrics_snapshot_impl(video_id, provider_name, captured_at, scheduled_task_id, job_id)


def _fetch_metrics_snapshot_impl(
    video_id: str,
    provider_name: str,
    captured_at: datetime | None = None,
    scheduled_task_id: str | None = None,
    job_id: str | None = None,
) -> str | None:
    with get_db() as db:
        job: Job | None = None
        video_uuid: uuid.UUID | None = None
        scheduled = _get_scheduled_task(db, scheduled_task_id) if scheduled_task_id else None
        try:
            video_uuid = _safe_uuid(video_id, "video_id")
            job = _resolve_snapshot_job(db, job_id=job_id, video_id=video_uuid)
            if job is None and scheduled is not None:
                job = db.execute(
                    select(Job)
                    .where(Job.type == "snapshot")
                    .where(Job.video_id == scheduled.video_id)
                    .where(Job.status.in_(["queued", "running"]))
                    .order_by(Job.created_at.desc())
                ).scalar_one_or_none()
            if job is None:
                fallback_job = Job(type="snapshot", status="running", video_id=video_uuid)
                db.add(fallback_job)
                db.flush()
                job = fallback_job
            if job is not None:
                job.status = "running"
                _log(
                    db,
                    job.id,
                    "info",
                    "snapshot fetch started",
                    {"provider": provider_name, "scheduled_task_id": str(scheduled.id) if scheduled is not None else None, "video_id": str(video_uuid)},
                )

            vid = db.get(Video, video_uuid)
            if not vid:
                raise RuntimeError("video not found")
            if not _is_valid_tiktok_video_url(vid.url):
                raise RuntimeError("invalid tiktok video URL: must be a TikTok video URL in the form /video/<id>")

            prov = _provider(provider_name)
            p = prov.fetch_metrics(db, vid, captured_at)
            upsert_snapshot(
                db,
                video=vid,
                captured_at=p.captured_at,
                source=p.source,
                view_count=p.view_count,
                like_count=p.like_count,
                comment_count=p.comment_count,
                share_count=p.share_count,
                bookmark_count=p.bookmark_count,
                raw=p.raw,
            )
            if (
                p.view_count is None
                and p.like_count is None
                and p.comment_count is None
                and p.share_count is None
                and p.bookmark_count is None
            ):
                parse_warning = None
                if isinstance(p.raw, dict):
                    parse_warning = p.raw.get("parse_warning")
                _log(
                    db,
                    job.id if job is not None else uuid.uuid4(),
                    "warning",
                    "snapshot fetch returned no numeric metrics",
                    {
                        "video_id": str(video_uuid),
                        "provider": provider_name,
                        "parse_warning": parse_warning,
                        "scheduled_task_id": str(scheduled.id) if scheduled is not None else None,
                    },
                )

            if job is not None:
                job.status = "done"
                job.progress = 100
            _finalize_scheduled_task(scheduled, success=True)
            _log(
                db,
                job.id if job is not None else uuid.uuid4(),
                "info",
                "snapshot fetch done",
                {
                    "video_id": str(video_id),
                    "provider": provider_name,
                    "captured_at": p.captured_at.isoformat(),
                    "scheduled_task_id": str(scheduled.id) if scheduled is not None else None,
                },
            )
            return str(vid.id)
        except Exception as exc:  # noqa: BLE001
            detail = _fmt_job_error(exc)
            error_job = job
            if error_job is None and scheduled is not None:
                error_job = db.execute(
                    select(Job)
                    .where(Job.type == "snapshot")
                    .where(Job.video_id == scheduled.video_id)
                    .where(Job.status.in_(["queued", "running"]))
                    .order_by(Job.created_at.desc())
                ).scalar_one_or_none()
            if error_job is None:
                fallback_video_id = video_uuid or (scheduled.video_id if scheduled else None)
                fallback_job = Job(type="snapshot", status="failed", video_id=fallback_video_id, error=detail)
                db.add(fallback_job)
                db.flush()
                error_job = fallback_job
            else:
                error_job.status = "failed"
                error_job.error = detail
            _log(
                db,
                error_job.id,
                "error",
                detail,
                {"trace": traceback.format_exc(), "provider": provider_name, "scheduled_task_id": str(scheduled.id) if scheduled else None},
            )
            _finalize_scheduled_task(scheduled, success=False, error=detail)
            return None


def task_analyze_content(job_id: str, video_id: str) -> str | None:
    with get_db() as db:
        settings = get_settings()
        try:
            parsed_job_id = _safe_uuid(job_id, "job_id")
        except ValueError as exc:
            return None
        job = db.get(Job, parsed_job_id)
        if job is None:
            return None
        job.status = "running"
        _log(db, job.id, "info", "content analysis started", {"video_id": video_id})
        try:
            parsed_video_id = _safe_uuid(video_id, "video_id")
        except ValueError:
            job.status = "failed"
            job.error = "invalid video_id"
            return None

        vid = db.get(Video, parsed_video_id)
        if not vid:
            job.status = "failed"
            job.error = "video not found"
            return None

        if not settings.enable_content_analysis:
            job.status = "done"
            job.progress = 100
            _log(db, job.id, "info", "content analysis skipped (disabled)")
            return str(vid.id)

        try:
            tokens = build_content_tokens(vid.url)
            existing = db.get(ContentToken, vid.id)
            if existing is None:
                db.add(ContentToken(video_id=vid.id, schema_version="1.0", tokens_json=tokens))
            else:
                existing.tokens_json = tokens
                existing.updated_at = _now()
            job.status = "done"
            job.progress = 100
            _log(db, job.id, "info", "content analyzed", {"video_id": str(vid.id)})
            return str(vid.id)
        except Exception as exc:  # noqa: BLE001
            job.status = "failed"
            job.error = _fmt_job_error(exc)
            _log(db, job.id, "error", job.error, {"trace": traceback.format_exc()})
            return None


def task_compute_scores(window_days: int = 7, job_id: str | None = None) -> list[dict[str, Any]]:
    with get_db() as db:
        job: Job | None = None
        if job_id:
            try:
                job = db.get(Job, _safe_uuid(job_id, "job_id"))
            except ValueError:
                job = None
        if job is not None:
            job.status = "running"
            _log(db, job.id, "info", "compute scores started", {"window_days": window_days})

        try:
            video_ids = db.execute(select(Video.id)).scalars().all()
            rows = compute_scores_for_videos(db, list(video_ids), window_days=window_days)
            if job is not None:
                job.status = "done"
                job.progress = 100
            return rows
        except Exception as exc:  # noqa: BLE001
            if job is not None:
                job.status = "failed"
                job.error = _fmt_job_error(exc)
                _log(db, job.id, "error", job.error, {"trace": traceback.format_exc()})
            raise


def task_generate_brief(
    job_id: str,
    region: str,
    language: str,
    niche: str,
    window_days: int = 7,
    analysis_level: int | None = None,
    active_video_target: int | None = None,
    analysis_min_final_score: float | None = None,
) -> str | None:
    from datetime import date

    with get_db() as db:
        try:
            parsed_job_id = _safe_uuid(job_id, "job_id")
        except ValueError as exc:
            return None
        job = db.get(Job, parsed_job_id)
        if job is None:
            return None
        job.status = "running"
        _log(
            db,
            job.id,
            "info",
            "brief generation started",
            {
                "region": region,
                "language": language,
                "niche": niche,
                "window_days": window_days,
                "analysis_level": analysis_level,
                "active_video_target": active_video_target,
                "analysis_min_final_score": analysis_min_final_score,
            },
        )
        try:
            payload = build_brief_json(
                db,
                region=region,
                language=language,
                niche=niche,
                window_days=window_days,
                analysis_level=analysis_level,
                active_video_target=active_video_target,
                analysis_min_final_score=analysis_min_final_score,
            )
            window = payload["meta"]["window"]
            brief = CreativeBrief(
                region=region,
                language=language,
                niche=niche,
                window_start=date.fromisoformat(window["start"]),
                window_end=date.fromisoformat(window["end"]),
                brief_json=payload,
            )
            db.add(brief)
            db.flush()
            out = write_export(payload, brief_filename(brief.created_at, str(brief.id)))
            _log(
                db,
                job.id,
                "info",
                "brief generated",
                {"brief_id": str(brief.id), "path": str(out), "region": region, "language": language},
            )
            job.status = "done"
            job.progress = 100
            return str(brief.id)
        except Exception as exc:  # noqa: BLE001
            job.status = "failed"
            job.error = _fmt_job_error(exc)
            _log(db, job.id, "error", job.error, {"trace": traceback.format_exc()})
            return None
