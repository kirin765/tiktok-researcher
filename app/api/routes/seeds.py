from __future__ import annotations

from datetime import datetime
import uuid

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.schemas.seed_schemas import AddUrlRequest, ImportCsvResponse
from app.api.schemas.seed_schemas import SeedDiscoverRequest, SeedDiscoverResponse
from app.core.ids import normalize_tiktok_url
from app.db.models import Video
from app.db.session import get_db_session
from app.providers.apify_provider import ApifyProvider
from app.providers.base import BaseProvider, normalize_provider
from app.providers.csv_provider import CsvProvider
from app.settings import get_settings
from app.worker.tasks import _log
from app.worker.tasks import discover_videos_sync, ensure_video_with_provider, schedule_snapshot_tasks, task_discover_videos
from app.worker.queue import enqueue

router = APIRouter(prefix="/seeds", tags=["seeds"])


def _provider(name: str) -> BaseProvider:
    normalized = normalize_provider(name, default=get_settings().provider_default)
    if normalized == "apify":
        return ApifyProvider()
    if normalized == "csv":
        return CsvProvider()
    if normalized == "official":
        raise HTTPException(status_code=501, detail="provider 'official' is not enabled in phase-1")
    raise HTTPException(status_code=400, detail=f"unsupported provider '{name}'")


@router.post("/import-csv", response_model=ImportCsvResponse)
async def import_csv(
    file: UploadFile = File(...),
    provider: str = Query("csv"),
    db: Session = Depends(get_db_session),
):
    try:
        provider_name = normalize_provider(provider, default="csv")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if provider_name != "csv":
        raise HTTPException(status_code=400, detail="import-csv supports only provider=csv")

    data = await file.read()
    prov = CsvProvider()

    try:
        rows = prov.parse_csv(data)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"invalid csv: {exc}") from exc

    imported = 0
    skipped = 0
    scheduled = 0

    for row in rows:
        url = (row.get("url") or "").strip()
        if not url:
            skipped += 1
            continue
        try:
            video = prov.upsert_video_from_url(db, url, row.get("region"), row.get("language"))
        except Exception:
            skipped += 1
            continue

        # optional initial metadata and counts
        if row.get("publishedAt"):
            try:
                video.published_at = datetime.fromisoformat(row.get("publishedAt").replace("Z", "+00:00"))
            except Exception:
                pass
        if row.get("durationSec"):
            try:
                video.duration_sec = int(row.get("durationSec"))
            except Exception:
                pass

        from app.providers.csv_provider import _parse_ts, _to_int

        snap_at = _parse_ts(row.get("capturedAt"))
        counts = {
            "view_count": _to_int(row.get("viewCount")),
            "like_count": _to_int(row.get("likeCount")),
            "comment_count": _to_int(row.get("commentCount")),
            "share_count": _to_int(row.get("shareCount")),
            "bookmark_count": _to_int(row.get("bookmarkCount")),
        }
        if any(v is not None for v in counts.values()):
            from app.worker.tasks import upsert_snapshot

            upsert_snapshot(
                db,
                video=video,
                captured_at=snap_at,
                source="csv",
                view_count=counts["view_count"],
                like_count=counts["like_count"],
                comment_count=counts["comment_count"],
                share_count=counts["share_count"],
                bookmark_count=counts["bookmark_count"],
                raw={"provider": "csv"},
            )

        scheduled += schedule_snapshot_tasks(db, video)
        imported += 1

    return ImportCsvResponse(
        batch_id=str(uuid.uuid4()),
        imported=imported,
        skipped=skipped,
        scheduled_snapshots=scheduled,
    )


@router.post("/add-url")
def add_url(payload: AddUrlRequest, db: Session = Depends(get_db_session)):
    provider_name = normalize_provider(payload.provider, default=get_settings().provider_default)
    _ = _provider(provider_name)
    url = normalize_tiktok_url(payload.url)

    existing = db.execute(select(Video).where(Video.url == url)).scalar_one_or_none()
    if existing is not None:
        scheduled = schedule_snapshot_tasks(db, existing) > 0
        return {"video_id": str(existing.id), "scheduled": bool(scheduled)}

    video = ensure_video_with_provider(db, provider_name, url, payload.region, payload.language)
    scheduled = schedule_snapshot_tasks(db, video)
    return {"video_id": str(video.id), "scheduled": bool(scheduled)}


@router.post("/discover", response_model=SeedDiscoverResponse)
def discover(payload: SeedDiscoverRequest, db: Session = Depends(get_db_session)):
    provider_name = normalize_provider(payload.provider, default=get_settings().provider_default)
    if not any((payload.query, payload.creator_handle, payload.hashtag, payload.challenge)):
        raise HTTPException(status_code=400, detail="one of query, creator_handle, hashtag, challenge is required")
    if payload.max_results <= 0:
        raise HTTPException(status_code=400, detail="max_results must be positive")

    provider = _provider(provider_name)
    if not hasattr(provider, "discover_videos"):
        raise HTTPException(status_code=501, detail=f"provider '{provider_name}' does not support discovery")

    settings = get_settings()
    requested_max_results = max(1, min(payload.max_results, 200))
    sync_mode = requested_max_results <= settings.discovery_sync_max_results

    if not sync_mode:
        from app.db.models import Job
        job = Job(type="discover", status="queued")
        db.add(job)
        db.flush()
        db.commit()
        db.refresh(job)
        _log(
            db,
            job.id,
            "info",
            "seed discover enqueued",
            {
                "provider": provider_name,
                "query": payload.query,
                "creator_handle": payload.creator_handle,
                "hashtag": payload.hashtag,
                "challenge": payload.challenge,
                "max_results": requested_max_results,
                "cursor": payload.cursor,
            },
        )

        enqueue(
            task_discover_videos,
            provider_name,
            payload.query,
            payload.creator_handle,
            payload.hashtag,
            payload.challenge,
            payload.region,
            payload.language,
            payload.sort,
            payload.time_window_start,
            payload.time_window_end,
            requested_max_results,
            payload.cursor,
            str(job.id),
        )

        return SeedDiscoverResponse(
            mode="async",
            provider=provider_name,
            discovered=0,
            imported=0,
            skipped=0,
            scheduled_snapshots=0,
            video_ids=[],
            job_id=str(job.id),
        )

    (
        discovered,
        imported,
        skipped,
        scheduled,
        imported_video_ids,
    ), _next_cursor = discover_videos_sync(
        db,
        prov=provider,
        provider_name=provider_name,
        query=payload.query,
        creator_handle=payload.creator_handle,
        hashtag=payload.hashtag,
        challenge=payload.challenge,
        region=payload.region,
        language=payload.language,
        sort=payload.sort,
        time_window_start=payload.time_window_start,
        time_window_end=payload.time_window_end,
        max_results=requested_max_results,
        cursor=payload.cursor,
    )
    return SeedDiscoverResponse(
        mode="sync",
        provider=provider_name,
        discovered=discovered,
        imported=imported,
        skipped=skipped,
        scheduled_snapshots=scheduled,
        video_ids=imported_video_ids,
        next_cursor=_next_cursor,
    )
