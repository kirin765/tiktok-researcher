from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.scoring import compute_scores_for_videos
from app.db.models import ContentToken, MetricSnapshot, Video
from app.db.session import get_db_session

router = APIRouter(prefix="/videos", tags=["videos"])


@router.get("")
def list_videos(
    sort: str = "pop_score",
    window_days: int = 7,
    limit: int = 100,
    region: str | None = Query(default=None),
    language: str | None = Query(default=None),
    db: Session = Depends(get_db_session),
):
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be positive")
    q = select(Video).order_by(Video.created_at.desc())
    if region:
        q = q.where(Video.region == region)
    if language:
        q = q.where(Video.language == language)
    rows = db.execute(q.limit(limit * 4)).scalars().all()

    score_map = {s["video_id"]: s["pop_score"] for s in compute_scores_for_videos(db, [r.id for r in rows], window_days=window_days)}

    payload: list[dict] = []
    for row in rows:
        payload.append(
            {
                "id": str(row.id),
                "url": row.url,
                "region": row.region,
                "language": row.language,
                "platform": row.platform,
                "platform_video_id": row.platform_video_id,
                "published_at": row.published_at,
                "pop_score": score_map.get(str(row.id)),
            }
        )

    if sort == "pop_score":
        payload.sort(key=lambda x: (x["pop_score"] is None, x["pop_score"]), reverse=True)
    elif sort != "created_at":
        raise HTTPException(status_code=400, detail="unsupported sort")
    return payload[:limit]


@router.get("/{video_id}")
def get_video(video_id: UUID, db: Session = Depends(get_db_session)):
    row = db.get(Video, video_id)
    if row is None:
        raise HTTPException(status_code=404, detail="video not found")
    return {
        "id": str(row.id),
        "url": row.url,
        "platform": row.platform,
        "platform_video_id": row.platform_video_id,
        "author_id": row.author_id,
        "author_handle": row.author_handle,
        "published_at": row.published_at,
        "duration_sec": row.duration_sec,
        "caption_keywords": row.caption_keywords,
        "hashtags": row.hashtags,
        "sound_title": row.sound_title,
        "sound_is_original": row.sound_is_original,
        "width": row.width,
        "height": row.height,
        "has_audio": row.has_audio,
    }


@router.get("/{video_id}/snapshots")
def list_snapshots(video_id: UUID, db: Session = Depends(get_db_session)):
    rows = (
        db.execute(
            select(MetricSnapshot)
            .where(MetricSnapshot.video_id == video_id)
            .order_by(MetricSnapshot.captured_at.desc())
        )
        .scalars()
        .all()
    )
    return [
        {
            "id": r.id,
            "captured_at": r.captured_at,
            "source": r.source,
            "view_count": r.view_count,
            "like_count": r.like_count,
            "comment_count": r.comment_count,
            "share_count": r.share_count,
            "bookmark_count": r.bookmark_count,
        }
        for r in rows
    ]


@router.get("/{video_id}/tokens")
def get_tokens(video_id: UUID, db: Session = Depends(get_db_session)):
    token = db.get(ContentToken, video_id)
    if token is None:
        raise HTTPException(status_code=404, detail="content tokens not found")
    return token.tokens_json
