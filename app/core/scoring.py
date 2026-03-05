from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
import statistics
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import MetricSnapshot


def _safe_ratio(n: int | None, d: int | None) -> float:
    return float(n or 0) / max(float(d or 0), 1.0)


def _nearest_after(snapshots: list[MetricSnapshot], target: datetime) -> MetricSnapshot | None:
    picked = None
    for snapshot in snapshots:
        if snapshot.captured_at >= target:
            picked = snapshot
            break
    return picked


def _zscore(value: float | None, values: list[float]) -> float:
    if value is None:
        return 0.0
    if not values:
        return 0.0
    mean = statistics.mean(values)
    st = statistics.pstdev(values)
    if st == 0:
        return 0.0
    return (value - mean) / st


@dataclass
class ScoreRow:
    video_id: str
    pop_score: float | None
    snapshot_0h: dict[str, Any]
    snapshot_24h: dict[str, Any]
    delta_views_24h: int | None
    share_rate_24h: float | None
    save_rate_24h: float | None
    like_rate_24h: float | None
    comment_rate_24h: float | None


def compute_score_from_snapshots(snapshots: list[MetricSnapshot]) -> ScoreRow | None:
    if not snapshots:
        return None

    snapshots = sorted(snapshots, key=lambda s: s.captured_at)
    t0 = snapshots[0]
    target = t0.captured_at + timedelta(hours=24)
    t24 = _nearest_after(snapshots[1:], target) or snapshots[-1]

    d0 = t0.view_count or 0
    d24 = t24.view_count or 0
    delta_views = max(d24 - d0, 0)

    share_rate = _safe_ratio(t24.share_count, t24.view_count)
    save_rate = _safe_ratio(t24.bookmark_count, t24.view_count)
    like_rate = _safe_ratio(t24.like_count, t24.view_count)
    comment_rate = _safe_ratio(t24.comment_count, t24.view_count)

    return ScoreRow(
        video_id=str(t0.video_id),
        pop_score=None,
        snapshot_0h={
            "view": t0.view_count,
            "like": t0.like_count,
            "share": t0.share_count,
            "save": t0.bookmark_count,
            "comment": t0.comment_count,
        },
        snapshot_24h={
            "view": t24.view_count,
            "like": t24.like_count,
            "share": t24.share_count,
            "save": t24.bookmark_count,
            "comment": t24.comment_count,
        },
        delta_views_24h=delta_views,
        share_rate_24h=share_rate,
        save_rate_24h=save_rate,
        like_rate_24h=like_rate,
        comment_rate_24h=comment_rate,
    )


def compute_scores_for_videos(db: Session, video_ids: list, window_days: int = 7) -> list[dict]:
    rows: list[ScoreRow] = []
    threshold = datetime.now(tz=timezone.utc) - timedelta(days=window_days)

    for video_id in video_ids:
        snaps = (
            db.execute(
                select(MetricSnapshot)
                .where(MetricSnapshot.video_id == video_id)
                .where(MetricSnapshot.captured_at >= threshold)
                .order_by(MetricSnapshot.captured_at)
            )
            .scalars()
            .all()
        )
        score = compute_score_from_snapshots(list(snaps))
        if score:
            rows.append(score)

    x1s = [math.log1p(r.delta_views_24h or 0) if r.delta_views_24h is not None else 0.0 for r in rows]
    x2s = [r.share_rate_24h or 0.0 for r in rows]
    x3s = [r.save_rate_24h or 0.0 for r in rows]
    x4s = [r.comment_rate_24h or 0.0 for r in rows]

    out = []
    for r in rows:
        z1 = _zscore(math.log1p(r.delta_views_24h or 0), x1s)
        z2 = _zscore(r.share_rate_24h or 0.0, x2s)
        z3 = _zscore(r.save_rate_24h or 0.0, x3s)
        z4 = _zscore(r.comment_rate_24h or 0.0, x4s)
        pop = 0.45 * z1 + 0.25 * z2 + 0.20 * z3 + 0.10 * z4
        out.append(
            {
                "video_id": r.video_id,
                "pop_score": pop,
                "snapshot_0h": r.snapshot_0h,
                "snapshot_24h": r.snapshot_24h,
                "delta_views_24h": r.delta_views_24h,
                "share_rate_24h": r.share_rate_24h,
                "save_rate_24h": r.save_rate_24h,
                "like_rate_24h": r.like_rate_24h,
                "comment_rate_24h": r.comment_rate_24h,
            }
        )
    return out
