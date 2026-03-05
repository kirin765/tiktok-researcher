from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import MetricSnapshot
from app.db.session import get_db_session

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get("/videos")
def top_stats(db: Session = Depends(get_db_session)):
    rows = db.execute(
        select(
            MetricSnapshot.video_id,
            func.count(MetricSnapshot.id).label("snap_count"),
            func.max(MetricSnapshot.view_count).label("max_view"),
        ).group_by(MetricSnapshot.video_id)
    ).all()
    return [
        {
            "video_id": str(r.video_id),
            "snap_count": r.snap_count,
            "max_view": r.max_view,
        }
        for r in rows
    ]
