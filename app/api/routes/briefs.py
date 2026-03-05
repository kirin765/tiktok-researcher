from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import CreativeBrief
from app.db.session import get_db_session
from app.core.brief_builder import build_brief_json, persist_brief
from app.core.storage import brief_filename, write_export

router = APIRouter(prefix="/briefs", tags=["briefs"])


@router.get("")
def list_briefs(region: str | None = None, language: str | None = None, niche: str | None = None, limit: int = 10, db: Session = Depends(get_db_session)):
    q = select(CreativeBrief).order_by(CreativeBrief.created_at.desc()).limit(limit)
    if region:
        q = q.where(CreativeBrief.region == region)
    if language:
        q = q.where(CreativeBrief.language == language)
    if niche:
        q = q.where(CreativeBrief.niche == niche)
    rows = db.execute(q).scalars().all()
    return [
        {
            "id": str(r.id),
            "region": r.region,
            "language": r.language,
            "niche": r.niche,
            "window_start": str(r.window_start),
            "window_end": str(r.window_end),
            "created_at": r.created_at,
        }
        for r in rows
    ]


@router.get("/{brief_id}")
def get_brief(brief_id: UUID, db: Session = Depends(get_db_session)):
    row = db.get(CreativeBrief, brief_id)
    if row is None:
        raise HTTPException(status_code=404, detail="brief not found")
    return row.brief_json


@router.get("/{brief_id}/export")
def export_brief(brief_id: UUID, db: Session = Depends(get_db_session)):
    row = db.get(CreativeBrief, brief_id)
    if row is None:
        raise HTTPException(status_code=404, detail="brief not found")
    filename = brief_filename(row.created_at, str(row.id))
    path = write_export(row.brief_json, filename)
    return FileResponse(path, media_type="application/json", filename=filename)


@router.post("/generate")
def generate(payload: dict, db: Session = Depends(get_db_session)):
    for key in ("region", "language", "niche"):
        if not payload.get(key):
            raise HTTPException(status_code=400, detail=f"{key} required")

    window_days = int(payload.get("window_days", 7))
    row = persist_brief(db, payload["region"], payload["language"], payload["niche"], window_days)
    db.flush()
    return {"id": str(row.id)}
