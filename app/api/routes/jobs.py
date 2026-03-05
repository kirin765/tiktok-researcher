from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.schemas.job_schemas import (
    AnalyzeContentRequest,
    ComputeScoresRequest,
    FetchSnapshotRequest,
    GenerateBriefRequest,
    JobCreateResponse,
)
from app.db.models import Job, JobLog
from app.db.session import get_db_session
from app.providers.base import normalize_provider
from app.worker.queue import enqueue
from app.worker.tasks import (
    task_analyze_content,
    task_compute_scores,
    task_fetch_metrics_snapshot,
    task_generate_brief,
)

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid captured_at") from exc


def _parse_uuid(value: str, field: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid {field}") from exc


def _resolve_provider(provider: str) -> str:
    try:
        return normalize_provider(provider)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/fetch-snapshot", response_model=JobCreateResponse)
def fetch_snapshot(payload: FetchSnapshotRequest, db: Session = Depends(get_db_session)):
    video_uuid = _parse_uuid(payload.video_id, "video_id")
    captured_at = _parse_datetime(payload.captured_at)
    provider = _resolve_provider(payload.provider)

    existing = (
        db.execute(
            select(Job)
            .where(Job.type == "snapshot")
            .where(Job.video_id == video_uuid)
            .where(Job.status.in_(["queued", "running"]))
            .order_by(Job.created_at.desc())
        )
        .scalars()
        .first()
    )
    if existing:
        return JobCreateResponse(job_id=str(existing.id))

    job = Job(type="snapshot", status="queued", video_id=video_uuid)
    db.add(job)
    db.flush()
    db.commit()
    enqueue(task_fetch_metrics_snapshot, str(video_uuid), provider, captured_at, None, str(job.id))
    return JobCreateResponse(job_id=str(job.id))


@router.post("/analyze-content", response_model=JobCreateResponse)
def analyze_content(payload: AnalyzeContentRequest, db: Session = Depends(get_db_session)):
    video_uuid = _parse_uuid(payload.video_id, "video_id")
    job = Job(type="analyze", status="queued", video_id=video_uuid)
    db.add(job)
    db.flush()
    db.commit()
    enqueue(task_analyze_content, str(job.id), str(video_uuid))
    return JobCreateResponse(job_id=str(job.id))


@router.post("/generate-brief", response_model=JobCreateResponse)
def generate_brief(payload: GenerateBriefRequest, db: Session = Depends(get_db_session)):
    job = Job(type="brief", status="queued")
    db.add(job)
    db.flush()
    db.commit()
    enqueue(
        task_generate_brief,
        str(job.id),
        payload.region,
        payload.language,
        payload.niche,
        payload.window_days,
    )
    return JobCreateResponse(job_id=str(job.id))


@router.post("/compute-scores", response_model=JobCreateResponse)
def compute_scores(payload: ComputeScoresRequest, db: Session = Depends(get_db_session)):
    job = Job(type="compute_scores", status="queued")
    db.add(job)
    db.flush()
    db.commit()
    enqueue(task_compute_scores, payload.window_days, str(job.id))
    return JobCreateResponse(job_id=str(job.id))


@router.get("")
def list_jobs(status: str | None = None, limit: int = 50, db: Session = Depends(get_db_session)):
    q = select(Job).order_by(Job.created_at.desc()).limit(limit)
    if status:
        q = q.where(Job.status == status).order_by(Job.created_at.desc())
    rows = db.execute(q).scalars().all()
    return [
        {
            "id": str(r.id),
            "type": r.type,
            "status": r.status,
            "progress": r.progress,
            "video_id": str(r.video_id) if r.video_id else None,
            "error": r.error,
            "created_at": r.created_at,
            "updated_at": r.updated_at,
        }
        for r in rows
    ]


@router.get("/{job_id}")
def get_job(job_id: str, db: Session = Depends(get_db_session)):
    row = db.get(Job, _parse_uuid(job_id, "job_id"))
    if row is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {
        "id": str(row.id),
        "type": row.type,
        "status": row.status,
        "progress": row.progress,
        "video_id": str(row.video_id) if row.video_id else None,
        "error": row.error,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


@router.get("/{job_id}/logs")
def job_logs(job_id: str, tail: int = 200, db: Session = Depends(get_db_session)):
    parsed_job_id = _parse_uuid(job_id, "job_id")
    rows = (
        db.execute(
            select(JobLog)
            .where(JobLog.job_id == parsed_job_id)
            .order_by(JobLog.ts.desc())
            .limit(tail)
        )
        .scalars()
        .all()
    )
    return [
        {
            "id": r.id,
            "ts": r.ts,
            "level": r.level,
            "message": r.message,
            "meta": r.meta,
        }
        for r in rows
    ]
