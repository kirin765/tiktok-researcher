from __future__ import annotations

from pydantic import BaseModel


class FetchSnapshotRequest(BaseModel):
    video_id: str
    provider: str = "apify"
    captured_at: str | None = None


class AnalyzeContentRequest(BaseModel):
    video_id: str


class GenerateBriefRequest(BaseModel):
    region: str
    language: str
    niche: str
    window_days: int = 7


class ComputeScoresRequest(BaseModel):
    window_days: int = 7


class JobCreateRequest(BaseModel):
    video_id: str
    provider: str = "apify"


class JobCreateResponse(BaseModel):
    job_id: str
