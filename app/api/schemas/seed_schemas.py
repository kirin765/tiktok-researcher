from __future__ import annotations

from pydantic import BaseModel


class AddUrlRequest(BaseModel):
    url: str
    region: str = "KR"
    language: str = "ko"
    provider: str | None = None


class ImportCsvResponse(BaseModel):
    batch_id: str
    imported: int
    skipped: int
    scheduled_snapshots: int


class SeedDiscoverRequest(BaseModel):
    provider: str | None = "apify"
    query: str | None = None
    creator_handle: str | None = None
    hashtag: str | None = None
    challenge: str | None = None
    region: str = "KR"
    language: str = "ko"
    sort: str | None = None
    time_window_start: str | None = None
    time_window_end: str | None = None
    max_results: int = 200
    cursor: str | None = None


class SeedDiscoverResponse(BaseModel):
    mode: str
    provider: str
    discovered: int
    imported: int
    skipped: int
    scheduled_snapshots: int
    video_ids: list[str]
    next_cursor: str | None = None
    job_id: str | None = None
