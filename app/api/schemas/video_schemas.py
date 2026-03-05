from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class SnapshotResponse(BaseModel):
    id: int
    captured_at: datetime
    source: str
    view_count: int | None = None
    like_count: int | None = None
    comment_count: int | None = None
    share_count: int | None = None
    bookmark_count: int | None = None


class VideoListItem(BaseModel):
    id: UUID
    url: str
    region: str | None = None
    language: str | None = None
    platform: str
    platform_video_id: str | None
    published_at: datetime | None
    pop_score: float | None
