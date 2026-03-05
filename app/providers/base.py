from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import Video


@dataclass
class MetricPayload:
    captured_at: datetime
    source: str
    view_count: int | None
    like_count: int | None
    comment_count: int | None
    share_count: int | None
    bookmark_count: int | None
    raw: dict | None = None


ALLOWED_PROVIDERS = {"apify", "csv", "official"}


def normalize_provider(raw: str | None, default: str = "apify") -> str:
    value = (raw or default).strip().lower()
    if not value:
        return default
    if value not in ALLOWED_PROVIDERS:
        raise ValueError(f"unsupported provider '{value}', expected one of: {', '.join(sorted(ALLOWED_PROVIDERS))}")
    return value


class ProviderDisabledError(ValueError):
    """Raised when provider is configured as disabled or not part of phase-1."""


class BaseProvider(ABC):
    name = "base"

    @abstractmethod
    def upsert_video_from_url(self, session: Session, url: str, region: str | None, language: str | None) -> Video:
        raise NotImplementedError

    @abstractmethod
    def fetch_metadata(self, session: Session, video: Video) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def fetch_metrics(self, session: Session, video: Video, captured_at=None) -> MetricPayload:
        raise NotImplementedError

    def fetch_snapshot(self, session: Session, video: Video, captured_at=None) -> MetricPayload:
        return self.fetch_metrics(session, video, captured_at=captured_at)

    def discover_videos(
        self,
        session: Session,
        query: str | None = None,
        creator_handle: str | None = None,
        hashtag: str | None = None,
        challenge: str | None = None,
        region: str | None = None,
        language: str | None = None,
        sort: str | None = None,
        time_window_start: str | None = None,
        time_window_end: str | None = None,
        max_results: int = 100,
        cursor: str | None = None,
        **_kwargs: object,
    ) -> tuple[list[dict[str, Any]], str | None]:
        raise NotImplementedError(f"discovery is not implemented in provider '{self.name}'")
