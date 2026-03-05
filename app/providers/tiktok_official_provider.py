from __future__ import annotations

from datetime import datetime

from app.providers.base import BaseProvider, MetricPayload
from app.db.models import Video


class TikTokOfficialProvider(BaseProvider):
    name = "official"

    def upsert_video_from_url(self, session, url: str, region: str | None, language: str | None) -> Video:
        raise NotImplementedError("TikTok official API provider is phase-2 only")

    def fetch_metadata(self, session, video: Video) -> dict:
        raise NotImplementedError("TikTok official API provider is phase-2 only")

    def fetch_metrics(self, session, video: Video, captured_at=None) -> MetricPayload:
        raise NotImplementedError("TikTok official API provider is phase-2 only")
