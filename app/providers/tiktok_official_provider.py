from __future__ import annotations

from datetime import datetime

from app.providers.base import BaseProvider, MetricPayload
from app.db.models import Video


class TikTokOfficialProvider(BaseProvider):
    name = "official"

    def discover_videos(
        self,
        session,
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
    ) -> tuple[list[dict], str | None]:
        del session, query, creator_handle, hashtag, challenge, region, language, sort, time_window_start, time_window_end, max_results, cursor
        raise NotImplementedError("TikTok official API provider is phase-2 only")

    def upsert_video_from_url(self, session, url: str, region: str | None, language: str | None) -> Video:
        raise NotImplementedError("TikTok official API provider is phase-2 only")

    def fetch_metadata(self, session, video: Video) -> dict:
        raise NotImplementedError("TikTok official API provider is phase-2 only")

    def fetch_metrics(self, session, video: Video, captured_at=None) -> MetricPayload:
        raise NotImplementedError("TikTok official API provider is phase-2 only")
