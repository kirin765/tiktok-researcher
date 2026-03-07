from __future__ import annotations

import csv
from datetime import datetime, timezone
from io import StringIO

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.ids import extract_tiktok_video_id, normalize_tiktok_url
from app.db.models import Video
from app.providers.base import BaseProvider, MetricPayload


def _to_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        try:
            return int(float(s))
        except ValueError:
            return None


def _parse_ts(raw: str | None):
    if not raw:
        return datetime.now(tz=timezone.utc)
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except Exception:
        return datetime.now(tz=timezone.utc)


class CsvProvider(BaseProvider):
    name = "csv"

    def upsert_video_from_url(self, session: Session, url: str, region: str | None, language: str | None) -> Video:
        normalized = normalize_tiktok_url(url)
        platform_video_id = extract_tiktok_video_id(normalized)
        if platform_video_id is None:
            raise ValueError(f"invalid tiktok video URL: {url}")
        existing = session.execute(select(Video).where(Video.url == normalized)).scalar_one_or_none()
        if existing:
            if region and not existing.region:
                existing.region = region
            if language and not existing.language:
                existing.language = language
            return existing

        vid = Video(
            platform="tiktok",
            url=normalized,
            platform_video_id=platform_video_id,
            region=region,
            language=language,
            caption_keywords=[],
            hashtags=[],
        )
        session.add(vid)
        session.flush()
        return vid

    def fetch_metadata(self, session: Session, video: Video) -> dict[str, str | None]:
        return {"platform": video.platform, "platform_video_id": video.platform_video_id}

    def fetch_metrics(self, session: Session, video: Video, captured_at=None) -> MetricPayload:
        now = captured_at or datetime.now(tz=timezone.utc)
        return MetricPayload(
            captured_at=now,
            source=self.name,
            view_count=None,
            like_count=None,
            comment_count=None,
            share_count=None,
            bookmark_count=None,
            raw={"provider": self.name},
        )

    def parse_csv(self, content: bytes) -> list[dict]:
        text = content.decode("utf-8-sig")
        return list(csv.DictReader(StringIO(text)))
