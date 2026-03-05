from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
import requests

from app.core.ids import extract_tiktok_video_id, normalize_tiktok_url
from app.db.models import Video
from app.providers.base import BaseProvider, MetricPayload
from app.settings import get_settings


class ApifyProvider(BaseProvider):
    name = "apify"

    def upsert_video_from_url(self, session, url: str, region: str | None, language: str | None) -> Video:
        normalized = normalize_tiktok_url(url)
        existing = session.execute(select(Video).where(Video.url == normalized)).scalar_one_or_none()
        if existing:
            if region and not existing.region:
                existing.region = region
            if language and not existing.language:
                existing.language = language
            if not existing.platform_video_id:
                existing.platform_video_id = extract_tiktok_video_id(normalized)
            return existing

        row = Video(
            platform="tiktok",
            url=normalized,
            platform_video_id=extract_tiktok_video_id(normalized),
            region=region,
            language=language,
            caption_keywords=[],
            hashtags=[],
        )
        session.add(row)
        session.flush()
        return row

    def fetch_metadata(self, session, video: Video) -> dict[str, Any]:
        del session
        payload = self._run_actor(video)
        item = self._pick_item(payload)
        if not item:
            return {"platform": video.platform, "platform_video_id": video.platform_video_id}

        author = item.get("author", {})
        sound = item.get("sound")
        sound_payload = {
            "id": self._safe_str(sound.get("id") if isinstance(sound, dict) else item.get("soundId")),
            "title": self._safe_str(sound.get("title") if isinstance(sound, dict) else item.get("soundTitle")),
            "is_original": self._safe_bool(sound.get("isOriginal") if isinstance(sound, dict) else item.get("soundIsOriginal")),
        }

        return {
            "platform": video.platform,
            "platform_video_id": video.platform_video_id or self._safe_str(item.get("id")),
            "author_id": self._safe_str(item.get("authorId") or author.get("id") or item.get("author_id")),
            "author_handle": self._safe_str(item.get("authorUsername") or author.get("username") or author.get("uniqueId")),
            "published_at": self._safe_timestamp(item.get("createTime") or item.get("createdAt") or item.get("publishedAt")),
            "duration_sec": self._safe_int(item.get("durationSec") or item.get("duration")),
            "sound_id": sound_payload["id"],
            "sound_title": sound_payload["title"],
            "sound_is_original": sound_payload["is_original"],
            "caption_keywords": self._safe_list(item.get("caption_keywords") or item.get("caption_keywords_list") or item.get("hashtags")),
            "hashtags": self._safe_list(item.get("hashtags")),
        }

    def fetch_metrics(self, session, video: Video, captured_at=None) -> MetricPayload:
        del session
        payload = self._run_actor(video)
        item = self._pick_item(payload)
        now = captured_at or datetime.now(tz=timezone.utc)
        if not item:
            return MetricPayload(
                captured_at=now,
                source=self.name,
                view_count=None,
                like_count=None,
                comment_count=None,
                share_count=None,
                bookmark_count=None,
                raw={"provider": self.name, "actor_payload": []},
            )

        stats = item.get("stats", {})
        raw = {"provider": self.name, "item": item}
        return MetricPayload(
            captured_at=now,
            source=self.name,
            view_count=self._safe_int(stats.get("playCount") or item.get("viewCount") or item.get("views")),
            like_count=self._safe_int(stats.get("diggCount") or item.get("likeCount")),
            comment_count=self._safe_int(stats.get("commentCount")),
            share_count=self._safe_int(stats.get("shareCount")),
            bookmark_count=self._safe_int(stats.get("collectCount") or item.get("saveCount") or item.get("bookmarkCount")),
            raw=raw,
        )

    def fetch_snapshot(self, session, video: Video, captured_at=None) -> MetricPayload:
        return self.fetch_metrics(session, video, captured_at=captured_at)

    def _run_actor(self, video: Video) -> list[dict[str, Any]]:
        settings = get_settings()
        token = settings.apify_token
        if not token:
            raise RuntimeError("APIFY_TOKEN is required for apify provider")

        actor_id = settings.apify_actor_id
        timeout = settings.apify_actor_timeout

        response = requests.post(
            self._build_actor_url(actor_id, token),
            json={"startUrls": [{"url": video.url}], "proxyConfiguration": {"useApifyProxy": True}, "maxRequestRetries": 2},
            timeout=timeout,
        )
        response.raise_for_status()

        payload = response.json()
        if isinstance(payload, dict):
            if payload.get("error"):
                raise RuntimeError(str(payload.get("error")))
            items = payload.get("items", [])
            if isinstance(items, list):
                return self._as_dict_list(items)
            if isinstance(payload.get("data"), list):
                return self._as_dict_list(payload["data"])
            return []
        if isinstance(payload, list):
            return self._as_dict_list(payload)
        return []

    @staticmethod
    def _build_actor_url(actor_id: str, token: str) -> str:
        return f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items?token={token}"

    @staticmethod
    def _pick_item(payload: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not payload:
            return None
        return payload[0]

    @staticmethod
    def _as_dict_list(items: object) -> list[dict[str, Any]]:
        if not isinstance(items, list):
            return []
        out: list[dict[str, Any]] = []
        for item in items:
            if isinstance(item, dict):
                out.append(item)
        return out

    @staticmethod
    def _safe_int(raw: Any) -> int | None:
        if raw is None:
            return None
        try:
            return int(float(raw))
        except Exception:
            return None

    @staticmethod
    def _safe_str(raw: Any) -> str | None:
        if raw is None:
            return None
        value = str(raw).strip()
        return value or None

    @staticmethod
    def _safe_bool(raw: Any) -> bool | None:
        if raw is None:
            return None
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            normalized = raw.strip().lower()
            if normalized in {"1", "true", "yes", "on", "y"}:
                return True
            if normalized in {"0", "false", "no", "off", "n"}:
                return False
        return bool(raw)

    @staticmethod
    def _safe_timestamp(raw: Any) -> datetime | None:
        if raw is None:
            return None
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except Exception:
            return None

    @staticmethod
    def _safe_list(raw: Any) -> list:
        if raw is None:
            return []
        if isinstance(raw, list):
            return [str(item).strip() for item in raw if str(item).strip()]
        if isinstance(raw, tuple):
            return [str(item).strip() for item in raw if str(item).strip()]
        if isinstance(raw, str):
            return [x.strip() for x in raw.split(",") if x.strip()]
        return []
