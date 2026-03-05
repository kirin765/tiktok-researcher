from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import random
import threading
import time
from typing import Any

from sqlalchemy import select
import requests

from app.core.ids import extract_tiktok_video_id, normalize_tiktok_url
from app.db.models import Video
from app.providers.base import BaseProvider, MetricPayload
from app.settings import get_settings


class ApifyProvider(BaseProvider):
    name = "apify"
    _thread_gate: threading.BoundedSemaphore | None = None
    _thread_gate_limit: int = 1

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
    ) -> tuple[list[dict[str, Any]], str | None]:
        del session
        if max_results <= 0:
            return [], None
        settings = get_settings()

        payload = {
            "resultsPerPage": max_results,
        }
        if query:
            payload["searchQueries"] = [query.strip()]
            payload["searchSection"] = "/video"
        if hashtag:
            payload["hashtags"] = [self._clean_keyword(hashtag)]
        if challenge:
            payload["hashtags"] = [self._clean_keyword(challenge)]
        if creator_handle:
            payload["profiles"] = [self._clean_keyword(creator_handle)]
        if sort:
            normalized_sort = self._normalize_search_sort(sort)
            if normalized_sort is not None:
                payload["searchSorting"] = normalized_sort
            else:
                if time_window_start or time_window_end:
                    raise ValueError(f"unsupported sort value for apify discovery: {sort}")
        if time_window_start or time_window_end:
            # clockworks tiktok-scraper does not expose a direct free-form range in this schema
            # keep explicit input for future compatibility and avoid silent behavior drift
            pass
        if cursor:
            # first-party actor does not currently expose cursor-based pagination for the used inputs
            pass
        if region:
            # schema does not include region/language; keep explicit no-op to avoid accidental drift
            pass

        items, raw = self._run_actor_payload(
            self._sanitize_payload(payload),
            actor_id=settings.apify_discovery_actor_id,
            timeout=settings.apify_discovery_timeout,
        )
        cursor = None
        if isinstance(raw, dict):
            if raw.get("nextCursor"):
                cursor = str(raw.get("nextCursor"))
        out: list[dict[str, Any]] = []

        for item in items:
            normalized = self._normalize_discovered_item(item)
            if normalized:
                out.append(normalized)

        return out, str(cursor) if cursor is not None else None

    @contextmanager
    def _apify_slot(self):
        settings = get_settings()
        limit = max(1, settings.apify_max_concurrent_requests)
        timeout = max(1, settings.apify_actor_timeout)
        lock_connection = self._redis_connection()

        if lock_connection is None:
            if self._thread_gate is None or self._thread_gate_limit != limit:
                self._thread_gate = threading.BoundedSemaphore(limit)
                self._thread_gate_limit = limit
            self._thread_gate.acquire()
            try:
                yield
                return
            finally:
                self._thread_gate.release()

        acquired = None
        slot_indexes = list(range(limit))
        while acquired is None:
            random.shuffle(slot_indexes)
            for idx in slot_indexes:
                lock = lock_connection.lock(
                    f"apify:request-slot:{idx}",
                    timeout=timeout,
                    blocking=False,
                )
                if lock.acquire(blocking=False):
                    acquired = lock
                    break
            if acquired is None:
                time.sleep(0.05)

        try:
            yield
        finally:
            if acquired is not None:
                acquired.release()

    @staticmethod
    def _clean_keyword(value: str) -> str:
        return value.strip().lstrip("@").lstrip("#")

    @staticmethod
    def _normalize_search_sort(value: str) -> str | None:
        normalized = value.strip().lower()
        if normalized in {"0", "relevance", "top"}:
            return "0"
        if normalized in {"1", "latest", "newest", "latest"}:
            return "1"
        if normalized in {"3", "popular"}:
            return "3"
        return None

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
        payload = {
            "postURLs": [video.url],
            "proxyConfiguration": {"useApifyProxy": True},
            "maxRequestRetries": 2,
        }
        return self._run_actor_payload(payload, actor_id=None, timeout=None)[0]

    def _run_actor_payload(
        self,
        payload: dict[str, Any],
        *,
        actor_id: str | None = None,
        timeout: int | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        settings = get_settings()
        token = settings.apify_token
        if not token:
            raise RuntimeError("APIFY_TOKEN is required for apify provider")

        resolved_actor_id = actor_id or settings.apify_actor_id
        resolved_timeout = timeout if timeout is not None else settings.apify_actor_timeout

        with self._apify_slot():
            response = requests.post(
                self._build_actor_url(resolved_actor_id, token),
                json=self._sanitize_payload(payload),
                timeout=resolved_timeout,
            )
        if not response.ok:
            try:
                body = response.json()
            except Exception:
                body = response.text
            if isinstance(body, dict):
                error = body.get("error") or body.get("message") or body
            else:
                error = body
            raise RuntimeError(f"Apify actor call failed ({response.status_code}): {error}")
        response.raise_for_status()

        payload = response.json()
        if isinstance(payload, dict):
            if payload.get("error"):
                raise RuntimeError(str(payload.get("error")))
            items = []
            if isinstance(payload.get("items"), list):
                items = payload.get("items")
            elif isinstance(payload.get("videos"), list):
                items = payload.get("videos")
            elif isinstance(payload.get("data"), list):
                items = payload.get("data")
            elif isinstance(payload.get("results"), list):
                items = payload.get("results")
            return self._as_dict_list(items), payload
        if isinstance(payload, list):
            return self._as_dict_list(payload), {}
        return [], {}

    @staticmethod
    def _sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
        sanitized: dict[str, Any] = {}
        for key, value in payload.items():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            sanitized[key] = value
        return sanitized

    @staticmethod
    def _extract_video_url(item: dict[str, Any]) -> str | None:
        candidate_keys = (
            "webVideoUrl",
            "videoUrl",
            "url",
            "link",
            "aweme_url",
            "awemeUrl",
            "post_url",
        )
        for key in candidate_keys:
            raw = item.get(key)
            if isinstance(raw, str):
                url = raw.strip()
                if url:
                    return url
        raw_id = item.get("id") or item.get("aweme_id")
        if raw_id is not None:
            return f"https://www.tiktok.com/@fallback/video/{str(raw_id)}"
        return None

    def _normalize_discovered_item(self, item: dict[str, Any]) -> dict[str, Any] | None:
        url = self._extract_video_url(item)
        if not url:
            return None
        return {
            "url": normalize_tiktok_url(url),
            "platform_video_id": self._safe_str(item.get("id") or item.get("aweme_id") or item.get("videoId")),
            "published_at": self._safe_timestamp(item.get("createTime") or item.get("create_time") or item.get("publishedAt")),
            "duration_sec": self._safe_int(item.get("duration") or item.get("durationSec") or item.get("videoDuration")),
            "caption_keywords": self._safe_list(item.get("caption_keywords") or item.get("captionKeywords") or item.get("hashtags")),
            "hashtags": self._safe_list(item.get("hashtags")),
            "author_id": self._safe_str(item.get("authorId")),
            "author_handle": self._safe_str(item.get("authorUsername") or item.get("author")),
            "sound_id": self._safe_str(item.get("soundId") or (item.get("sound", {}) or {}).get("id")),
            "sound_title": self._safe_str(item.get("soundTitle") or (item.get("sound", {}) or {}).get("title")),
            "sound_is_original": self._safe_bool(item.get("soundIsOriginal") or (item.get("sound", {}) or {}).get("isOriginal")),
            "width": self._safe_int(item.get("width")),
            "height": self._safe_int(item.get("height")),
            "has_audio": bool(item.get("hasAudio")) if item.get("hasAudio") is not None else None,
        }

    @staticmethod
    def _build_actor_url(actor_id: str, token: str) -> str:
        return f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items?token={token}"

    @staticmethod
    def _redis_connection():
        settings = get_settings()
        if not settings.redis_url:
            return None
        try:
            from redis import Redis

            conn = Redis.from_url(settings.redis_url)
            conn.ping()
            return conn
        except Exception:
            return None

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
