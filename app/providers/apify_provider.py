from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import random
import threading
import time
import re
from typing import Any
from urllib.parse import quote

from sqlalchemy import select
import requests
from redis.exceptions import LockNotOwnedError

from app.core.ids import extract_tiktok_video_id, normalize_tiktok_url
from app.db.models import Video
from app.providers.base import BaseProvider, MetricPayload
from app.settings import get_settings


class ApifyProvider(BaseProvider):
    name = "apify"
    _thread_gate: threading.BoundedSemaphore | None = None
    _thread_gate_limit: int = 1
    _run_id_pattern = re.compile(r"run ID:\s*([A-Za-z0-9_-]+)", re.IGNORECASE)
    _RUN_SYNC_TIMEOUT_CAP_SECONDS = 300
    _metric_key_paths = {
        "view_count": (
            ("stats", "playCount"),
            ("stats", "viewCount"),
            ("stats", "play_count"),
            ("stats", "view_count"),
            ("playCount",),
            ("viewCount",),
            ("play_count",),
            ("view_count",),
            ("videoPlayCount",),
            ("views",),
            ("item", "stats", "playCount"),
            ("item", "stats", "viewCount"),
            ("item", "stats", "play_count"),
            ("item", "playCount"),
            ("item", "viewCount"),
        ),
        "like_count": (
            ("stats", "diggCount"),
            ("stats", "likeCount"),
            ("stats", "like_count"),
            ("diggCount",),
            ("likeCount",),
            ("like_count",),
            ("item", "stats", "diggCount"),
            ("item", "stats", "likeCount"),
            ("item", "diggCount"),
        ),
        "comment_count": (
            ("stats", "commentCount"),
            ("stats", "comment_count"),
            ("commentCount",),
            ("comment_count",),
            ("item", "stats", "commentCount"),
            ("item", "commentCount"),
        ),
        "share_count": (
            ("stats", "shareCount"),
            ("stats", "share_count"),
            ("shareCount",),
            ("share_count",),
            ("item", "stats", "shareCount"),
            ("item", "shareCount"),
        ),
        "bookmark_count": (
            ("stats", "collectCount"),
            ("stats", "saveCount"),
            ("stats", "bookmarkCount"),
            ("stats", "bookmark_count"),
            ("collectCount",),
            ("saveCount",),
            ("collect_count",),
            ("bookmarkCount",),
            ("bookmark_count",),
            ("item", "stats", "collectCount"),
            ("item", "stats", "saveCount"),
            ("item", "collectCount"),
        ),
    }

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
                try:
                    acquired.release()
                except LockNotOwnedError:
                    pass

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
        platform_video_id = extract_tiktok_video_id(normalized)
        if platform_video_id is None:
            raise ValueError(f"invalid tiktok video URL: {url}")
        existing = session.execute(select(Video).where(Video.url == normalized)).scalar_one_or_none()
        if existing:
            if region and not existing.region:
                existing.region = region
            if language and not existing.language:
                existing.language = language
            if not existing.platform_video_id:
                existing.platform_video_id = platform_video_id
            return existing

        row = Video(
            platform="tiktok",
            url=normalized,
            platform_video_id=platform_video_id,
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

        view_count = self._extract_metric(item, self._metric_key_paths["view_count"])
        like_count = self._extract_metric(item, self._metric_key_paths["like_count"])
        comment_count = self._extract_metric(item, self._metric_key_paths["comment_count"])
        share_count = self._extract_metric(item, self._metric_key_paths["share_count"])
        bookmark_count = self._extract_metric(item, self._metric_key_paths["bookmark_count"])
        raw = {"provider": self.name, "item": item}
        if (
            view_count is None
            and like_count is None
            and comment_count is None
            and share_count is None
            and bookmark_count is None
        ):
            raw["parse_warning"] = "missing_numeric_metrics"
        return MetricPayload(
            captured_at=now,
            source=self.name,
            view_count=view_count,
            like_count=like_count,
            comment_count=comment_count,
            share_count=share_count,
            bookmark_count=bookmark_count,
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
        request_timeout = max(1, min(self._RUN_SYNC_TIMEOUT_CAP_SECONDS, resolved_timeout))

        try:
            with self._apify_slot():
                response = requests.post(
                    self._build_actor_url(resolved_actor_id, token, request_timeout=request_timeout),
                    json=self._sanitize_payload(payload),
                    timeout=resolved_timeout,
                )
        except requests.exceptions.Timeout as exc:
            raise RuntimeError(f"Apify actor call timeout ({resolved_timeout}s): {exc}") from exc
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(f"Apify actor call failed: {exc}") from exc
        if not response.ok:
            try:
                body = response.json()
            except Exception:
                body = response.text
            detail = self._format_apify_error(body, token)
            raise RuntimeError(
                f"Apify actor call failed ({response.status_code}) at {self._redact_apify_url(getattr(response, 'url', 'unknown'))}: {detail}"
            )
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
            normalized_id = str(raw_id).strip()
            if normalized_id.isdigit():
                return f"https://www.tiktok.com/@apify/video/{normalized_id}"
        return None

    def _normalize_discovered_item(self, item: dict[str, Any]) -> dict[str, Any] | None:
        url = self._extract_video_url(item)
        if not url:
            return None
        normalized_url = normalize_tiktok_url(url)
        platform_video_id = self._safe_str(item.get("id") or item.get("aweme_id") or item.get("videoId"))
        if platform_video_id is None:
            platform_video_id = extract_tiktok_video_id(normalized_url)
        if not platform_video_id or not platform_video_id.isdigit():
            return None
        return {
            "url": normalized_url,
            "platform_video_id": platform_video_id,
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
    def _normalize_actor_id(raw_actor_id: str) -> str:
        if not raw_actor_id:
            raise ValueError("APIFY actor id is required")
        resolved_actor_id = raw_actor_id.strip()
        if not resolved_actor_id:
            raise ValueError("APIFY actor id is required")
        if "/" in resolved_actor_id and "~" not in resolved_actor_id:
            resolved_actor_id = "~".join(part for part in resolved_actor_id.split("/") if part)
        if not resolved_actor_id:
            raise ValueError("APIFY actor id is required")
        return resolved_actor_id

    @staticmethod
    def _build_actor_url(actor_id: str, token: str, request_timeout: int | None = None) -> str:
        resolved_actor_id = ApifyProvider._normalize_actor_id(actor_id)
        resolved_actor_id = quote(resolved_actor_id, safe="~@")
        url = f"https://api.apify.com/v2/acts/{resolved_actor_id}/run-sync-get-dataset-items?token={token}"
        if request_timeout is not None:
            url = f"{url}&timeout={max(1, int(request_timeout))}"
        return url

    @staticmethod
    def _redact_apify_url(url: str) -> str:
        if not isinstance(url, str):
            return "unknown"
        return re.sub(r"token=[^&]+", "token=***", url)

    @classmethod
    def _extract_run_id(cls, text: str) -> str | None:
        match = cls._run_id_pattern.search(text)
        if not match:
            return None
        return match.group(1)

    @staticmethod
    def _extract_run_hint(token: str, run_id: str) -> str | None:
        try:
            response = requests.get(f"https://api.apify.com/v2/actor-runs/{run_id}?token={token}", timeout=8)
        except Exception:
            return None
        if not response.ok:
            return None
        try:
            payload = response.json()
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None

        status = payload.get("status")
        status_message = payload.get("statusMessage")
        exit_code = payload.get("exitCode")
        parts: list[str] = []
        if status:
            parts.append(f"status={status}")
        if exit_code is not None:
            parts.append(f"exit_code={exit_code}")
        if status_message:
            parts.append(f"status_message={status_message}")
        return ", ".join(parts) if parts else None

    @classmethod
    def _format_apify_error(cls, body: object, token: str | None = None) -> str:
        if isinstance(body, dict):
            detail = body.get("error")
            if isinstance(detail, dict):
                detail_type = detail.get("type")
                detail_message = detail.get("message")
                if detail_type and detail_message:
                    error = f"{detail_type}: {detail_message}"
                elif detail_type:
                    error = str(detail_type)
                elif detail_message:
                    error = str(detail_message)
                else:
                    error = str(detail)
            elif detail is not None:
                error = str(detail)
            elif body.get("type") and body.get("message"):
                error = f'{body.get("type")}: {body.get("message")}'
            elif body.get("type") is not None:
                error = str(body.get("type"))
            elif body.get("message") is not None:
                error = str(body.get("message"))
            else:
                error = str(body)
        elif isinstance(body, str):
            error = body.strip() or "unknown"
        else:
            error = str(body)

        if token:
            run_id = cls._extract_run_id(error)
            if run_id:
                run_hint = cls._extract_run_hint(token, run_id)
                if run_hint:
                    error = f"{error} (run_hint: {run_hint})"
        return error

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
        for item in payload:
            if ApifyProvider._item_has_metrics(item):
                return item
        return payload[0]

    @classmethod
    def _item_has_metrics(cls, item: dict[str, Any]) -> bool:
        if not isinstance(item, dict):
            return False
        for paths in cls._metric_key_paths.values():
            for path in paths:
                raw = cls._extract_nested(item, path)
                if raw is None:
                    continue
                if cls._safe_int(raw) is not None:
                    return True
        return False

    @staticmethod
    def _extract_nested(item: dict[str, Any], path: tuple[str, ...]) -> Any | None:
        current: Any = item
        for key in path:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
            if current is None:
                return None
        return current

    @classmethod
    def _extract_metric(cls, item: dict[str, Any], paths: tuple[tuple[str, ...], ...]) -> int | None:
        for path in paths:
            raw = cls._extract_nested(item, path)
            parsed = cls._safe_int(raw)
            if parsed is not None:
                return parsed
        return None

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
