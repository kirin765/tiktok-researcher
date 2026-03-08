from __future__ import annotations

import os


_ALLOWED_PROVIDERS = {"apify", "csv", "official"}


def _to_provider(raw: str | None, default: str) -> str:
    value = (raw or default).strip().lower()
    if not value:
        return default
    if value not in _ALLOWED_PROVIDERS:
        raise ValueError(f"unsupported provider_default '{value}', expected one of: {', '.join(sorted(_ALLOWED_PROVIDERS))}")
    return value


def _to_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on", "y"}


def _to_float(raw: str | None, default: float) -> float:
    if raw is None:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _parse_int_list(raw: str | None, default: list[int]) -> list[int]:
    if raw is None or not str(raw).strip():
        return list(default)
    values: list[int] = []
    for part in str(raw).split(","):
        value = part.strip()
        if not value:
            continue
        try:
            parsed = int(value)
        except Exception:
            continue
        if parsed < 0:
            continue
        if parsed not in values:
            values.append(parsed)
    return values if values else list(default)


class Settings:
    def __init__(self) -> None:
        self.database_url = os.getenv("DATABASE_URL", "sqlite:///./viral_factory.db")
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.storage_dir = os.getenv("STORAGE_DIR", "./data/storage")
        self.export_dir = os.getenv("EXPORT_DIR", "./data/exports")

        self.provider_default = _to_provider(os.getenv("PROVIDER_DEFAULT"), "apify")
        self.apify_token = os.getenv("APIFY_TOKEN") or None
        self.apify_actor_id = os.getenv("APIFY_ACTOR_ID") or "clockworks/tiktok-scraper"
        self.apify_actor_timeout = int(os.getenv("APIFY_ACTOR_TIMEOUT", "900"))
        self.apify_discovery_actor_id = os.getenv("APIFY_DISCOVERY_ACTOR_ID") or self.apify_actor_id
        self.discovery_sync_max_results = int(os.getenv("DISCOVERY_SYNC_MAX_RESULTS", "120"))
        self.apify_discovery_timeout = int(os.getenv("APIFY_DISCOVERY_TIMEOUT", str(self.apify_actor_timeout)))
        self.apify_max_concurrent_requests = max(1, int(os.getenv("APIFY_MAX_CONCURRENT_REQUESTS", "3")))
        self.scheduled_discovery_enabled = _to_bool("SCHEDULED_DISCOVERY_ENABLED", False)
        self.scheduled_discovery_interval_hours = max(1, int(os.getenv("SCHEDULED_DISCOVERY_INTERVAL_HOURS", "1")))
        self.scheduled_discovery_max_results = min(50, max(1, int(os.getenv("SCHEDULED_DISCOVERY_MAX_RESULTS", "50"))))
        self.scheduled_discovery_provider = _to_provider(os.getenv("SCHEDULED_DISCOVERY_PROVIDER"), self.provider_default)
        self.scheduled_discovery_query = (os.getenv("SCHEDULED_DISCOVERY_QUERY") or "trending").strip()
        self.scheduled_discovery_region = (os.getenv("SCHEDULED_DISCOVERY_REGION") or "KR").strip()
        self.scheduled_discovery_language = (os.getenv("SCHEDULED_DISCOVERY_LANGUAGE") or "ko").strip()
        self.scheduled_discovery_sort = (os.getenv("SCHEDULED_DISCOVERY_SORT") or "latest").strip()
        self.scheduled_brief_enabled = _to_bool("SCHEDULED_BRIEF_ENABLED", False)
        self.scheduled_brief_interval_hours = max(1, int(os.getenv("SCHEDULED_BRIEF_INTERVAL_HOURS", "24")))
        self.scheduled_brief_region = (os.getenv("SCHEDULED_BRIEF_REGION") or self.scheduled_discovery_region).strip()
        self.scheduled_brief_language = (os.getenv("SCHEDULED_BRIEF_LANGUAGE") or self.scheduled_discovery_language).strip()
        self.scheduled_brief_niche = (os.getenv("SCHEDULED_BRIEF_NICHE") or "general").strip()
        self.scheduled_brief_window_days = max(
            1,
            int(
                os.getenv(
                    "SCHEDULED_BRIEF_WINDOW_DAYS",
                    os.getenv("BRIEF_WINDOW_DAYS", "7"),
                )
            ),
        )
        self.scheduled_brief_analysis_level = max(
            0,
            min(
                2,
                int(
                    os.getenv(
                        "SCHEDULED_BRIEF_ANALYSIS_LEVEL",
                        os.getenv("ANALYSIS_LEVEL", "1"),
                    )
                ),
            ),
        )
        self.scheduled_brief_active_video_target = max(
            1,
            int(
                os.getenv(
                    "SCHEDULED_BRIEF_ACTIVE_VIDEO_TARGET",
                    os.getenv("ACTIVE_VIDEO_TARGET", "200"),
                )
            ),
        )
        self.scheduled_brief_analysis_min_final_score = _to_float(
            os.getenv(
                "SCHEDULED_BRIEF_ANALYSIS_MIN_FINAL_SCORE",
                os.getenv("ANALYSIS_MIN_FINAL_SCORE", "-1000000"),
            ),
            -1000000.0,
        )
        self.video_pool_maintenance_enabled = _to_bool("VIDEO_POOL_MAINTENANCE_ENABLED", True)
        self.video_pool_maintenance_interval_minutes = max(1, int(os.getenv("VIDEO_POOL_MAINTENANCE_INTERVAL_MINUTES", "10")))
        self.rq_job_timeout = max(60, int(os.getenv("RQ_WORKER_JOB_TIMEOUT", "600")))

        self.analysis_level = max(0, min(2, int(os.getenv("ANALYSIS_LEVEL", "1"))))
        self.active_video_target = max(1, int(os.getenv("ACTIVE_VIDEO_TARGET", "200")))
        self.video_pool_target = max(1, int(os.getenv("VIDEO_POOL_TARGET", str(self.active_video_target))))
        self.analysis_min_final_score = _to_float(os.getenv("ANALYSIS_MIN_FINAL_SCORE"), -1000000.0)
        self.snapshot_schedule_offsets_seconds = _parse_int_list(
            os.getenv("SNAPSHOT_SCHEDULE_OFFSETS_SECONDS"),
            [0, 86400, 259200],
        )

        self.telegram_bot_token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip() or None
        self.telegram_chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip() or None
        self.telegram_enabled = _to_bool(
            "TELEGRAM_ENABLED",
            self.telegram_bot_token is not None and self.telegram_chat_id is not None,
        )
        self.scheduler_health_check_interval_hours = max(1, int(os.getenv("SCHEDULER_HEALTH_CHECK_INTERVAL_HOURS", "3")))
        self.scheduler_running_stale_minutes = max(1, int(os.getenv("SCHEDULER_RUNNING_STALE_MINUTES", "30")))

        self.enable_content_analysis = _to_bool("ENABLE_CONTENT_ANALYSIS", False)
        self.enable_asr = _to_bool("ENABLE_ASR", False)
        self.cleanup_source_video = _to_bool("CLEANUP_SOURCE_VIDEO", False)
        self.max_snapshot_attempts = int(os.getenv("MAX_SNAPSHOT_ATTEMPTS", "5"))
        self.retry_base_seconds = int(os.getenv("RETRY_BASE_SECONDS", "60"))
        self.retry_cap_seconds = int(os.getenv("RETRY_CAP_SECONDS", "3600"))

        self.brief_window_days = int(os.getenv("BRIEF_WINDOW_DAYS", "7"))
        self.brief_top_k = int(os.getenv("BRIEF_TOP_K", "100"))

        # default true for local smoke/dev, false when explicitly passed false.
        self.worker_sync = _to_bool("WORKER_SYNC", not self.redis_url.startswith("redis://"))
        self.log_level = os.getenv("LOG_LEVEL", "INFO").upper()


def get_settings() -> Settings:
    return Settings()
