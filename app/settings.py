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


class Settings:
    def __init__(self) -> None:
        self.database_url = os.getenv("DATABASE_URL", "sqlite:///./viral_factory.db")
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.storage_dir = os.getenv("STORAGE_DIR", "./data/storage")
        self.export_dir = os.getenv("EXPORT_DIR", "./data/exports")

        self.provider_default = _to_provider(os.getenv("PROVIDER_DEFAULT"), "apify")
        self.apify_token = os.getenv("APIFY_TOKEN") or None
        self.apify_actor_id = os.getenv("APIFY_ACTOR_ID") or "clockworks/tiktok-scraper"
        self.apify_actor_timeout = int(os.getenv("APIFY_ACTOR_TIMEOUT", "120"))
        self.apify_discovery_actor_id = os.getenv("APIFY_DISCOVERY_ACTOR_ID") or self.apify_actor_id
        self.discovery_sync_max_results = int(os.getenv("DISCOVERY_SYNC_MAX_RESULTS", "120"))
        self.apify_discovery_timeout = int(os.getenv("APIFY_DISCOVERY_TIMEOUT", str(self.apify_actor_timeout)))
        self.apify_max_concurrent_requests = max(1, int(os.getenv("APIFY_MAX_CONCURRENT_REQUESTS", "3")))

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
