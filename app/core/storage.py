from __future__ import annotations

from datetime import datetime
from pathlib import Path

import json

from app.settings import get_settings


def ensure_storage() -> None:
    settings = get_settings()
    Path(settings.storage_dir).mkdir(parents=True, exist_ok=True)
    Path(settings.export_dir).mkdir(parents=True, exist_ok=True)


def write_export(payload: dict, filename: str) -> Path:
    ensure_storage()
    settings = get_settings()
    out = Path(settings.export_dir) / filename
    out.write_text(json.dumps(payload), encoding="utf-8")
    return out


def brief_filename(created_at: datetime, brief_id: str) -> str:
    return f"creative_brief_{created_at:%Y%m%d}.json"
