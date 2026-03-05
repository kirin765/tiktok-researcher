from __future__ import annotations

import json
import subprocess
from pathlib import Path


def ffprobe(path: str) -> dict:
    media = Path(path)
    if not media.exists():
        return {"duration": 0, "width": 0, "height": 0, "has_audio": False}

    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height",
                "-show_entries",
                "format=duration",
                "-of",
                "json",
                str(media),
            ],
            capture_output=True,
            check=True,
            text=True,
        )
        payload = json.loads(result.stdout or "{}")
        width = _safe_int((payload.get("streams") or [{}])[0].get("width")) or 0
        height = _safe_int((payload.get("streams") or [{}])[0].get("height")) or 0
        duration = _safe_float((payload.get("format") or {}).get("duration")) or 0.0
        return {"duration": duration, "width": width, "height": height, "has_audio": ffprobe_has_audio(media)}
    except Exception:
        return {"duration": 0, "width": 0, "height": 0, "has_audio": False}


def ffprobe_has_audio(media: Path) -> bool:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "a",
                "-show_entries",
                "stream=codec_type",
                "-of",
                "json",
                str(media),
            ],
            capture_output=True,
            check=True,
            text=True,
        )
        payload = json.loads(result.stdout or "{}")
        streams = payload.get("streams") or []
        return bool(streams)
    except Exception:
        return False


def _safe_int(raw) -> int | None:
    try:
        return int(raw)
    except Exception:
        return None


def _safe_float(raw) -> float | None:
    try:
        return float(raw)
    except Exception:
        return None
