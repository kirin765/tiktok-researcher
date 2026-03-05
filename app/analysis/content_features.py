from __future__ import annotations

from pathlib import Path
import shutil
import statistics
import tempfile

from app.analysis.audio import analyze_audio
from app.analysis.download import download_video
from app.analysis.ffprobe import ffprobe
from app.analysis.ocr import extract_ocr
from app.analysis.shots import detect_shots
from app.settings import get_settings


def default_content_tokens():
    return {
        "schema_version": "1.0",
        "duration_sec": 0,
        "resolution": {"width": 0, "height": 0},
        "hook_proxy": {
            "first_text_time_sec": None,
            "first_face_time_sec": None,
            "cuts_in_first_3s": None,
        },
        "pacing_proxy": {
            "cut_rate_per_sec": None,
            "avg_shot_len_sec": None,
            "shot_len_p50_sec": None,
            "shot_len_p90_sec": None,
        },
        "subtitle_proxy": {
            "subtitle_presence_ratio": None,
            "text_change_rate_per_sec": None,
            "bottom_text_ratio": None,
            "avg_chars_per_line_est": None,
        },
        "audio_proxy": {
            "voice_presence_ratio": None,
            "speech_rate_wpm_est": None,
            "silence_ratio": None,
            "music_energy_est": None,
        },
        "extensions": {},
    }


def build_content_tokens(url: str) -> dict:
    settings = get_settings()
    token = default_content_tokens()
    temp_dir: Path | None = None
    file_path: Path | None = None

    try:
        temp_dir = Path(tempfile.mkdtemp())
        file_path = Path(download_video(url, out_path=str(temp_dir / "video.mp4")))
        meta = ffprobe(str(file_path))
        token["duration_sec"] = int(meta.get("duration", 0) or 0)
        token["resolution"] = {"width": int(meta.get("width", 0) or 0), "height": int(meta.get("height", 0) or 0)}
        token["pacing_proxy"]["avg_shot_len_sec"] = None
        token["pacing_proxy"]["shot_len_p50_sec"] = None
        token["pacing_proxy"]["shot_len_p90_sec"] = None
        token["audio_proxy"].update(analyze_audio(str(file_path)))
        shots = detect_shots(str(file_path))
        token["pacing_proxy"]["cut_rate_per_sec"] = _calc_cut_rate(shots, token["duration_sec"])
        ocr = extract_ocr(str(file_path))
        token["subtitle_proxy"]["text_change_rate_per_sec"] = _calc_text_change_rate(ocr)
        token["subtitle_proxy"]["subtitle_presence_ratio"] = 1.0 if ocr else 0.0
        token["hook_proxy"]["cuts_in_first_3s"] = _count_cuts_in_first_sec(shots, 3.0)
        return token
    except Exception:
        return token
    finally:
        if file_path is not None and settings.cleanup_source_video:
            try:
                file_path.unlink(missing_ok=True)
            except Exception:
                pass
        if temp_dir is not None:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass


def _count_cuts_in_first_sec(shots: list, seconds: float) -> int | None:
    if not shots:
        return None
    return sum(1 for s in shots if (s or 0) <= seconds)


def _calc_text_change_rate(ocr_segments: list[dict]) -> float | None:
    if not ocr_segments:
        return None
    return float(len(ocr_segments)) / max(len(ocr_segments), 1)


def _calc_cut_rate(shots: list, duration_sec: int) -> float | None:
    if not shots or not duration_sec:
        return None
    return float(max(len(shots) - 1, 0)) / max(duration_sec, 1)
