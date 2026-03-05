from __future__ import annotations


def analyze_audio(path: str) -> dict:
    # Optional ASR/audio module fallback.
    return {"voice_presence_ratio": None, "speech_rate_wpm_est": None, "silence_ratio": None, "music_energy_est": None}
