from __future__ import annotations


def detect_shots(path: str) -> list[float]:
    # Minimal heuristic-based fallback:
    # without CV runtime dependency, assume a single-shot timeline.
    return []
