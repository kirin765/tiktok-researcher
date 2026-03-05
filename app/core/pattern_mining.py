from __future__ import annotations

import statistics


def _mean(values: list[float]) -> float:
    return statistics.mean(values) if values else 0.0


def mine_patterns(top: list[dict], bottom: list[dict]) -> list[dict]:
    if not top and not bottom:
        return []

    def _select(path: tuple[str, ...], row: dict):
        cur = row
        for key in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(key)
        return cur

    feature_paths = [
        ("hook_proxy", "cuts_in_first_3s"),
        ("pacing_proxy", "cut_rate_per_sec"),
        ("subtitle_proxy", "subtitle_presence_ratio"),
        ("audio_proxy", "music_energy_est"),
        ("subtitle_proxy", "avg_chars_per_line_est"),
    ]

    out = []
    for path in feature_paths:
        top_vals = [float(_select(path, x) or 0.0) for x in top if _select(path, x) is not None]
        bottom_vals = [float(_select(path, x) or 0.0) for x in bottom if _select(path, x) is not None]
        out.append(
            {
                "feature": "/".join(path),
                "direction": "higher_is_better",
                "evidence": f"top mean {_mean(top_vals):.4f} vs bottom mean {_mean(bottom_vals):.4f}",
            }
        )
    return out
