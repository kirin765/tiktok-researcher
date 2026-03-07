from __future__ import annotations

import uuid
from datetime import date, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.pattern_mining import mine_patterns
from app.core.scoring import compute_scores_for_videos
from app.db.models import ContentToken, CreativeBrief, Video
from app.settings import get_settings


def _safe_int(raw: object, default: int | None = None) -> int | None:
    try:
        return int(raw)
    except Exception:
        return default


def _safe_float(raw: object, default: float | None = None) -> float | None:
    try:
        return float(raw)
    except Exception:
        return default


def _safe_get(root: object, path: list[str], default: object = None) -> object:
    cur = root
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key, default)
    return cur if cur is not None else default


def _analysis_weights(level: int) -> tuple[float, float]:
    if level <= 0:
        return 1.0, 0.0
    if level == 1:
        return 0.70, 0.30
    return 0.65, 0.35


def _build_content_signals(level: int, tokens_json: dict | None) -> tuple[float, dict[str, object]]:
    tokens = tokens_json or {}
    if not isinstance(tokens, dict):
        tokens = {}

    if level <= 0:
        return 0.0, {"enabled": False}

    hook = tokens.get("hook_proxy") if isinstance(tokens.get("hook_proxy"), dict) else {}
    pacing = tokens.get("pacing_proxy") if isinstance(tokens.get("pacing_proxy"), dict) else {}
    subtitle = tokens.get("subtitle_proxy") if isinstance(tokens.get("subtitle_proxy"), dict) else {}
    audio = tokens.get("audio_proxy") if isinstance(tokens.get("audio_proxy"), dict) else {}
    resolution = tokens.get("resolution") if isinstance(tokens.get("resolution"), dict) else {}

    cut_signal = _safe_int(_safe_get(hook, ["cuts_in_first_3s"]))
    cut_rate = _safe_float(_safe_get(pacing, ["cut_rate_per_sec"]))
    subtitle_ratio = _safe_float(_safe_get(subtitle, ["subtitle_presence_ratio"]))
    text_ratio = _safe_float(_safe_get(subtitle, ["avg_chars_per_line_est"]))
    music_energy = _safe_float(_safe_get(audio, ["music_energy_est"]))
    duration_sec = _safe_float(tokens.get("duration_sec"), 0.0)
    width = _safe_int(resolution.get("width"), 0) or 0
    height = _safe_int(resolution.get("height"), 0) or 0

    score = 0.0
    signals: dict[str, object] = {
        "enabled": True,
        "level": level,
        "has_content_tokens": bool(tokens),
    }

    if isinstance(cut_signal, int):
        if cut_signal >= 2:
            score += 0.30
            signals["hook"] = "strong"
        elif cut_signal >= 1:
            score += 0.15
            signals["hook"] = "weak"
        else:
            signals["hook"] = "absent"

    if cut_rate is not None:
        if 0.4 <= cut_rate <= 2.2:
            score += 0.22
            signals["pacing"] = "strong"
        elif 0.2 <= cut_rate <= 4.0:
            score += 0.10
            signals["pacing"] = "usable"
        else:
            signals["pacing"] = "weak"

    if subtitle_ratio is not None:
        ratio = max(0.0, min(1.0, subtitle_ratio))
        score += 0.18 * ratio
        signals["subtitle_ratio"] = ratio

    if text_ratio is not None:
        if text_ratio <= 18:
            score += 0.10
            signals["subtitle_density"] = "tight"
        else:
            signals["subtitle_density"] = "wide"

    if music_energy is not None:
        if music_energy >= 0.5:
            score += 0.10
            signals["audio"] = "energetic"
        else:
            signals["audio"] = "flat"

    if duration_sec is not None:
        if 6.0 <= duration_sec <= 18.0:
            score += 0.10
            signals["duration"] = "short-form"
        elif 18.0 < duration_sec <= 40.0:
            score += 0.06
            signals["duration"] = "mid-form"
        elif duration_sec > 0:
            signals["duration"] = "long-form"

    if width >= 720 and height >= 720:
        score += 0.04
        signals["resolution"] = "1080p_or_higher"
    elif width >= 540 and height >= 540:
        signals["resolution"] = "hdish"
        score += 0.02

    return min(score, 1.0), signals


def build_brief_json(
    db: Session,
    region: str,
    language: str,
    niche: str,
    window_days: int = 7,
    analysis_level: int | None = None,
    active_video_target: int | None = None,
    analysis_min_final_score: float | None = None,
) -> dict:
    settings = get_settings()
    window_end = date.today()
    window_start = window_end - timedelta(days=window_days)

    ids = db.execute(select(Video.id).where(Video.region == region).where(Video.language == language)).scalars().all()
    scored = compute_scores_for_videos(db, list(ids), window_days=window_days)
    token_rows = db.execute(select(ContentToken.video_id, ContentToken.tokens_json).where(ContentToken.video_id.in_(ids))).all()
    token_map = {row.video_id: row.tokens_json for row in token_rows}

    ranked = []
    resolved_analysis_level = settings.analysis_level if analysis_level is None else analysis_level
    resolved_analysis_level = max(0, min(2, int(resolved_analysis_level)))
    resolved_active_target = settings.active_video_target if active_video_target is None else active_video_target
    resolved_active_target = max(1, int(resolved_active_target))
    resolved_min_score = settings.analysis_min_final_score if analysis_min_final_score is None else analysis_min_final_score
    resolved_min_score = float(resolved_min_score)

    viral_weight, content_weight = _analysis_weights(resolved_analysis_level)

    for row in scored:
        row_id = uuid.UUID(row["video_id"])
        tokens = token_map.get(row_id)
        content_score, signals = _build_content_signals(resolved_analysis_level, tokens if isinstance(tokens, dict) else {})
        pop_score = 0.0 if row["pop_score"] is None else row["pop_score"]
        if resolved_analysis_level == 2 and not bool(tokens):
            final_score = pop_score * 0.45
            signals["analysis_penalty"] = "level_2_requires_content_tokens"
        else:
            final_score = (viral_weight * pop_score) + (content_weight * content_score * 3.0)

        candidate = {
            **row,
            "analysis_level": resolved_analysis_level,
            "content_score": content_score,
            "content_signals": signals,
            "final_score": final_score,
        }

        if final_score >= resolved_min_score:
            ranked.append(candidate)

    scored_sorted = sorted(ranked, key=lambda item: item["final_score"], reverse=True)
    active_count = min(resolved_active_target, len(scored_sorted))
    active_scored = scored_sorted[:active_count]

    if scored_sorted:
        top_ratio = max(1, int(len(active_scored) * 0.10))
        bottom_ratio = max(1, int(len(active_scored) * 0.50))
        top_count = min(settings.brief_top_k, top_ratio)
        top_items = active_scored[:top_count]
        bottom_items = active_scored[-bottom_ratio:]
    else:
        top_items = []
        bottom_items = []

    top_video_ids = [uuid.UUID(item["video_id"]) for item in top_items]
    top_tokens = [db.get(ContentToken, video_id) for video_id in top_video_ids]
    top_jsons = [ct.tokens_json for ct in top_tokens if ct]

    bottom_video_ids = [uuid.UUID(item["video_id"]) for item in bottom_items]
    bottom_tokens = [db.get(ContentToken, video_id) for video_id in bottom_video_ids]
    bottom_jsons = [ct.tokens_json for ct in bottom_tokens if ct]

    top_videos = []
    for item in top_items:
        video = db.get(Video, uuid.UUID(item["video_id"]))
        if video is None:
            continue
        has_tokens = db.get(ContentToken, video.id) is not None
        top_videos.append(
            {
                "video_id": str(video.id),
                "url": video.url,
                "published_at": video.published_at.isoformat() if video.published_at else None,
                "snapshot_0h": item["snapshot_0h"],
                "snapshot_24h": item["snapshot_24h"],
                "pop_score": item["pop_score"],
                "final_score": item["final_score"],
                "content_score": item["content_score"],
                "analysis_level": item["analysis_level"],
                "analysis_signals": item["content_signals"],
                "content_tokens_ref": {"has_tokens": has_tokens},
            }
        )

    return {
        "meta": {
            "generatedAt": date.today().isoformat(),
            "platform": "tiktok",
            "region": region,
            "language": language,
            "niche": niche,
            "window": {"start": str(window_start), "end": str(window_end)},
            "dataset": {"numVideos": len(scored_sorted), "numScored": len(top_items)},
            "analysis": {
                "analysis_level": resolved_analysis_level,
                "active_target": resolved_active_target,
                "active_count": len(active_scored),
                "analysis_min_final_score": resolved_min_score,
            },
        },
        "objective": {
            "primaryMetric": "view_velocity_24h",
            "secondaryMetrics": ["share_rate", "bookmark_rate", "like_rate"],
            "scoreFormula": f"{viral_weight:.2f}*viral_score+{content_weight:.2f}*content_score",
        },
        "top_videos": top_videos,
        "pattern_library": {
            "high_signal_features": mine_patterns(top_jsons, bottom_jsons),
            "hook_archetypes": [
                {
                    "name": "Curiosity gap",
                    "rules": ["open with conflict in first 2s", "resolve by 3s"],
                }
            ],
            "editing_rules": {
                "recommended_duration_sec_range": [12, 28],
                "cut_rate_per_sec_range": [0.6, 2.0],
            },
            "subtitle_rules": {
                "presence_ratio_min": 0.7,
                "one_line_preferred": True,
                "max_chars_per_line_est": 18,
            },
            "cta_patterns": [{"type": "save", "timing_sec_range": [8, 15]}],
        },
        "generation_request": {
            "numConcepts": 10,
            "returnTopK": 3,
            "required_outputs": [
                "concept_title",
                "hook_script_0_2s",
                "script_full",
                "shotlist_with_timestamps",
                "onscreen_text_timeline",
                "edit_recipe",
                "caption_and_hashtags",
                "ab_test_variants",
            ],
            "hard_constraints": [
                "single_hook_first_2sec",
                "duration_18_to_40_sec",
                "one_cta_minimum",
                "clear_caption_present",
            ],
        },
    }


def persist_brief(
    db: Session,
    region: str,
    language: str,
    niche: str,
    window_days: int,
    analysis_level: int | None = None,
    active_video_target: int | None = None,
    analysis_min_final_score: float | None = None,
) -> CreativeBrief:
    payload = build_brief_json(
        db,
        region=region,
        language=language,
        niche=niche,
        window_days=window_days,
        analysis_level=analysis_level,
        active_video_target=active_video_target,
        analysis_min_final_score=analysis_min_final_score,
    )
    brief = CreativeBrief(
        region=region,
        language=language,
        niche=niche,
        window_start=date.fromisoformat(payload["meta"]["window"]["start"]),
        window_end=date.fromisoformat(payload["meta"]["window"]["end"]),
        brief_json=payload,
    )
    db.add(brief)
    return brief


def get_brief_payload(db: Session, brief_id: str) -> dict:
    brief = db.get(CreativeBrief, brief_id)
    return brief.brief_json if brief else {}
