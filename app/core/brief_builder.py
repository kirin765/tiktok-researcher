from __future__ import annotations

from datetime import date, timedelta
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.pattern_mining import mine_patterns
from app.core.scoring import compute_scores_for_videos
from app.db.models import ContentToken, CreativeBrief, Video
from app.settings import get_settings


def build_brief_json(db: Session, region: str, language: str, niche: str, window_days: int = 7) -> dict:
    settings = get_settings()
    window_end = date.today()
    window_start = window_end - timedelta(days=window_days)

    ids = db.execute(select(Video.id).where(Video.region == region).where(Video.language == language)).scalars().all()
    scored = compute_scores_for_videos(db, list(ids), window_days=window_days)
    scored_sorted = sorted(scored, key=lambda x: x["pop_score"], reverse=True)

    if scored_sorted:
        top_ratio = max(1, int(len(scored_sorted) * 0.10))
        bottom_ratio = max(1, int(len(scored_sorted) * 0.50))
        top_count = min(settings.brief_top_k, top_ratio)
        top_items = scored_sorted[:top_count]
        bottom_items = scored_sorted[-bottom_ratio:]
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
        },
        "objective": {
            "primaryMetric": "view_velocity_24h",
            "secondaryMetrics": ["share_rate", "bookmark_rate", "like_rate"],
            "scoreFormula": "0.45*z(log1p(delta_views_24h))+0.25*z(share_rate)+0.20*z(save_rate)+0.10*z(comment_rate)",
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


def persist_brief(db: Session, region: str, language: str, niche: str, window_days: int) -> CreativeBrief:
    payload = build_brief_json(db, region=region, language=language, niche=niche, window_days=window_days)
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
