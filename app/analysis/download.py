from __future__ import annotations

from pathlib import Path
import tempfile


def download_video(url: str, out_path: str | None = None) -> str:
    output = Path(out_path or tempfile.mkdtemp())
    output.mkdir(parents=True, exist_ok=True)

    try:
        import yt_dlp  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("yt-dlp not installed") from exc

    out_file = output / "video.mp4"
    ydl_opts = {
        "outtmpl": str(out_file),
        "format": "mp4/best",
        "noplaylist": True,
        "quiet": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

    if not out_file.exists():
        # fallback for extension variations
        candidates = list(output.glob("*.mp4"))
        if not candidates:
            raise RuntimeError("yt-dlp output not found")
        return str(candidates[0])
    return str(out_file)
