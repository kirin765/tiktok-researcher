# Viral Factory

TikTok video discovery/snapshot/analysis pipeline for `viral-factory`.

## Quick Start

```bash
cp .env.example .env
python -m pip install -r requirements.txt
python -m app.cli.commands upgrade

docker compose up --build -d
docker compose exec api alembic upgrade head
```

`WORKER_SYNC=true` is useful for local smoke and CI verification.

## API

- `GET /health`
- `POST /seeds/import-csv`
- `POST /seeds/add-url`
- `GET /videos`
- `GET /videos/{id}/snapshots`
- `GET /videos/{id}/tokens`
- `POST /jobs/fetch-snapshot`
- `POST /jobs/analyze-content`
- `POST /jobs/generate-brief`
- `POST /jobs/compute-scores`
- `GET /jobs`
- `GET /jobs/{id}`
- `GET /jobs/{id}/logs`
- `GET /briefs`
- `GET /briefs/{id}`
- `GET /briefs/{id}/export`

## Environment Defaults

- `PROVIDER_DEFAULT=apify`
- `APIFY_TOKEN=` (required for live apify fetch; tests use mock/stub response)
- `APIFY_ACTOR_ID=clockworks/tiktok-scraper`
- `APIFY_ACTOR_TIMEOUT=120`
- `ENABLE_CONTENT_ANALYSIS=false`
- `BRIEF_WINDOW_DAYS=7`
- `BRIEF_TOP_K=100`

## Smoke Smoke (compose-based)

```bash
docker compose up --build -d
docker compose exec api alembic upgrade head
curl -s http://localhost:8000/health
curl -X POST "http://localhost:8000/seeds/add-url" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://www.tiktok.com/@demo/video/123456","provider":"apify","region":"KR","language":"ko"}'
curl -X POST "http://localhost:8000/jobs/fetch-snapshot" \
  -H "Content-Type: application/json" \
  -d '{"video_id":"<PASTE_VIDEO_ID>","provider":"apify"}'
curl -s "http://localhost:8000/videos/<PASTE_VIDEO_ID>/snapshots"
```

If your APIFY token/actor is not yet configured, run the snapshot job with `provider":"csv"` for a local smoke verification:

```bash
curl -X POST "http://localhost:8000/jobs/fetch-snapshot" \
  -H "Content-Type: application/json" \
  -d '{"video_id":"<PASTE_VIDEO_ID>","provider":"csv"}'
```

and then continue:

```bash
curl -X POST "http://localhost:8000/jobs/generate-brief" \
  -H "Content-Type: application/json" \
  -d '{"region":"KR","language":"ko","niche":"general","window_days":7}'
```
curl -s "http://localhost:8000/briefs"
```

## Notes

- `import-csv` only accepts `provider=csv`.
- `/seeds/add-url` and `/jobs/fetch-snapshot` default to `provider=apify`.
- `TikTokOfficialProvider` is intentionally blocked in phase-1 and returns explicit 501.
