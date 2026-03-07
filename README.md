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
- `POST /seeds/discover`
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
- `APIFY_ACTOR_ID=clockworks~tiktok-scraper`
- `APIFY_DISCOVERY_ACTOR_ID=` (optional; defaults to `APIFY_ACTOR_ID`)
- `APIFY_ACTOR_TIMEOUT=900`
- `APIFY_DISCOVERY_TIMEOUT=900`
- `APIFY_MAX_CONCURRENT_REQUESTS=6`
- `DISCOVERY_SYNC_MAX_RESULTS=120`
- `SCHEDULED_DISCOVERY_ENABLED=false` (legacy interval-based discover; disabled by default in new flow)
- `VIDEO_POOL_MAINTENANCE_ENABLED=true`
- `VIDEO_POOL_MAINTENANCE_INTERVAL_MINUTES=10`
- `VIDEO_POOL_TARGET=200`
- `SCHEDULED_DISCOVERY_MAX_RESULTS=50` (batch cap used by maintenance and manual schedule)
- `SCHEDULED_DISCOVERY_PROVIDER=apify`
- `SCHEDULED_DISCOVERY_QUERY=trending`
- `SCHEDULED_DISCOVERY_REGION=KR`
- `SCHEDULED_DISCOVERY_LANGUAGE=ko`
- `SCHEDULED_DISCOVERY_SORT=latest`
- `SCHEDULED_BRIEF_ENABLED=false` (set true for daily automatic brief generation)
- `SCHEDULED_BRIEF_INTERVAL_HOURS=24`
- `SCHEDULED_BRIEF_REGION=KR`
- `SCHEDULED_BRIEF_LANGUAGE=ko`
- `SCHEDULED_BRIEF_NICHE=general`
- `SCHEDULED_BRIEF_WINDOW_DAYS=7`
- `SCHEDULED_BRIEF_ANALYSIS_LEVEL=1`
- `SCHEDULED_BRIEF_ACTIVE_VIDEO_TARGET=200`
- `SCHEDULED_BRIEF_ANALYSIS_MIN_FINAL_SCORE=-1000000`
- `SCHEDULER_HEALTH_CHECK_INTERVAL_HOURS=3` (scheduler container health check interval, hours)
- `SCHEDULER_RUNNING_STALE_MINUTES=30` (running jobs older than this are treated as stale)
- `RQ_WORKER_JOB_TIMEOUT=1200` (seconds, timeout passed to rq workers)
- `TELEGRAM_ENABLED=true`
- `TELEGRAM_BOT_TOKEN=` (required for Telegram alerts)
- `TELEGRAM_CHAT_ID=` (required for Telegram alerts)
- `ENABLE_CONTENT_ANALYSIS=false`
- `BRIEF_WINDOW_DAYS=7`
- `BRIEF_TOP_K=100`
- `ANALYSIS_LEVEL=1` (0: popularity only, 1: popularity + content signals, 2: strict content signals)
- `ACTIVE_VIDEO_TARGET=200`
- `ANALYSIS_MIN_FINAL_SCORE=-1000000`

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
