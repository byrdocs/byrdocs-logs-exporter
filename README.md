# BYR Docs Logs Exporter

TypeScript Cloudflare Worker that imports daily analytics logs from Cloudflare R2 into PostgreSQL.

## Features

- Cloudflare Worker runtime with built-in cron scheduling.
- Scheduled import every day at `01:00 UTC`.
- Manual import endpoint with optional `force=true` reimport mode.
- Dynamic table creation and schema evolution by `dataset`.
- Import status tracking in the `_import` table.

## Requirements

- Node.js 22 LTS (recommended).
- Cloudflare account with Workers enabled.
- R2 bucket containing files named `YYYY-MM-DD.json.gz`.
- PostgreSQL reachable from Workers (Hyperdrive is recommended).

## Setup

1. Install dependencies:

   ```bash
   npm install
   ```

2. Update `wrangler.toml`:
   - Set `name` to your worker name.
   - Set `bucket_name` and `preview_bucket_name` in `[[r2_buckets]]`.
   - If using Hyperdrive, uncomment and fill the `[[hyperdrive]]` block.

3. Configure database credentials as secrets:

   ```bash
   wrangler secret put LOGS_DB_URL
   ```

   Or provide split variables instead:

   - `LOGS_DB_HOST`
   - `LOGS_DB_PORT`
   - `LOGS_DB_USERNAME`
   - `LOGS_DB_PASSWORD`
   - `LOGS_DB_DATABASE`
   - `LOGS_DB_SSLMODE` (optional: `prefer`, `require`, `disable`, etc.)

   If `LOGS_DB_SSLMODE` is not set, the driver uses its default behavior.

4. (Recommended) Protect endpoints:

   ```bash
   wrangler secret put API_TOKEN
   ```

5. Run locally:

   ```bash
   npm run dev
   ```

   `npm run dev` uses Cloudflare remote mode so it reads the real R2 bucket.
   Use `npm run dev:local` if you explicitly want local emulation.

6. Deploy:

   ```bash
   npm run deploy
   ```

## Endpoints

- `GET /health`
- `GET|POST /import?date=YYYY-MM-DD&force=true&timeoutMs=120000`
  - `date` is optional; defaults to yesterday (UTC).
  - `force=true` deletes existing rows for that date and dataset before reimport.
  - `timeoutMs` is optional and defaults to `120000`.
- `GET /diag`
  - probes R2 listing and PostgreSQL connectivity.
  - add `?tcp=1` to include raw TCP object/string probe details.
- `GET /list?prefix=`

If `API_TOKEN` is set, pass it as either:

- `Authorization: Bearer <token>`
- `x-api-token: <token>`

## Cron

The cron trigger is configured in `wrangler.toml`:

```toml
[triggers]
crons = ["0 1 * * *"]
```

At each run, the worker imports logs for yesterday (UTC).

## Local variables

Use `.dev.vars.example` as a template for local development.

If imports appear to hang, check the `wrangler dev` terminal logs and call `/diag`.
