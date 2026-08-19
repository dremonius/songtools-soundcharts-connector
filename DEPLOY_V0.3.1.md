# Songtools Soundcharts Connector v0.3.1

This patch adds a targeted recovery tool for the bulk-job rows that failed only because the connector was authenticated against the wrong Soundcharts team and exhausted that team's monthly quota.

## Changed files

Upload/replace these files in the existing GitHub repository, preserving their paths:

- `package.json`
- `src/server.mjs`
- `src/database.mjs`

No environment-variable changes are required for this patch. Keep the corrected Soundcharts AppID/Token configuration already deployed in Render.

## New MCP tool

`bulk_job_requeue_quota_failures`

The tool only matches failed rows whose stored error/result contains the explicit Soundcharts phrase `monthly available quota`. It intentionally does **not** requeue generic 429 rate-limit errors, 404s, artist-name mismatches, or ambiguous artist resolutions.

It defaults to dry-run mode.

### 1. Deploy and verify

After GitHub is updated, let Render redeploy. Verify `/health` reports:

```json
{"version":"0.3.1"}
```

Reconnect/refresh MCP Inspector so the new tool appears.

### 2. Dry run first

Run:

```json
{
  "jobId": 1,
  "dryRun": true
}
```

The response reports:

- `matchedQuotaFailures` — exact count across all failed rows in PostgreSQL
- `requeued` — 0 during dry run
- `untouchedFailures` — failures that will remain untouched

### 3. Requeue only quota failures

If the dry-run count looks correct, run:

```json
{
  "jobId": 1,
  "dryRun": false
}
```

This resets only matching quota-failure rows to:

- `status = pending`
- `attempts = 0`
- `result_status = requeued_quota`

The old stored error/result remains in place until the retry overwrites it, preserving an audit trail. The job is reopened in `paused` state and `completed_at` is cleared.

For safety, a real requeue is only allowed when the job is currently `completed` or `paused`.

### 4. Resume the job

Run `bulk_job_start`:

```json
{
  "jobId": 1
}
```

Then monitor with `bulk_job_status` until it completes again.

## Expected behavior

The previously successful `completed`, `partial`, and `skipped` rows are not reprocessed. Only the old monthly-quota failures are reopened. Some retried rows may still end as genuine identity/404 failures after Soundcharts is reached successfully; that is expected.
