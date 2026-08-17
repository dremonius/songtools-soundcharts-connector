# Songtools Soundcharts Connector

A small, read-only remote MCP server for batch-enriching Songtools artist data with Soundcharts.

## What it exposes

- `soundcharts_quota` — check current Soundcharts quota/rate-limit consumption before a large run.
- `resolve_spotify_tracks` — resolve up to 500 Spotify track IDs to Soundcharts song/artist IDs and representative-track distributor.
- `enrich_artists` — enrich up to 500 artists per tool call with:
  - Soundcharts artist ID
  - Spotify monthly listeners nearest Songtools Day 1
  - Spotify monthly listeners nearest Songtools Day 30
  - latest available Spotify monthly listeners
  - total main-performer tracks released
  - distributor on the representative promoted track
- `refresh_artists_current` — refresh already-resolved artists using only cached Soundcharts IDs; returns latest listeners + current catalog count.
- `collect_promoted_track_distributors` — optional second pass across up to 100 supplied promoted Spotify tracks per artist to list distinct distributors.

All tools are read-only.

## Why the connector is batch-oriented

The Soundcharts REST API is still entity-oriented. This server accepts large batches from ChatGPT and performs those REST calls concurrently inside the connector. It caches the Soundcharts OAuth access token in memory and lets the Songtools workbook/database cache permanent Soundcharts artist IDs and historical Day 1/Day 30 values.

The minimum first-time enrichment is approximately four Soundcharts requests per artist:

1. representative Spotify track -> Soundcharts song/artist
2. one historical monthly-listener window covering Day 1 and Day 30
3. one recent monthly-listener window for the latest value
4. one artist-song request with `limit=1` to read `page.total`

Once `soundchartsArtistId`, Day 1 and Day 30 are cached, refreshes can skip track resolution and historical history if the caller only requests current fields in a later connector version.

## Soundcharts authentication

### Recommended: OAuth client credentials

Set:

```bash
SOUNDCHARTS_CLIENT_ID=...
SOUNDCHARTS_CLIENT_SECRET=...
SOUNDCHARTS_TEAM_ID=...   # optional
```

The server requests short-lived access tokens from Soundcharts and never exposes the client secret to ChatGPT.

### Legacy credentials

If your Soundcharts account still uses legacy credentials:

```bash
SOUNDCHARTS_APP_ID=...
SOUNDCHARTS_API_KEY=...
```

Do not set both methods unless you intend OAuth to take precedence.

## Run locally

Requires Node.js 20+.

```bash
cp .env.example .env
# fill in Soundcharts credentials
set -a; source .env; set +a
npm install
npm start
```

Health check:

```bash
curl http://localhost:3000/health
```

The remote MCP endpoint is:

```text
http://localhost:3000/mcp
```

ChatGPT cannot connect directly to localhost; deploy the server or use an OpenAI-supported secure MCP tunnel.

## Deploy on Render

1. Put this folder in a private GitHub repository.
2. In Render, create a Blueprint/Web Service from the repository (`render.yaml` is included).
3. Add `SOUNDCHARTS_CLIENT_ID` and `SOUNDCHARTS_CLIENT_SECRET` as secret environment variables. Add `SOUNDCHARTS_TEAM_ID` only if required by your account.
4. Render generates `MCP_BEARER_TOKEN`; copy it to your password manager.
5. Confirm `https://YOUR-SERVICE.onrender.com/health` returns `ok: true`.
6. Your MCP endpoint is `https://YOUR-SERVICE.onrender.com/mcp`.

If your ChatGPT/OpenAI client cannot send a custom Authorization header to an MCP server, leave `MCP_BEARER_TOKEN` unset only for a short test, or put the service behind a supported OAuth/gateway setup. Do not leave a credential-bearing proxy openly usable on the internet long-term.

## OpenAI Responses API test

Remote MCP tools can be supplied to the Responses API with a `server_url` and optional headers. A typical configuration is conceptually:

```js
const tools = [{
  type: "mcp",
  server_label: "songtools_soundcharts",
  server_url: "https://YOUR-SERVICE.onrender.com/mcp",
  headers: {
    Authorization: `Bearer ${process.env.MCP_BEARER_TOKEN}`
  },
  allowed_tools: [
    "soundcharts_quota",
    "resolve_spotify_tracks",
    "enrich_artists",
    "refresh_artists_current",
    "collect_promoted_track_distributors"
  ],
  require_approval: "never"
}];
```

The connector itself is read-only, but you should still restrict access because it consumes your Soundcharts quota.

## ChatGPT custom app

Current ChatGPT custom MCP availability depends on plan/workspace. In eligible accounts, enable Developer Mode, create a custom app, supply the deployed MCP endpoint, choose the available authentication mechanism, scan tools, and test the draft app.

If your ChatGPT plan does not support custom MCP apps, the same server can still be used through an OpenAI API application that enables Remote MCP, or you can use the included code after moving to an eligible plan/workspace.

## Data semantics

### Monthly listeners

Soundcharts can have missing observation days. For Day 1 and Day 30 the connector requests one range from Day -7 through Day 37 and returns the closest actual observation to each target date. Both target and observation dates are returned.

For “Today,” the connector requests the last 14 calendar days and returns the latest available Soundcharts observation; the observation date is included.

### Track count

`totalTracksReleased` uses Soundcharts' artist songs endpoint with `mainPerformer=1`, `limit=1`, and reads `page.total`. This avoids downloading the whole catalog just to count it.

### Distributor

The primary enrichment returns the distributor attached by Soundcharts to the representative promoted track. Soundcharts labels distributor data as Beta. If you need multiple distributors, call `collect_promoted_track_distributors` with the Spotify IDs of all Songtools-promoted tracks for that artist. Exhaustively scanning every catalog track would cost substantially more API requests because distributor is song metadata.

## Recommended Songtools workflow

First run:

1. Check `soundcharts_quota`.
2. Send artists to `enrich_artists` in batches of up to 500.
3. Persist `soundchartsArtistId`, Day 1 values/dates, Day 30 values/dates.
4. Optionally run promoted-track distributor collection as a second pass.

Future refreshes should reuse Soundcharts artist IDs and historical values rather than resolving/fetching them again. Use `refresh_artists_current` for that current-only pass.
