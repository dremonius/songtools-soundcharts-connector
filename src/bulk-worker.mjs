import {
  enrichArtistsCached,
  resolveSpotifyTracksCached,
  refreshArtistsCurrentCached
} from "./cached-enrichment.mjs";

function envInt(name, fallback, min = 0, max = Number.MAX_SAFE_INTEGER) {
  const n = Number.parseInt(process.env[name] || "", 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function envBool(name, fallback = true) {
  const raw = String(process.env[name] ?? "").trim().toLowerCase();
  if (!raw) return fallback;
  return !["0", "false", "no", "off"].includes(raw);
}

function dateOnly(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const s = String(value);
  return s.length >= 10 ? s.slice(0, 10) : s;
}

function errorMessage(result, fallback = "Unknown enrichment error") {
  return result?.error?.message || result?.message || fallback;
}

function retryable(result) {
  const status = Number(result?.error?.status);
  if (status === 429 || status >= 500) return true;
  const message = String(errorMessage(result, "")).toLowerCase();
  return /(timeout|timed out|network|fetch failed|econnreset|econnrefused|socket|temporary|temporarily|rate limit)/.test(message);
}

async function finalizeFailure(database, row, result, maxAttempts) {
  const message = errorMessage(result);
  if (row.attempts < maxAttempts && retryable(result)) {
    await database.finishBulkItem(row.id, "pending", {
      result,
      resultStatus: "retry_pending",
      error: message
    });
    return "retried";
  }
  await database.finishBulkItem(row.id, "failed", {
    result,
    resultStatus: result?.cacheStatus || "failed",
    error: message
  });
  return "failed";
}

async function processFullItems({ client, database, rows, concurrency, maxAttempts }) {
  if (!rows.length) return;
  let results;
  try {
    results = await enrichArtistsCached(
      client,
      database,
      rows.map((row) => ({
        artistName: row.artist_name,
        spotifyTrackId: row.spotify_track_id,
        firstSongtoolsDate: dateOnly(row.first_songtools_date)
      })),
      concurrency
    );
  } catch (error) {
    results = rows.map(() => ({ ok: false, error: { message: error?.message || String(error), status: error?.status ?? null } }));
  }

  await Promise.all(rows.map(async (row, index) => {
    const result = results[index];
    if (result?.ok) {
      await database.finishBulkItem(row.id, "completed", {
        result,
        resultStatus: result.cacheStatus || "full_enrichment",
        error: null
      });
    } else {
      await finalizeFailure(database, row, result, maxAttempts);
    }
  }));
}

async function processCurrentOnlyItems({ client, database, rows, concurrency, maxAttempts }) {
  if (!rows.length) return;
  let resolutions;
  try {
    resolutions = await resolveSpotifyTracksCached(
      client,
      database,
      rows.map((row) => ({ artistName: row.artist_name, spotifyTrackId: row.spotify_track_id })),
      concurrency
    );
  } catch (error) {
    resolutions = rows.map(() => ({ ok: false, error: { message: error?.message || String(error), status: error?.status ?? null } }));
  }

  const refreshRows = [];
  const refreshInputs = [];
  const resolutionByItemId = new Map();

  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i];
    const resolution = resolutions[i];
    if (!resolution?.ok) {
      await finalizeFailure(database, row, resolution, maxAttempts);
      continue;
    }
    if (!resolution.soundchartsArtistId) {
      await database.finishBulkItem(row.id, "failed", {
        result: resolution,
        resultStatus: "artist_unresolved",
        error: "Spotify track resolved, but Soundcharts could not identify one artist unambiguously."
      });
      continue;
    }
    refreshRows.push(row);
    refreshInputs.push({
      artistName: row.artist_name,
      soundchartsArtistId: resolution.soundchartsArtistId
    });
    resolutionByItemId.set(row.id, resolution);
  }

  if (!refreshRows.length) return;

  let currentResults;
  try {
    currentResults = await refreshArtistsCurrentCached(client, database, refreshInputs, concurrency);
  } catch (error) {
    currentResults = refreshRows.map(() => ({ ok: false, error: { message: error?.message || String(error), status: error?.status ?? null } }));
  }

  await Promise.all(refreshRows.map(async (row, index) => {
    const current = currentResults[index];
    const resolution = resolutionByItemId.get(row.id) || {};
    if (!current?.ok) {
      await finalizeFailure(database, row, current, maxAttempts);
      return;
    }
    const merged = {
      ...resolution,
      ...current,
      artistName: row.artist_name,
      spotifyTrackId: row.spotify_track_id,
      soundchartsSongId: resolution.soundchartsSongId ?? current.soundchartsSongId ?? null,
      soundchartsArtistId: resolution.soundchartsArtistId ?? current.soundchartsArtistId ?? null,
      soundchartsArtistName: resolution.soundchartsArtistName ?? current.soundchartsArtistName ?? null,
      artistMatchConfidence: resolution.artistMatchConfidence ?? current.artistMatchConfidence ?? null,
      representativeTrackDistributor: resolution.distributor ?? current.representativeTrackDistributor ?? null,
      distributorScope: "representative_track",
      cacheStatus: current.cacheStatus || resolution.cacheStatus || "current_only"
    };
    await database.finishBulkItem(row.id, "partial", {
      result: merged,
      resultStatus: "current_only",
      error: "Historical Day 1/Day 30 metrics were not requested because firstSongtoolsDate is unavailable."
    });
  }));
}

async function processBulkBatch({ client, database, rows, concurrency, maxAttempts }) {
  const full = [];
  const currentOnly = [];
  const skipped = [];

  for (const row of rows) {
    if (row.spotify_track_id && row.first_songtools_date) full.push(row);
    else if (row.spotify_track_id) currentOnly.push(row);
    else skipped.push(row);
  }

  if (skipped.length) {
    await Promise.all(skipped.map((row) => database.finishBulkItem(row.id, "skipped", {
      result: null,
      resultStatus: "missing_spotify_track",
      error: row.first_songtools_date
        ? "No representative Spotify track ID is available for this artist."
        : "No representative Spotify track ID or exact first Songtools date is available for this artist."
    })));
  }

  await processFullItems({ client, database, rows: full, concurrency, maxAttempts });
  await processCurrentOnlyItems({ client, database, rows: currentOnly, concurrency, maxAttempts });
}

export function startBulkWorker({ client, database, concurrency = 20 } = {}) {
  if (!client || !database) throw new Error("Bulk worker requires Soundcharts client and database");

  const enabled = envBool("BULK_WORKER_ENABLED", true);
  const batchSize = envInt("BULK_BATCH_SIZE", 50, 1, 500);
  const pollMs = envInt("BULK_POLL_MS", 1500, 250, 60_000);
  const batchDelayMs = envInt("BULK_BATCH_DELAY_MS", 250, 0, 60_000);
  const maxAttempts = envInt("BULK_MAX_ATTEMPTS", 3, 1, 20);
  const staleMinutes = envInt("BULK_STALE_MINUTES", 15, 1, 1440);

  let stopped = false;
  let timer = null;
  let active = Promise.resolve();

  const schedule = (delay = pollMs) => {
    if (stopped || !enabled) return;
    timer = setTimeout(() => {
      active = tick().catch((error) => console.error("Bulk worker tick failed", error));
    }, delay);
    timer.unref?.();
  };

  const tick = async () => {
    if (stopped || !enabled) return;
    let jobId = null;
    try {
      jobId = await database.getRunnableBulkJob();
      if (!jobId) {
        schedule(pollMs);
        return;
      }

      const rows = await database.claimBulkBatch(jobId, batchSize, maxAttempts, staleMinutes);
      if (!rows.length) {
        await database.completeBulkJobIfDone(jobId);
        schedule(pollMs);
        return;
      }

      await processBulkBatch({ client, database, rows, concurrency, maxAttempts });
      await database.completeBulkJobIfDone(jobId);
      schedule(batchDelayMs);
    } catch (error) {
      console.error(`Bulk worker error${jobId ? ` on job ${jobId}` : ""}`, error);
      if (jobId) await database.setBulkJobError(jobId, error?.message || String(error)).catch(() => {});
      schedule(pollMs);
    }
  };

  if (enabled) schedule(500);
  else console.log("Bulk worker disabled by BULK_WORKER_ENABLED");

  return {
    stop() {
      stopped = true;
      if (timer) clearTimeout(timer);
    },
    async waitForIdle() {
      await active.catch(() => {});
    },
    config: { enabled, batchSize, pollMs, batchDelayMs, maxAttempts, staleMinutes, concurrency }
  };
}
