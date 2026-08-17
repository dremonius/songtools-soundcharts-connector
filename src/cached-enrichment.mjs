import {
  enrichArtists,
  resolveSpotifyTracks,
  refreshArtistsCurrent,
  collectPromotedTrackDistributors
} from "./enrichment.mjs";

function envNumber(name, fallback) {
  const n = Number(process.env[name]);
  return Number.isFinite(n) ? n : fallback;
}

function dateOnly(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const s = String(value);
  return s.length >= 10 ? s.slice(0, 10) : s;
}

function isFresh(timestamp, hours) {
  if (!timestamp) return false;
  const t = new Date(timestamp).getTime();
  if (!Number.isFinite(t)) return false;
  return Date.now() - t < hours * 60 * 60 * 1000;
}

function cachedToResult(row, input = {}) {
  if (!row?.soundcharts_artist_id) return null;
  return {
    ok: true,
    artistName: input.artistName ?? row.requested_artist_name ?? null,
    spotifyTrackId: input.spotifyTrackId ?? row.representative_spotify_track_id ?? row.resolved_spotify_track_id ?? null,
    soundchartsSongId: row.representative_soundcharts_song_id ?? row.resolved_soundcharts_song_id ?? null,
    soundchartsArtistId: row.soundcharts_artist_id,
    soundchartsArtistName: row.soundcharts_artist_name ?? null,
    artistMatchConfidence: row.match_confidence ?? row.resolved_match_confidence ?? null,
    representativeTrackDistributor: row.representative_track_distributor ?? row.resolved_distributor ?? null,
    representativeTrackGenres: Array.isArray(row.representative_track_genres) ? row.representative_track_genres : [],
    spotifyMonthlyListenersDay1: row.day1_monthly_listeners === null || row.day1_monthly_listeners === undefined ? null : Number(row.day1_monthly_listeners),
    day1TargetDate: dateOnly(row.day1_target_date),
    day1ObservationDate: dateOnly(row.day1_observation_date),
    spotifyMonthlyListenersDay30: row.day30_monthly_listeners === null || row.day30_monthly_listeners === undefined ? null : Number(row.day30_monthly_listeners),
    day30TargetDate: dateOnly(row.day30_target_date),
    day30ObservationDate: dateOnly(row.day30_observation_date),
    spotifyMonthlyListenersToday: row.latest_monthly_listeners === null || row.latest_monthly_listeners === undefined ? null : Number(row.latest_monthly_listeners),
    todayObservationDate: dateOnly(row.latest_observation_date),
    totalTracksReleased: row.total_tracks_released === null || row.total_tracks_released === undefined ? null : Number(row.total_tracks_released),
    distributorScope: "representative_track"
  };
}

function mergeFreshWithCached(fresh, cached, input) {
  const prior = cachedToResult(cached, input) || {};
  return {
    ...prior,
    ...fresh,
    artistName: input.artistName ?? fresh.artistName ?? prior.artistName ?? null,
    spotifyTrackId: input.spotifyTrackId ?? fresh.spotifyTrackId ?? prior.spotifyTrackId ?? null,
    soundchartsSongId: fresh.soundchartsSongId ?? prior.soundchartsSongId ?? null,
    soundchartsArtistName: fresh.soundchartsArtistName ?? prior.soundchartsArtistName ?? null,
    artistMatchConfidence: fresh.artistMatchConfidence ?? prior.artistMatchConfidence ?? null,
    representativeTrackDistributor: fresh.representativeTrackDistributor ?? prior.representativeTrackDistributor ?? null,
    representativeTrackGenres: Array.isArray(fresh.representativeTrackGenres) && fresh.representativeTrackGenres.length
      ? fresh.representativeTrackGenres
      : (prior.representativeTrackGenres || [])
  };
}

async function mapLimit(items, limit, worker) {
  const results = new Array(items.length);
  let cursor = 0;
  const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const i = cursor++;
      if (i >= items.length) return;
      try {
        results[i] = await worker(items[i], i);
      } catch (error) {
        results[i] = {
          ok: false,
          input: items[i],
          error: {
            name: error?.name || "Error",
            message: error?.message || String(error),
            status: error?.status ?? null,
            body: error?.body ?? null
          }
        };
      }
    }
  });
  await Promise.all(runners);
  return results;
}

function summarize(results) {
  return {
    cacheHits: results.filter((r) => r?.cacheStatus === "hit").length,
    currentRefreshes: results.filter((r) => r?.cacheStatus === "current_refreshed").length,
    fullEnrichments: results.filter((r) => r?.cacheStatus === "full_enrichment" || r?.cacheStatus === "api_resolved").length,
    failures: results.filter((r) => !r?.ok).length
  };
}

export async function resolveSpotifyTracksCached(client, database, items, concurrency = 20) {
  const runId = database ? await database.startRun("resolve_spotify_tracks", items.length) : null;
  const results = await mapLimit(items, concurrency, async (item) => {
    if (database) {
      const cached = await database.getTrackResolution(item.spotifyTrackId, item.artistName);
      if (cached) {
        return {
          ok: Boolean(cached.soundcharts_song_id),
          artistName: item.artistName ?? cached.requested_artist_name ?? null,
          spotifyTrackId: item.spotifyTrackId,
          soundchartsSongId: cached.soundcharts_song_id ?? null,
          songName: cached.song_name ?? null,
          songCreditName: cached.song_credit_name ?? null,
          distributor: cached.distributor ?? null,
          soundchartsArtistId: cached.soundcharts_artist_id ?? null,
          soundchartsArtistName: cached.soundcharts_artist_name ?? null,
          artistMatchConfidence: cached.match_confidence ?? null,
          artistCandidates: [],
          cacheStatus: "hit"
        };
      }
    }

    const [fresh] = await resolveSpotifyTracks(client, [item], 1);
    if (fresh?.ok && database) await database.saveResolution(fresh);
    return { ...fresh, cacheStatus: fresh?.ok ? "api_resolved" : "api_error" };
  });

  if (database && runId) await database.finishRun(runId, summarize(results));
  return results;
}

export async function enrichArtistsCached(client, database, items, concurrency = 20) {
  const currentCacheHours = Math.max(0, envNumber("CURRENT_CACHE_HOURS", 20));
  const runId = database ? await database.startRun("enrich_artists", items.length, { currentCacheHours }) : null;

  const results = await mapLimit(items, concurrency, async (input) => {
    const cached = database ? await database.getCachedArtist(input) : null;
    const requestedFirstDate = dateOnly(input.firstSongtoolsDate);
    const cachedFirstDate = dateOnly(cached?.first_songtools_date);
    const historicalHit = Boolean(
      cached?.soundcharts_artist_id &&
      cached?.historical_refreshed_at &&
      requestedFirstDate &&
      cachedFirstDate === requestedFirstDate
    );
    const currentHit = Boolean(cached?.current_refreshed_at && isFresh(cached.current_refreshed_at, currentCacheHours));
    const forceRefreshCurrent = Boolean(input.forceRefreshCurrent);

    if (historicalHit && currentHit && !forceRefreshCurrent) {
      return { ...cachedToResult(cached, input), cacheStatus: "hit" };
    }

    if (historicalHit && cached?.soundcharts_artist_id) {
      const [freshCurrent] = await refreshArtistsCurrent(client, [{
        artistName: input.artistName,
        soundchartsArtistId: cached.soundcharts_artist_id
      }], 1);
      if (!freshCurrent?.ok) return { ...freshCurrent, cacheStatus: "api_error" };
      const merged = mergeFreshWithCached(freshCurrent, cached, input);
      if (database) {
        await database.saveEnrichment(merged, input, { historicalFetched: false, currentFetched: true });
      }
      return { ...merged, cacheStatus: "current_refreshed" };
    }

    const effectiveInput = {
      ...input,
      soundchartsArtistId: input.soundchartsArtistId ?? cached?.soundcharts_artist_id ?? undefined
    };
    delete effectiveInput.forceRefreshCurrent;

    const [fresh] = await enrichArtists(client, [effectiveInput], 1);
    if (!fresh?.ok) return { ...fresh, cacheStatus: "api_error" };
    const merged = mergeFreshWithCached(fresh, cached, input);
    if (database) {
      await database.saveEnrichment(merged, input, { historicalFetched: true, currentFetched: true });
    }
    return { ...merged, cacheStatus: "full_enrichment" };
  });

  if (database && runId) await database.finishRun(runId, summarize(results));
  return results;
}

export async function refreshArtistsCurrentCached(client, database, items, concurrency = 20) {
  const currentCacheHours = Math.max(0, envNumber("CURRENT_CACHE_HOURS", 20));
  const runId = database ? await database.startRun("refresh_artists_current", items.length, { currentCacheHours }) : null;

  const results = await mapLimit(items, concurrency, async (input) => {
    const cached = database ? await database.getCachedArtist(input) : null;
    if (cached?.current_refreshed_at && isFresh(cached.current_refreshed_at, currentCacheHours) && !input.forceRefresh) {
      return { ...cachedToResult(cached, input), cacheStatus: "hit" };
    }

    const soundchartsArtistId = input.soundchartsArtistId ?? cached?.soundcharts_artist_id;
    if (!soundchartsArtistId) throw new Error("soundchartsArtistId is required");
    const [fresh] = await refreshArtistsCurrent(client, [{
      artistName: input.artistName,
      soundchartsArtistId
    }], 1);
    if (!fresh?.ok) return { ...fresh, cacheStatus: "api_error" };
    const merged = mergeFreshWithCached(fresh, cached, input);
    if (database) {
      await database.saveEnrichment(merged, input, { historicalFetched: false, currentFetched: true });
    }
    return { ...merged, cacheStatus: "current_refreshed" };
  });

  if (database && runId) await database.finishRun(runId, summarize(results));
  return results;
}

export async function collectPromotedTrackDistributorsCached(client, database, items, concurrency = 20) {
  const distributorCacheDays = Math.max(0, envNumber("DISTRIBUTOR_CACHE_DAYS", 30));
  const runId = database ? await database.startRun("collect_promoted_track_distributors", items.length, { distributorCacheDays }) : null;

  const results = await mapLimit(items, concurrency, async (item) => {
    const trackIds = [...new Set((item.spotifyTrackIds || []).filter(Boolean))];
    const cachedTracks = [];
    const missingTrackIds = [];

    for (const spotifyTrackId of trackIds) {
      const cached = database ? await database.getPromotedTrack(spotifyTrackId) : null;
      const freshEnough = cached?.fetched_at && isFresh(cached.fetched_at, distributorCacheDays * 24);
      if (cached && freshEnough) {
        cachedTracks.push({
          spotifyTrackId,
          soundchartsSongId: cached.soundcharts_song_id ?? null,
          songName: cached.song_name ?? null,
          distributor: cached.distributor ?? null,
          cacheStatus: "hit"
        });
      } else {
        missingTrackIds.push(spotifyTrackId);
      }
    }

    let freshTracks = [];
    if (missingTrackIds.length) {
      const [freshGroup] = await collectPromotedTrackDistributors(client, [{
        artistName: item.artistName,
        spotifyTrackIds: missingTrackIds
      }], Math.min(concurrency, 10));
      if (!freshGroup?.ok) return { ...freshGroup, cacheStatus: "api_error" };
      freshTracks = (freshGroup.tracks || []).map((track) => ({ ...track, cacheStatus: "api_resolved" }));
      if (database) {
        for (const track of freshTracks) await database.savePromotedTrack(track);
      }
    }

    const tracks = [...cachedTracks, ...freshTracks];
    const distributors = [...new Set(tracks.map((t) => t?.distributor).filter(Boolean))].sort();
    return {
      ok: true,
      artistName: item.artistName ?? null,
      distributors,
      distributorScope: "promoted_tracks_provided",
      tracks,
      cacheStatus: missingTrackIds.length ? "current_refreshed" : "hit"
    };
  });

  if (database && runId) await database.finishRun(runId, summarize(results));
  return results;
}
