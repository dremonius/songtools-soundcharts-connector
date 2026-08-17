function normalizeName(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function isoDate(value) {
  if (!value) return null;
  const d = new Date(`${String(value).slice(0, 10)}T00:00:00Z`);
  return Number.isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
}

function addDays(dateString, days) {
  const d = new Date(`${dateString}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function todayUtc() {
  return new Date().toISOString().slice(0, 10);
}

function unwrapObject(response) {
  return response?.object ?? response ?? null;
}

function chooseArtistFromSong(song, requestedArtistName) {
  const main = Array.isArray(song?.mainArtists) && song.mainArtists.length ? song.mainArtists : [];
  const all = Array.isArray(song?.artists) ? song.artists : [];
  const candidates = main.length ? main : all;
  if (!candidates.length) return { artist: null, confidence: "none", candidates: [] };
  if (candidates.length === 1) return { artist: candidates[0], confidence: "single", candidates };

  const target = normalizeName(requestedArtistName);
  if (target) {
    const exact = candidates.filter((a) => normalizeName(a?.name) === target);
    if (exact.length === 1) return { artist: exact[0], confidence: "exact_name", candidates };

    const contained = candidates.filter((a) => {
      const n = normalizeName(a?.name);
      return n && (target.includes(n) || n.includes(target));
    });
    if (contained.length === 1) return { artist: contained[0], confidence: "name_contains", candidates };
  }

  return { artist: null, confidence: "ambiguous", candidates };
}

function nearestObservation(items, targetDate) {
  if (!Array.isArray(items) || !items.length || !targetDate) return null;
  const target = new Date(`${targetDate}T00:00:00Z`).getTime();
  let best = null;
  for (const item of items) {
    if (!item?.date || item?.value === undefined || item?.value === null) continue;
    const t = new Date(item.date).getTime();
    if (!Number.isFinite(t)) continue;
    const distance = Math.abs(t - target);
    if (!best || distance < best.distance || (distance === best.distance && t < best.time)) {
      best = {
        date: new Date(t).toISOString().slice(0, 10),
        value: Number(item.value),
        distance,
        time: t
      };
    }
  }
  return best ? { date: best.date, value: best.value } : null;
}

function latestObservation(items) {
  if (!Array.isArray(items) || !items.length) return null;
  let best = null;
  for (const item of items) {
    if (!item?.date || item?.value === undefined || item?.value === null) continue;
    const t = new Date(item.date).getTime();
    if (!Number.isFinite(t)) continue;
    if (!best || t > best.time) {
      best = { date: new Date(t).toISOString().slice(0, 10), value: Number(item.value), time: t };
    }
  }
  return best ? { date: best.date, value: best.value } : null;
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

export async function resolveSpotifyTracks(client, items, concurrency = 20) {
  return mapLimit(items, concurrency, async (item) => {
    const response = await client.getSongBySpotifyTrackId(item.spotifyTrackId);
    const song = unwrapObject(response);
    const chosen = chooseArtistFromSong(song, item.artistName);
    return {
      ok: Boolean(song?.uuid),
      artistName: item.artistName ?? null,
      spotifyTrackId: item.spotifyTrackId,
      soundchartsSongId: song?.uuid ?? null,
      songName: song?.name ?? null,
      songCreditName: song?.creditName ?? null,
      distributor: song?.distributor || null,
      soundchartsArtistId: chosen.artist?.uuid ?? null,
      soundchartsArtistName: chosen.artist?.name ?? null,
      artistMatchConfidence: chosen.confidence,
      artistCandidates: chosen.candidates.map((a) => ({ uuid: a?.uuid ?? null, name: a?.name ?? null }))
    };
  });
}

async function enrichOne(client, input) {
  const firstDate = isoDate(input.firstSongtoolsDate);
  if (!firstDate) throw new Error("firstSongtoolsDate must be a valid YYYY-MM-DD date");

  let song = null;
  let artistUuid = input.soundchartsArtistId || null;
  let matchedArtistName = null;
  let matchConfidence = artistUuid ? "provided_id" : null;

  if (!artistUuid) {
    if (!input.spotifyTrackId) throw new Error("Provide spotifyTrackId or soundchartsArtistId");
    song = unwrapObject(await client.getSongBySpotifyTrackId(input.spotifyTrackId));
    const chosen = chooseArtistFromSong(song, input.artistName);
    if (!chosen.artist?.uuid) {
      const names = chosen.candidates.map((a) => a?.name).filter(Boolean).join(", ");
      throw new Error(`Could not resolve one artist from representative track. Candidates: ${names || "none"}`);
    }
    artistUuid = chosen.artist.uuid;
    matchedArtistName = chosen.artist.name || null;
    matchConfidence = chosen.confidence;
  }

  const day1 = firstDate;
  const day30 = addDays(firstDate, 30);
  const historyStart = addDays(firstDate, -7);
  const historyEnd = addDays(firstDate, 37);
  const currentEnd = todayUtc();
  const currentStart = addDays(currentEnd, -14);

  const [historyResponse, currentResponse, songsResponse] = await Promise.all([
    client.getSpotifyMonthlyListeners(artistUuid, {
      startDate: historyStart,
      endDate: historyEnd,
      sort: "asc",
      limit: 100
    }),
    client.getSpotifyMonthlyListeners(artistUuid, {
      startDate: currentStart,
      endDate: currentEnd,
      sort: "desc",
      limit: 100
    }),
    client.getArtistSongs(artistUuid, { offset: 0, limit: 1, mainPerformer: 1 })
  ]);

  const historyItems = historyResponse?.items || [];
  const currentItems = currentResponse?.items || [];
  const obs1 = nearestObservation(historyItems, day1);
  const obs30 = nearestObservation(historyItems, day30);
  const current = latestObservation(currentItems);

  return {
    ok: true,
    artistName: input.artistName ?? null,
    spotifyTrackId: input.spotifyTrackId ?? null,
    soundchartsSongId: song?.uuid ?? null,
    soundchartsArtistId: artistUuid,
    soundchartsArtistName: matchedArtistName,
    artistMatchConfidence: matchConfidence,
    representativeTrackDistributor: song?.distributor || null,
    representativeTrackGenres: Array.isArray(song?.genres)
      ? song.genres.flatMap((g) => [g?.root, ...(Array.isArray(g?.sub) ? g.sub : [])]).filter(Boolean)
      : [],
    spotifyMonthlyListenersDay1: obs1?.value ?? null,
    day1TargetDate: day1,
    day1ObservationDate: obs1?.date ?? null,
    spotifyMonthlyListenersDay30: obs30?.value ?? null,
    day30TargetDate: day30,
    day30ObservationDate: obs30?.date ?? null,
    spotifyMonthlyListenersToday: current?.value ?? null,
    todayObservationDate: current?.date ?? null,
    totalTracksReleased: songsResponse?.page?.total ?? null,
    distributorScope: "representative_track"
  };
}

export async function enrichArtists(client, items, concurrency = 20) {
  return mapLimit(items, concurrency, async (item) => enrichOne(client, item));
}


export async function refreshArtistsCurrent(client, items, concurrency = 20) {
  return mapLimit(items, concurrency, async (input) => {
    if (!input.soundchartsArtistId) throw new Error("soundchartsArtistId is required");
    const currentEnd = todayUtc();
    const currentStart = addDays(currentEnd, -14);
    const [currentResponse, songsResponse] = await Promise.all([
      client.getSpotifyMonthlyListeners(input.soundchartsArtistId, {
        startDate: currentStart,
        endDate: currentEnd,
        sort: "desc",
        limit: 100
      }),
      client.getArtistSongs(input.soundchartsArtistId, { offset: 0, limit: 1, mainPerformer: 1 })
    ]);
    const current = latestObservation(currentResponse?.items || []);
    return {
      ok: true,
      artistName: input.artistName ?? null,
      soundchartsArtistId: input.soundchartsArtistId,
      spotifyMonthlyListenersToday: current?.value ?? null,
      todayObservationDate: current?.date ?? null,
      totalTracksReleased: songsResponse?.page?.total ?? null
    };
  });
}

export async function collectPromotedTrackDistributors(client, items, concurrency = 20) {
  return mapLimit(items, concurrency, async (item) => {
    const trackIds = [...new Set((item.spotifyTrackIds || []).filter(Boolean))];
    const tracks = await mapLimit(trackIds, Math.min(concurrency, 10), async (spotifyTrackId) => {
      const song = unwrapObject(await client.getSongBySpotifyTrackId(spotifyTrackId));
      return {
        spotifyTrackId,
        soundchartsSongId: song?.uuid ?? null,
        songName: song?.name ?? null,
        distributor: song?.distributor || null
      };
    });
    const distributors = [...new Set(tracks.map((t) => t?.distributor).filter(Boolean))].sort();
    return {
      ok: true,
      artistName: item.artistName ?? null,
      distributors,
      distributorScope: "promoted_tracks_provided",
      tracks
    };
  });
}
