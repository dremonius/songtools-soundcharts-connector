import { Pool } from "pg";

function envInt(name, fallback) {
  const n = Number.parseInt(process.env[name] || "", 10);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeName(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export class ArtistDatabase {
  constructor(connectionString = process.env.DATABASE_URL || "") {
    if (!connectionString) throw new Error("DATABASE_URL is required");
    this.pool = new Pool({
      connectionString,
      max: Math.max(1, Math.min(20, envInt("DATABASE_POOL_MAX", 10))),
      connectionTimeoutMillis: envInt("DATABASE_CONNECT_TIMEOUT_MS", 10_000),
      idleTimeoutMillis: envInt("DATABASE_IDLE_TIMEOUT_MS", 30_000)
    });
  }

  async init() {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`
        CREATE TABLE IF NOT EXISTS soundcharts_artists (
          soundcharts_artist_id UUID PRIMARY KEY,
          requested_artist_name TEXT,
          soundcharts_artist_name TEXT,
          match_confidence TEXT,
          first_songtools_date DATE,
          representative_spotify_track_id TEXT,
          representative_soundcharts_song_id UUID,
          representative_track_distributor TEXT,
          representative_track_genres JSONB NOT NULL DEFAULT '[]'::jsonb,
          day1_target_date DATE,
          day1_observation_date DATE,
          day1_monthly_listeners BIGINT,
          day30_target_date DATE,
          day30_observation_date DATE,
          day30_monthly_listeners BIGINT,
          latest_observation_date DATE,
          latest_monthly_listeners BIGINT,
          total_tracks_released BIGINT,
          historical_refreshed_at TIMESTAMPTZ,
          current_refreshed_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      await client.query(`
        CREATE TABLE IF NOT EXISTS track_resolutions (
          spotify_track_id TEXT NOT NULL,
          requested_artist_name_normalized TEXT NOT NULL DEFAULT '',
          requested_artist_name TEXT,
          soundcharts_song_id UUID,
          song_name TEXT,
          song_credit_name TEXT,
          distributor TEXT,
          soundcharts_artist_id UUID REFERENCES soundcharts_artists(soundcharts_artist_id) ON DELETE SET NULL,
          soundcharts_artist_name TEXT,
          match_confidence TEXT,
          resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (spotify_track_id, requested_artist_name_normalized)
        )
      `);
      await client.query(`
        CREATE TABLE IF NOT EXISTS listener_observations (
          soundcharts_artist_id UUID NOT NULL REFERENCES soundcharts_artists(soundcharts_artist_id) ON DELETE CASCADE,
          metric TEXT NOT NULL,
          observation_date DATE NOT NULL,
          value BIGINT NOT NULL,
          source_context TEXT NOT NULL,
          fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (soundcharts_artist_id, metric, observation_date, source_context)
        )
      `);
      await client.query(`
        CREATE TABLE IF NOT EXISTS promoted_track_cache (
          spotify_track_id TEXT PRIMARY KEY,
          soundcharts_song_id UUID,
          song_name TEXT,
          distributor TEXT,
          fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      await client.query(`
        CREATE TABLE IF NOT EXISTS enrichment_runs (
          id BIGSERIAL PRIMARY KEY,
          tool_name TEXT NOT NULL,
          item_count INTEGER NOT NULL DEFAULT 0,
          cache_hits INTEGER NOT NULL DEFAULT 0,
          current_refreshes INTEGER NOT NULL DEFAULT 0,
          full_enrichments INTEGER NOT NULL DEFAULT 0,
          failures INTEGER NOT NULL DEFAULT 0,
          metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
          started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          completed_at TIMESTAMPTZ
        )
      `);
      await client.query("CREATE INDEX IF NOT EXISTS idx_soundcharts_artists_first_date ON soundcharts_artists(first_songtools_date)");
      await client.query("CREATE INDEX IF NOT EXISTS idx_track_resolutions_artist_id ON track_resolutions(soundcharts_artist_id)");
      await client.query("CREATE INDEX IF NOT EXISTS idx_listener_observations_artist_date ON listener_observations(soundcharts_artist_id, observation_date DESC)");
      await client.query("COMMIT");
      return true;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async ping() {
    const result = await this.pool.query("SELECT NOW() AS now");
    return { ok: true, now: result.rows[0]?.now ?? null };
  }

  async status() {
    const result = await this.pool.query(`
      SELECT
        (SELECT COUNT(*)::bigint FROM soundcharts_artists) AS artists,
        (SELECT COUNT(*)::bigint FROM track_resolutions) AS track_resolutions,
        (SELECT COUNT(*)::bigint FROM listener_observations) AS listener_observations,
        (SELECT COUNT(*)::bigint FROM enrichment_runs) AS enrichment_runs
    `);
    const row = result.rows[0] || {};
    return {
      ok: true,
      artists: Number(row.artists || 0),
      trackResolutions: Number(row.track_resolutions || 0),
      listenerObservations: Number(row.listener_observations || 0),
      enrichmentRuns: Number(row.enrichment_runs || 0)
    };
  }

  async getCachedArtist({ soundchartsArtistId = null, spotifyTrackId = null, artistName = null } = {}) {
    if (soundchartsArtistId) {
      const result = await this.pool.query(
        "SELECT * FROM soundcharts_artists WHERE soundcharts_artist_id = $1 LIMIT 1",
        [soundchartsArtistId]
      );
      return result.rows[0] || null;
    }

    if (spotifyTrackId) {
      const normalized = normalizeName(artistName);
      const result = await this.pool.query(`
        SELECT a.*, t.spotify_track_id AS resolved_spotify_track_id,
               t.soundcharts_song_id AS resolved_soundcharts_song_id,
               t.distributor AS resolved_distributor,
               t.match_confidence AS resolved_match_confidence
        FROM track_resolutions t
        LEFT JOIN soundcharts_artists a ON a.soundcharts_artist_id = t.soundcharts_artist_id
        WHERE t.spotify_track_id = $1
          AND t.requested_artist_name_normalized = $2
        LIMIT 1
      `, [spotifyTrackId, normalized]);
      return result.rows[0] || null;
    }

    return null;
  }

  async getTrackResolution(spotifyTrackId, artistName = null) {
    const normalized = normalizeName(artistName);
    const result = await this.pool.query(`
      SELECT * FROM track_resolutions
      WHERE spotify_track_id = $1
        AND requested_artist_name_normalized = $2
      LIMIT 1
    `, [spotifyTrackId, normalized]);
    return result.rows[0] || null;
  }

  async saveResolution(result) {
    if (!result?.spotifyTrackId) return;

    if (result.soundchartsArtistId) {
      await this.pool.query(`
        INSERT INTO soundcharts_artists (
          soundcharts_artist_id, requested_artist_name, soundcharts_artist_name, match_confidence,
          representative_spotify_track_id, representative_soundcharts_song_id,
          representative_track_distributor, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
        ON CONFLICT (soundcharts_artist_id) DO UPDATE SET
          requested_artist_name = COALESCE(EXCLUDED.requested_artist_name, soundcharts_artists.requested_artist_name),
          soundcharts_artist_name = COALESCE(EXCLUDED.soundcharts_artist_name, soundcharts_artists.soundcharts_artist_name),
          match_confidence = COALESCE(EXCLUDED.match_confidence, soundcharts_artists.match_confidence),
          representative_spotify_track_id = COALESCE(EXCLUDED.representative_spotify_track_id, soundcharts_artists.representative_spotify_track_id),
          representative_soundcharts_song_id = COALESCE(EXCLUDED.representative_soundcharts_song_id, soundcharts_artists.representative_soundcharts_song_id),
          representative_track_distributor = COALESCE(EXCLUDED.representative_track_distributor, soundcharts_artists.representative_track_distributor),
          updated_at = NOW()
      `, [
        result.soundchartsArtistId,
        result.artistName ?? null,
        result.soundchartsArtistName ?? null,
        result.artistMatchConfidence ?? null,
        result.spotifyTrackId,
        result.soundchartsSongId ?? null,
        result.distributor ?? result.representativeTrackDistributor ?? null
      ]);
    }

    await this.pool.query(`
      INSERT INTO track_resolutions (
        spotify_track_id, requested_artist_name_normalized, requested_artist_name,
        soundcharts_song_id, song_name, song_credit_name, distributor,
        soundcharts_artist_id, soundcharts_artist_name, match_confidence,
        resolved_at, updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),NOW())
      ON CONFLICT (spotify_track_id, requested_artist_name_normalized) DO UPDATE SET
        requested_artist_name = EXCLUDED.requested_artist_name,
        soundcharts_song_id = EXCLUDED.soundcharts_song_id,
        song_name = EXCLUDED.song_name,
        song_credit_name = EXCLUDED.song_credit_name,
        distributor = EXCLUDED.distributor,
        soundcharts_artist_id = EXCLUDED.soundcharts_artist_id,
        soundcharts_artist_name = EXCLUDED.soundcharts_artist_name,
        match_confidence = EXCLUDED.match_confidence,
        resolved_at = NOW(),
        updated_at = NOW()
    `, [
      result.spotifyTrackId,
      normalizeName(result.artistName),
      result.artistName ?? null,
      result.soundchartsSongId ?? null,
      result.songName ?? null,
      result.songCreditName ?? null,
      result.distributor ?? result.representativeTrackDistributor ?? null,
      result.soundchartsArtistId ?? null,
      result.soundchartsArtistName ?? null,
      result.artistMatchConfidence ?? null
    ]);
  }

  async saveEnrichment(result, input, { historicalFetched = false, currentFetched = false } = {}) {
    if (!result?.soundchartsArtistId) return;

    const historicalRefreshedAt = historicalFetched ? new Date() : null;
    const currentRefreshedAt = currentFetched ? new Date() : null;
    const firstSongtoolsDate = input?.firstSongtoolsDate ?? null;

    await this.pool.query(`
      INSERT INTO soundcharts_artists (
        soundcharts_artist_id, requested_artist_name, soundcharts_artist_name, match_confidence,
        first_songtools_date, representative_spotify_track_id, representative_soundcharts_song_id,
        representative_track_distributor, representative_track_genres,
        day1_target_date, day1_observation_date, day1_monthly_listeners,
        day30_target_date, day30_observation_date, day30_monthly_listeners,
        latest_observation_date, latest_monthly_listeners, total_tracks_released,
        historical_refreshed_at, current_refreshed_at, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,NOW()
      )
      ON CONFLICT (soundcharts_artist_id) DO UPDATE SET
        requested_artist_name = COALESCE(EXCLUDED.requested_artist_name, soundcharts_artists.requested_artist_name),
        soundcharts_artist_name = COALESCE(EXCLUDED.soundcharts_artist_name, soundcharts_artists.soundcharts_artist_name),
        match_confidence = COALESCE(EXCLUDED.match_confidence, soundcharts_artists.match_confidence),
        first_songtools_date = CASE WHEN EXCLUDED.historical_refreshed_at IS NOT NULL THEN EXCLUDED.first_songtools_date ELSE COALESCE(soundcharts_artists.first_songtools_date, EXCLUDED.first_songtools_date) END,
        representative_spotify_track_id = COALESCE(EXCLUDED.representative_spotify_track_id, soundcharts_artists.representative_spotify_track_id),
        representative_soundcharts_song_id = COALESCE(EXCLUDED.representative_soundcharts_song_id, soundcharts_artists.representative_soundcharts_song_id),
        representative_track_distributor = COALESCE(EXCLUDED.representative_track_distributor, soundcharts_artists.representative_track_distributor),
        representative_track_genres = CASE WHEN jsonb_array_length(EXCLUDED.representative_track_genres) > 0 THEN EXCLUDED.representative_track_genres ELSE soundcharts_artists.representative_track_genres END,
        day1_target_date = CASE WHEN EXCLUDED.historical_refreshed_at IS NOT NULL THEN EXCLUDED.day1_target_date ELSE soundcharts_artists.day1_target_date END,
        day1_observation_date = CASE WHEN EXCLUDED.historical_refreshed_at IS NOT NULL THEN EXCLUDED.day1_observation_date ELSE soundcharts_artists.day1_observation_date END,
        day1_monthly_listeners = CASE WHEN EXCLUDED.historical_refreshed_at IS NOT NULL THEN EXCLUDED.day1_monthly_listeners ELSE soundcharts_artists.day1_monthly_listeners END,
        day30_target_date = CASE WHEN EXCLUDED.historical_refreshed_at IS NOT NULL THEN EXCLUDED.day30_target_date ELSE soundcharts_artists.day30_target_date END,
        day30_observation_date = CASE WHEN EXCLUDED.historical_refreshed_at IS NOT NULL THEN EXCLUDED.day30_observation_date ELSE soundcharts_artists.day30_observation_date END,
        day30_monthly_listeners = CASE WHEN EXCLUDED.historical_refreshed_at IS NOT NULL THEN EXCLUDED.day30_monthly_listeners ELSE soundcharts_artists.day30_monthly_listeners END,
        latest_observation_date = CASE WHEN EXCLUDED.current_refreshed_at IS NOT NULL THEN EXCLUDED.latest_observation_date ELSE soundcharts_artists.latest_observation_date END,
        latest_monthly_listeners = CASE WHEN EXCLUDED.current_refreshed_at IS NOT NULL THEN EXCLUDED.latest_monthly_listeners ELSE soundcharts_artists.latest_monthly_listeners END,
        total_tracks_released = COALESCE(EXCLUDED.total_tracks_released, soundcharts_artists.total_tracks_released),
        historical_refreshed_at = COALESCE(EXCLUDED.historical_refreshed_at, soundcharts_artists.historical_refreshed_at),
        current_refreshed_at = COALESCE(EXCLUDED.current_refreshed_at, soundcharts_artists.current_refreshed_at),
        updated_at = NOW()
    `, [
      result.soundchartsArtistId,
      input?.artistName ?? result.artistName ?? null,
      result.soundchartsArtistName ?? null,
      result.artistMatchConfidence ?? null,
      firstSongtoolsDate,
      input?.spotifyTrackId ?? result.spotifyTrackId ?? null,
      result.soundchartsSongId ?? null,
      result.representativeTrackDistributor ?? null,
      JSON.stringify(Array.isArray(result.representativeTrackGenres) ? result.representativeTrackGenres : []),
      result.day1TargetDate ?? null,
      result.day1ObservationDate ?? null,
      result.spotifyMonthlyListenersDay1 ?? null,
      result.day30TargetDate ?? null,
      result.day30ObservationDate ?? null,
      result.spotifyMonthlyListenersDay30 ?? null,
      result.todayObservationDate ?? null,
      result.spotifyMonthlyListenersToday ?? null,
      result.totalTracksReleased ?? null,
      historicalRefreshedAt,
      currentRefreshedAt
    ]);

    if (input?.spotifyTrackId || result.spotifyTrackId) {
      await this.saveResolution({
        artistName: input?.artistName ?? result.artistName ?? null,
        spotifyTrackId: input?.spotifyTrackId ?? result.spotifyTrackId,
        soundchartsSongId: result.soundchartsSongId ?? null,
        songName: result.songName ?? null,
        songCreditName: result.songCreditName ?? null,
        distributor: result.representativeTrackDistributor ?? null,
        soundchartsArtistId: result.soundchartsArtistId,
        soundchartsArtistName: result.soundchartsArtistName ?? null,
        artistMatchConfidence: result.artistMatchConfidence ?? null
      });
    }

    const observations = [
      [result.day1ObservationDate, result.spotifyMonthlyListenersDay1, "day1"],
      [result.day30ObservationDate, result.spotifyMonthlyListenersDay30, "day30"],
      [result.todayObservationDate, result.spotifyMonthlyListenersToday, "current"]
    ];

    for (const [date, value, context] of observations) {
      if (!date || value === null || value === undefined) continue;
      await this.pool.query(`
        INSERT INTO listener_observations (
          soundcharts_artist_id, metric, observation_date, value, source_context, fetched_at
        ) VALUES ($1,'spotify_monthly_listeners',$2,$3,$4,NOW())
        ON CONFLICT (soundcharts_artist_id, metric, observation_date, source_context) DO UPDATE SET
          value = EXCLUDED.value,
          fetched_at = NOW()
      `, [result.soundchartsArtistId, date, value, context]);
    }
  }

  async getPromotedTrack(spotifyTrackId) {
    const result = await this.pool.query(
      "SELECT * FROM promoted_track_cache WHERE spotify_track_id = $1 LIMIT 1",
      [spotifyTrackId]
    );
    return result.rows[0] || null;
  }

  async savePromotedTrack(track) {
    if (!track?.spotifyTrackId) return;
    await this.pool.query(`
      INSERT INTO promoted_track_cache (
        spotify_track_id, soundcharts_song_id, song_name, distributor, fetched_at, updated_at
      ) VALUES ($1,$2,$3,$4,NOW(),NOW())
      ON CONFLICT (spotify_track_id) DO UPDATE SET
        soundcharts_song_id = EXCLUDED.soundcharts_song_id,
        song_name = EXCLUDED.song_name,
        distributor = EXCLUDED.distributor,
        fetched_at = NOW(),
        updated_at = NOW()
    `, [
      track.spotifyTrackId,
      track.soundchartsSongId ?? null,
      track.songName ?? null,
      track.distributor ?? null
    ]);
  }

  async startRun(toolName, itemCount, metadata = {}) {
    const result = await this.pool.query(`
      INSERT INTO enrichment_runs (tool_name, item_count, metadata)
      VALUES ($1,$2,$3::jsonb)
      RETURNING id
    `, [toolName, itemCount, JSON.stringify(metadata || {})]);
    return Number(result.rows[0]?.id);
  }

  async finishRun(runId, stats = {}, metadata = {}) {
    if (!runId) return;
    await this.pool.query(`
      UPDATE enrichment_runs SET
        cache_hits = $2,
        current_refreshes = $3,
        full_enrichments = $4,
        failures = $5,
        metadata = metadata || $6::jsonb,
        completed_at = NOW()
      WHERE id = $1
    `, [
      runId,
      stats.cacheHits || 0,
      stats.currentRefreshes || 0,
      stats.fullEnrichments || 0,
      stats.failures || 0,
      JSON.stringify(metadata || {})
    ]);
  }

  async close() {
    await this.pool.end();
  }
}
