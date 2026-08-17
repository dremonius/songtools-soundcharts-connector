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
      await client.query(`
        CREATE TABLE IF NOT EXISTS bulk_jobs (
          id BIGSERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          source_key TEXT UNIQUE,
          status TEXT NOT NULL DEFAULT 'paused',
          metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          started_at TIMESTAMPTZ,
          completed_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          last_error TEXT
        )
      `);
      await client.query(`
        CREATE TABLE IF NOT EXISTS bulk_job_items (
          id BIGSERIAL PRIMARY KEY,
          job_id BIGINT NOT NULL REFERENCES bulk_jobs(id) ON DELETE CASCADE,
          ordinal INTEGER NOT NULL,
          artist_name TEXT NOT NULL,
          spotify_track_id TEXT,
          first_songtools_date DATE,
          readiness TEXT,
          track_source TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0,
          result_status TEXT,
          result JSONB,
          error TEXT,
          claimed_at TIMESTAMPTZ,
          completed_at TIMESTAMPTZ,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE (job_id, ordinal)
        )
      `);
      await client.query("CREATE INDEX IF NOT EXISTS idx_bulk_jobs_status ON bulk_jobs(status, created_at)");
      await client.query("CREATE INDEX IF NOT EXISTS idx_bulk_job_items_claim ON bulk_job_items(job_id, status, ordinal)");
      await client.query("CREATE INDEX IF NOT EXISTS idx_bulk_job_items_status ON bulk_job_items(job_id, status)");
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
        (SELECT COUNT(*)::bigint FROM enrichment_runs) AS enrichment_runs,
        (SELECT COUNT(*)::bigint FROM bulk_jobs) AS bulk_jobs,
        (SELECT COUNT(*)::bigint FROM bulk_job_items) AS bulk_job_items
    `);
    const row = result.rows[0] || {};
    return {
      ok: true,
      artists: Number(row.artists || 0),
      trackResolutions: Number(row.track_resolutions || 0),
      listenerObservations: Number(row.listener_observations || 0),
      enrichmentRuns: Number(row.enrichment_runs || 0),
      bulkJobs: Number(row.bulk_jobs || 0),
      bulkJobItems: Number(row.bulk_job_items || 0)
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

  async createBulkJob({ name, sourceKey = null, items = [], metadata = {} }) {
    if (!Array.isArray(items) || !items.length) throw new Error("Bulk job requires at least one item");
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      let jobResult = await client.query(`
        INSERT INTO bulk_jobs (name, source_key, status, metadata)
        VALUES ($1,$2,'paused',$3::jsonb)
        ON CONFLICT (source_key) DO NOTHING
        RETURNING id, name, source_key, status, created_at
      `, [name || "Songtools artist enrichment", sourceKey, JSON.stringify(metadata || {})]);

      if (!jobResult.rows.length && sourceKey) {
        jobResult = await client.query(`
          SELECT id, name, source_key, status, created_at
          FROM bulk_jobs WHERE source_key = $1 LIMIT 1
        `, [sourceKey]);
        await client.query("COMMIT");
        const existing = jobResult.rows[0];
        return { created: false, jobId: Number(existing.id), name: existing.name, status: existing.status, sourceKey: existing.source_key };
      }

      const job = jobResult.rows[0];
      const normalized = items.map((item, index) => ({
        ordinal: Number.isFinite(Number(item.ordinal)) ? Number(item.ordinal) : index + 1,
        artist_name: String(item.artistName || "").trim(),
        spotify_track_id: item.spotifyTrackId ? String(item.spotifyTrackId).trim() : null,
        first_songtools_date: item.firstSongtoolsDate ? String(item.firstSongtoolsDate).slice(0, 10) : null,
        readiness: item.readiness ? String(item.readiness) : null,
        track_source: item.trackSource ? String(item.trackSource) : null
      })).filter((item) => item.artist_name);

      await client.query(`
        INSERT INTO bulk_job_items (
          job_id, ordinal, artist_name, spotify_track_id, first_songtools_date,
          readiness, track_source, status
        )
        SELECT $1, x.ordinal, x.artist_name, NULLIF(x.spotify_track_id,''),
               NULLIF(x.first_songtools_date,'')::date,
               x.readiness, x.track_source, 'pending'
        FROM jsonb_to_recordset($2::jsonb) AS x(
          ordinal INTEGER,
          artist_name TEXT,
          spotify_track_id TEXT,
          first_songtools_date TEXT,
          readiness TEXT,
          track_source TEXT
        )
        ON CONFLICT (job_id, ordinal) DO NOTHING
      `, [job.id, JSON.stringify(normalized)]);

      await client.query("COMMIT");
      return {
        created: true,
        jobId: Number(job.id),
        name: job.name,
        status: job.status,
        sourceKey: job.source_key,
        totalItems: normalized.length
      };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async listBulkJobs(limit = 20) {
    const safeLimit = Math.max(1, Math.min(100, Number(limit) || 20));
    const result = await this.pool.query(`
      SELECT j.*,
             COUNT(i.id)::bigint AS total_items,
             COUNT(i.id) FILTER (WHERE i.status = 'pending')::bigint AS pending_items,
             COUNT(i.id) FILTER (WHERE i.status = 'processing')::bigint AS processing_items,
             COUNT(i.id) FILTER (WHERE i.status = 'completed')::bigint AS completed_items,
             COUNT(i.id) FILTER (WHERE i.status = 'partial')::bigint AS partial_items,
             COUNT(i.id) FILTER (WHERE i.status = 'failed')::bigint AS failed_items,
             COUNT(i.id) FILTER (WHERE i.status = 'skipped')::bigint AS skipped_items
      FROM bulk_jobs j
      LEFT JOIN bulk_job_items i ON i.job_id = j.id
      GROUP BY j.id
      ORDER BY j.created_at DESC
      LIMIT $1
    `, [safeLimit]);
    return result.rows.map((row) => this._bulkJobSummary(row));
  }

  _bulkJobSummary(row) {
    if (!row) return null;
    const total = Number(row.total_items || 0);
    const completed = Number(row.completed_items || 0);
    const partial = Number(row.partial_items || 0);
    const failed = Number(row.failed_items || 0);
    const skipped = Number(row.skipped_items || 0);
    const finished = completed + partial + failed + skipped;
    return {
      jobId: Number(row.id),
      name: row.name,
      sourceKey: row.source_key ?? null,
      status: row.status,
      totalItems: total,
      pending: Number(row.pending_items || 0),
      processing: Number(row.processing_items || 0),
      completed,
      partial,
      failed,
      skipped,
      finished,
      percentFinished: total ? Number(((finished / total) * 100).toFixed(2)) : 0,
      createdAt: row.created_at ?? null,
      startedAt: row.started_at ?? null,
      completedAt: row.completed_at ?? null,
      updatedAt: row.updated_at ?? null,
      lastError: row.last_error ?? null,
      metadata: row.metadata ?? {}
    };
  }

  async bulkJobStatus(jobId = null) {
    const selector = jobId
      ? { clause: "WHERE j.id = $1", params: [jobId] }
      : { clause: "", params: [] };
    const result = await this.pool.query(`
      SELECT j.*,
             COUNT(i.id)::bigint AS total_items,
             COUNT(i.id) FILTER (WHERE i.status = 'pending')::bigint AS pending_items,
             COUNT(i.id) FILTER (WHERE i.status = 'processing')::bigint AS processing_items,
             COUNT(i.id) FILTER (WHERE i.status = 'completed')::bigint AS completed_items,
             COUNT(i.id) FILTER (WHERE i.status = 'partial')::bigint AS partial_items,
             COUNT(i.id) FILTER (WHERE i.status = 'failed')::bigint AS failed_items,
             COUNT(i.id) FILTER (WHERE i.status = 'skipped')::bigint AS skipped_items
      FROM bulk_jobs j
      LEFT JOIN bulk_job_items i ON i.job_id = j.id
      ${selector.clause}
      GROUP BY j.id
      ORDER BY j.created_at DESC
      LIMIT 1
    `, selector.params);
    if (!result.rows.length) return null;
    const summary = this._bulkJobSummary(result.rows[0]);
    const readinessResult = await this.pool.query(`
      SELECT COALESCE(readiness,'unknown') AS readiness, COUNT(*)::bigint AS count
      FROM bulk_job_items WHERE job_id = $1 GROUP BY COALESCE(readiness,'unknown') ORDER BY count DESC
    `, [summary.jobId]);
    summary.readiness = Object.fromEntries(readinessResult.rows.map((r) => [r.readiness, Number(r.count)]));
    return summary;
  }

  async setBulkJobStatus(jobId, status) {
    const allowed = new Set(['paused','queued','running','completed','cancelled']);
    if (!allowed.has(status)) throw new Error(`Invalid bulk job status: ${status}`);
    const result = await this.pool.query(`
      UPDATE bulk_jobs SET
        status = $2,
        started_at = CASE WHEN $2 IN ('queued','running') THEN COALESCE(started_at,NOW()) ELSE started_at END,
        completed_at = CASE WHEN $2 IN ('completed','cancelled') THEN NOW() ELSE NULL END,
        updated_at = NOW()
      WHERE id = $1
      RETURNING id
    `, [jobId, status]);
    if (!result.rows.length) throw new Error(`Bulk job ${jobId} not found`);
    return this.bulkJobStatus(jobId);
  }

  async getRunnableBulkJob() {
    const result = await this.pool.query(`
      SELECT j.id
      FROM bulk_jobs j
      WHERE j.status IN ('queued','running')
        AND EXISTS (
          SELECT 1 FROM bulk_job_items i
          WHERE i.job_id = j.id AND i.status = 'pending'
        )
      ORDER BY j.created_at ASC
      LIMIT 1
    `);
    return result.rows[0] ? Number(result.rows[0].id) : null;
  }

  async claimBulkBatch(jobId, limit = 50, maxAttempts = 3, staleMinutes = 15) {
    const safeLimit = Math.max(1, Math.min(500, Number(limit) || 50));
    const safeAttempts = Math.max(1, Math.min(20, Number(maxAttempts) || 3));
    const safeStale = Math.max(1, Math.min(1440, Number(staleMinutes) || 15));
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(`
        UPDATE bulk_job_items SET
          status = CASE WHEN attempts >= $2 THEN 'failed' ELSE 'pending' END,
          error = CASE WHEN attempts >= $2 THEN COALESCE(error,'Worker lease expired after maximum attempts') ELSE error END,
          claimed_at = NULL,
          updated_at = NOW()
        WHERE job_id = $1
          AND status = 'processing'
          AND claimed_at < NOW() - ($3::text || ' minutes')::interval
      `, [jobId, safeAttempts, safeStale]);

      const selected = await client.query(`
        SELECT id
        FROM bulk_job_items
        WHERE job_id = $1 AND status = 'pending' AND attempts < $3
        ORDER BY ordinal ASC
        LIMIT $2
        FOR UPDATE SKIP LOCKED
      `, [jobId, safeLimit, safeAttempts]);
      const ids = selected.rows.map((r) => r.id);
      if (!ids.length) {
        await client.query("COMMIT");
        return [];
      }
      const claimed = await client.query(`
        UPDATE bulk_job_items SET
          status = 'processing', attempts = attempts + 1, claimed_at = NOW(), updated_at = NOW()
        WHERE id = ANY($1::bigint[])
        RETURNING id, job_id, ordinal, artist_name, spotify_track_id, first_songtools_date,
                  readiness, track_source, status, attempts
      `, [ids]);
      await client.query(`
        UPDATE bulk_jobs SET status = 'running', started_at = COALESCE(started_at,NOW()), updated_at = NOW()
        WHERE id = $1 AND status = 'queued'
      `, [jobId]);
      await client.query("COMMIT");
      return claimed.rows.sort((a,b) => a.ordinal - b.ordinal);
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async finishBulkItem(itemId, status, { result = null, resultStatus = null, error = null } = {}) {
    const allowed = new Set(['completed','partial','failed','skipped','pending']);
    if (!allowed.has(status)) throw new Error(`Invalid bulk item status: ${status}`);
    await this.pool.query(`
      UPDATE bulk_job_items SET
        status = $2,
        result_status = $3,
        result = $4::jsonb,
        error = $5,
        claimed_at = CASE WHEN $2 = 'processing' THEN claimed_at ELSE NULL END,
        completed_at = CASE WHEN $2 IN ('completed','partial','failed','skipped') THEN NOW() ELSE NULL END,
        updated_at = NOW()
      WHERE id = $1
    `, [itemId, status, resultStatus, result ? JSON.stringify(result) : null, error]);
  }

  async completeBulkJobIfDone(jobId) {
    const result = await this.pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE status IN ('pending','processing'))::bigint AS open_items,
        COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_items
      FROM bulk_job_items WHERE job_id = $1
    `, [jobId]);
    const open = Number(result.rows[0]?.open_items || 0);
    if (open > 0) return false;
    await this.pool.query(`
      UPDATE bulk_jobs SET status = 'completed', completed_at = NOW(), updated_at = NOW()
      WHERE id = $1 AND status IN ('queued','running','paused')
    `, [jobId]);
    return true;
  }

  async setBulkJobError(jobId, error) {
    await this.pool.query(`
      UPDATE bulk_jobs SET last_error = $2, updated_at = NOW() WHERE id = $1
    `, [jobId, String(error || '').slice(0, 4000)]);
  }

  async getBulkJobFailures(jobId, limit = 50) {
    const safeLimit = Math.max(1, Math.min(200, Number(limit) || 50));
    const result = await this.pool.query(`
      SELECT ordinal, artist_name, spotify_track_id, first_songtools_date,
             readiness, attempts, result_status, error, result
      FROM bulk_job_items
      WHERE job_id = $1 AND status = 'failed'
      ORDER BY ordinal ASC
      LIMIT $2
    `, [jobId, safeLimit]);
    return result.rows.map((r) => ({
      ordinal: r.ordinal,
      artistName: r.artist_name,
      spotifyTrackId: r.spotify_track_id,
      firstSongtoolsDate: r.first_songtools_date ? String(r.first_songtools_date).slice(0,10) : null,
      readiness: r.readiness,
      attempts: r.attempts,
      resultStatus: r.result_status,
      error: r.error,
      result: r.result
    }));
  }

  async exportBulkJob(jobId) {
    const job = await this.bulkJobStatus(jobId);
    if (!job) return null;
    const result = await this.pool.query(`
      SELECT ordinal, artist_name, spotify_track_id, first_songtools_date,
             readiness, track_source, status, attempts, result_status, result, error
      FROM bulk_job_items WHERE job_id = $1 ORDER BY ordinal ASC
    `, [jobId]);
    return {
      job,
      items: result.rows.map((r) => ({
        ordinal: r.ordinal,
        artistName: r.artist_name,
        spotifyTrackId: r.spotify_track_id,
        firstSongtoolsDate: r.first_songtools_date ? String(r.first_songtools_date).slice(0,10) : null,
        readiness: r.readiness,
        trackSource: r.track_source,
        status: r.status,
        attempts: r.attempts,
        resultStatus: r.result_status,
        result: r.result,
        error: r.error
      }))
    };
  }

  async close() {
    await this.pool.end();
  }
}
