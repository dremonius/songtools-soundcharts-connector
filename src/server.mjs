import express from "express";
import { createHash } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";
import { SoundchartsClient } from "./soundcharts.mjs";
import { ArtistDatabase } from "./database.mjs";
import { startBulkWorker } from "./bulk-worker.mjs";
import {
  enrichArtistsCached,
  resolveSpotifyTracksCached,
  refreshArtistsCurrentCached,
  collectPromotedTrackDistributorsCached
} from "./cached-enrichment.mjs";

const PORT = Number.parseInt(process.env.PORT || "3000", 10);
const MCP_PATH = process.env.MCP_PATH || "/mcp";
const BULK_IMPORT_PATH = process.env.BULK_IMPORT_PATH || "/bulk-jobs/import";
const JSON_BODY_LIMIT = process.env.JSON_BODY_LIMIT || "8mb";
const CONCURRENCY = Math.max(1, Math.min(100, Number.parseInt(process.env.SOUNDCHARTS_CONCURRENCY || "20", 10)));
const MCP_BEARER_TOKEN = process.env.MCP_BEARER_TOKEN || "";
const DATABASE_CONFIGURED = Boolean(process.env.DATABASE_URL);
const soundcharts = new SoundchartsClient();

let database = null;
let databaseInitError = null;
if (DATABASE_CONFIGURED) {
  try {
    database = new ArtistDatabase();
    await database.init();
    console.log("PostgreSQL connected; artist intelligence schema is ready");
  } catch (error) {
    databaseInitError = error;
    database = null;
    console.error("PostgreSQL initialization failed", error);
  }
}

function persistenceDatabase() {
  if (DATABASE_CONFIGURED && !database) {
    throw new Error(`PostgreSQL is configured but unavailable; refusing enrichment without persistence. ${databaseInitError?.message || ""}`.trim());
  }
  if (!database) throw new Error("DATABASE_URL is required for this operation");
  return database;
}

function jsonResult(value) {
  return {
    content: [{ type: "text", text: JSON.stringify(value) }]
  };
}

function getServer() {
  const server = new McpServer({
    name: "songtools-soundcharts",
    version: "0.3.0"
  });

  server.registerTool(
    "soundcharts_quota",
    {
      title: "Soundcharts quota",
      description: "Return the current Soundcharts API quota/rate-limit usage. This endpoint is free in Soundcharts and should be checked before large enrichments.",
      inputSchema: {},
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: true }
    },
    async () => jsonResult(await soundcharts.getUsage())
  );

  server.registerTool(
    "database_status",
    {
      title: "Artist intelligence database status",
      description: "Check the persistent PostgreSQL cache and bulk-job tables. Makes no Soundcharts API calls.",
      inputSchema: {},
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false }
    },
    async () => {
      if (!DATABASE_CONFIGURED) return jsonResult({ ok: false, configured: false });
      if (!database) return jsonResult({ ok: false, configured: true, error: databaseInitError?.message || "Database unavailable" });
      return jsonResult({ configured: true, ...(await database.status()) });
    }
  );

  server.registerTool(
    "bulk_jobs",
    {
      title: "List bulk enrichment jobs",
      description: "List recent persistent bulk enrichment jobs and their progress. Makes no Soundcharts API calls.",
      inputSchema: {
        limit: z.number().int().min(1).max(100).optional()
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false }
    },
    async ({ limit }) => jsonResult({ jobs: await persistenceDatabase().listBulkJobs(limit || 20) })
  );

  server.registerTool(
    "bulk_job_status",
    {
      title: "Bulk enrichment job status",
      description: "Return detailed progress for one bulk job. Omit jobId to inspect the most recently created job.",
      inputSchema: {
        jobId: z.number().int().positive().optional()
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false }
    },
    async ({ jobId }) => jsonResult({ job: await persistenceDatabase().bulkJobStatus(jobId || null) })
  );

  server.registerTool(
    "bulk_job_start",
    {
      title: "Start or resume a bulk enrichment job",
      description: "Queue a previously imported bulk job. The Render worker processes it in resumable PostgreSQL-backed batches.",
      inputSchema: {
        jobId: z.number().int().positive()
      },
      annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: true }
    },
    async ({ jobId }) => jsonResult({ job: await persistenceDatabase().setBulkJobStatus(jobId, "queued") })
  );

  server.registerTool(
    "bulk_job_pause",
    {
      title: "Pause a bulk enrichment job",
      description: "Pause new batch claims for a bulk job. Any batch already in flight is allowed to finish and persist.",
      inputSchema: {
        jobId: z.number().int().positive()
      },
      annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: false }
    },
    async ({ jobId }) => jsonResult({ job: await persistenceDatabase().setBulkJobStatus(jobId, "paused") })
  );

  server.registerTool(
    "bulk_job_failures",
    {
      title: "Bulk enrichment failures",
      description: "Inspect failed artists from a bulk job, including the stored error and last result.",
      inputSchema: {
        jobId: z.number().int().positive(),
        limit: z.number().int().min(1).max(200).optional()
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: false }
    },
    async ({ jobId, limit }) => jsonResult({ failures: await persistenceDatabase().getBulkJobFailures(jobId, limit || 50) })
  );

  server.registerTool(
    "resolve_spotify_tracks",
    {
      title: "Resolve Spotify tracks to Soundcharts artists",
      description: "Resolve up to 500 Spotify track IDs to Soundcharts song/artist IDs and representative-track distributor data. Uses persistent PostgreSQL caching and artistName to disambiguate collaborations.",
      inputSchema: {
        items: z.array(z.object({
          artistName: z.string().optional(),
          spotifyTrackId: z.string().min(1)
        })).min(1).max(500)
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: true }
    },
    async ({ items }) => jsonResult({
      results: await resolveSpotifyTracksCached(soundcharts, persistenceDatabase(), items, CONCURRENCY),
      quotaRemainingHeader: soundcharts.lastQuotaRemaining
    })
  );

  server.registerTool(
    "enrich_artists",
    {
      title: "Batch enrich Songtools artists",
      description: "Enrich up to 500 artists with persistent caching. Historical Day 1/Day 30 values are fetched once per first Songtools date; current metrics are refreshed only when stale unless forceRefreshCurrent is true.",
      inputSchema: {
        items: z.array(z.object({
          artistName: z.string().optional(),
          spotifyTrackId: z.string().optional(),
          soundchartsArtistId: z.string().uuid().optional(),
          firstSongtoolsDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
          forceRefreshCurrent: z.boolean().optional()
        }).refine((x) => Boolean(x.spotifyTrackId || x.soundchartsArtistId), {
          message: "Provide spotifyTrackId or soundchartsArtistId"
        })).min(1).max(500)
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: true }
    },
    async ({ items }) => jsonResult({
      results: await enrichArtistsCached(soundcharts, persistenceDatabase(), items, CONCURRENCY),
      quotaRemainingHeader: soundcharts.lastQuotaRemaining
    })
  );

  server.registerTool(
    "refresh_artists_current",
    {
      title: "Refresh current artist metrics",
      description: "Refresh up to 500 already-resolved artists. Current values are cached for CURRENT_CACHE_HOURS (20 by default); set forceRefresh to bypass the freshness window.",
      inputSchema: {
        items: z.array(z.object({
          artistName: z.string().optional(),
          soundchartsArtistId: z.string().uuid(),
          forceRefresh: z.boolean().optional()
        })).min(1).max(500)
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: true }
    },
    async ({ items }) => jsonResult({
      results: await refreshArtistsCurrentCached(soundcharts, persistenceDatabase(), items, CONCURRENCY),
      quotaRemainingHeader: soundcharts.lastQuotaRemaining
    })
  );

  server.registerTool(
    "collect_promoted_track_distributors",
    {
      title: "Collect distributors across promoted tracks",
      description: "For each artist, inspect supplied Songtools-promoted Spotify track IDs and return distinct distributor values. Track-level distributor results are cached in PostgreSQL for 30 days by default.",
      inputSchema: {
        items: z.array(z.object({
          artistName: z.string().optional(),
          spotifyTrackIds: z.array(z.string().min(1)).min(1).max(100)
        })).min(1).max(200)
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: true }
    },
    async ({ items }) => jsonResult({
      results: await collectPromotedTrackDistributorsCached(soundcharts, persistenceDatabase(), items, CONCURRENCY),
      quotaRemainingHeader: soundcharts.lastQuotaRemaining
    })
  );

  return server;
}

const app = express();

function requiresConnectorAuth(req) {
  return req.path === MCP_PATH || req.path === BULK_IMPORT_PATH || req.path.startsWith("/bulk-jobs/");
}

app.use((req, res, next) => {
  if (!requiresConnectorAuth(req) || !MCP_BEARER_TOKEN) return next();
  const auth = req.headers.authorization || "";
  if (auth !== `Bearer ${MCP_BEARER_TOKEN}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
});
app.use(express.json({ limit: JSON_BODY_LIMIT }));

let bulkWorker = null;
if (database) {
  bulkWorker = startBulkWorker({ client: soundcharts, database, concurrency: CONCURRENCY });
  console.log("Persistent bulk worker ready", bulkWorker.config);
}

app.get("/health", async (_req, res) => {
  let databaseHealth = { configured: DATABASE_CONFIGURED, ok: !DATABASE_CONFIGURED };
  if (database) {
    try {
      databaseHealth = { configured: true, ...(await database.ping()) };
    } catch (error) {
      databaseHealth = { configured: true, ok: false, error: error?.message || String(error) };
    }
  } else if (DATABASE_CONFIGURED) {
    databaseHealth = { configured: true, ok: false, error: databaseInitError?.message || "Database unavailable" };
  }

  const ok = !DATABASE_CONFIGURED || databaseHealth.ok;
  res.status(ok ? 200 : 503).json({
    ok,
    service: "songtools-soundcharts",
    version: "0.3.0",
    mcpPath: MCP_PATH,
    bulkImportPath: BULK_IMPORT_PATH,
    database: databaseHealth,
    bulkWorker: bulkWorker?.config || null
  });
});

app.post(BULK_IMPORT_PATH, async (req, res) => {
  try {
    const db = persistenceDatabase();
    const body = req.body || {};
    const items = Array.isArray(body) ? body : body.items;
    if (!Array.isArray(items) || !items.length) {
      return res.status(400).json({ error: "Request body must contain a non-empty items array." });
    }
    if (items.length > 50_000) {
      return res.status(413).json({ error: "Bulk import is limited to 50,000 items per job." });
    }

    const invalid = items.findIndex((item) => !item || !String(item.artistName || "").trim());
    if (invalid >= 0) {
      return res.status(400).json({ error: `Item ${invalid + 1} is missing artistName.` });
    }

    const digest = createHash("sha256").update(JSON.stringify(items)).digest("hex");
    const sourceKey = body.sourceKey || `sha256:${digest}`;
    const generatedDate = body.generatedAt ? String(body.generatedAt).slice(0, 10) : new Date().toISOString().slice(0, 10);
    const name = body.name || `Songtools artist enrichment ${generatedDate}`;
    const readiness = {};
    for (const item of items) {
      const key = item.readiness || (item.spotifyTrackId ? (item.firstSongtoolsDate ? "full" : "current_only") : "missing_track");
      readiness[key] = (readiness[key] || 0) + 1;
    }

    const created = await db.createBulkJob({
      name,
      sourceKey,
      items,
      metadata: {
        schemaVersion: body.schemaVersion ?? null,
        generatedAt: body.generatedAt ?? null,
        sourceArtistCount: body.sourceArtistCount ?? items.length,
        readiness
      }
    });
    const job = await db.bulkJobStatus(created.jobId);
    res.status(created.created ? 201 : 200).json({
      ...created,
      job,
      message: created.created
        ? "Bulk job imported in paused state. Start it with the MCP tool bulk_job_start."
        : "This exact import already exists; returning the existing job."
    });
  } catch (error) {
    console.error("Bulk import failed", error);
    res.status(500).json({ error: error?.message || String(error) });
  }
});

app.get("/bulk-jobs/:jobId/results", async (req, res) => {
  try {
    const jobId = Number.parseInt(req.params.jobId, 10);
    if (!Number.isFinite(jobId) || jobId <= 0) return res.status(400).json({ error: "Invalid jobId" });
    const payload = await persistenceDatabase().exportBulkJob(jobId);
    if (!payload) return res.status(404).json({ error: "Bulk job not found" });
    res.json(payload);
  } catch (error) {
    console.error("Bulk export failed", error);
    res.status(500).json({ error: error?.message || String(error) });
  }
});

app.post(MCP_PATH, async (req, res) => {
  const server = getServer();
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  try {
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
    res.on("close", () => {
      transport.close();
      server.close();
    });
  } catch (error) {
    console.error("MCP request error", error);
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: "2.0",
        error: { code: -32603, message: "Internal server error" },
        id: null
      });
    }
  }
});

app.get(MCP_PATH, (_req, res) => {
  res.status(405).json({
    jsonrpc: "2.0",
    error: { code: -32000, message: "Method not allowed" },
    id: null
  });
});

app.delete(MCP_PATH, (_req, res) => {
  res.status(405).json({
    jsonrpc: "2.0",
    error: { code: -32000, message: "Method not allowed" },
    id: null
  });
});

const httpServer = app.listen(PORT, "0.0.0.0", (error) => {
  if (error) {
    console.error("Failed to start server", error);
    process.exit(1);
  }
  console.log(`Songtools Soundcharts connector v0.3.0 listening on port ${PORT}, MCP endpoint ${MCP_PATH}`);
});

async function shutdown(signal) {
  console.log(`Received ${signal}; shutting down`);
  bulkWorker?.stop();
  httpServer.close(async () => {
    if (bulkWorker) await bulkWorker.waitForIdle().catch(() => {});
    if (database) await database.close().catch(() => {});
    process.exit(0);
  });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
