import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { z } from "zod";
import { SoundchartsClient } from "./soundcharts.mjs";
import { ArtistDatabase } from "./database.mjs";
import {
  enrichArtistsCached,
  resolveSpotifyTracksCached,
  refreshArtistsCurrentCached,
  collectPromotedTrackDistributorsCached
} from "./cached-enrichment.mjs";

const PORT = Number.parseInt(process.env.PORT || "3000", 10);
const MCP_PATH = process.env.MCP_PATH || "/mcp";
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
    version: "0.2.0"
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
      description: "Check the persistent PostgreSQL cache and return row counts. Makes no Soundcharts API calls.",
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

const app = createMcpExpressApp({ host: "0.0.0.0" });
app.use((req, res, next) => {
  if (req.path !== MCP_PATH || !MCP_BEARER_TOKEN) return next();
  const auth = req.headers.authorization || "";
  if (auth !== `Bearer ${MCP_BEARER_TOKEN}`) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
});

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
    version: "0.2.0",
    mcpPath: MCP_PATH,
    database: databaseHealth
  });
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
  console.log(`Songtools Soundcharts connector listening on port ${PORT}, MCP endpoint ${MCP_PATH}`);
});

async function shutdown(signal) {
  console.log(`Received ${signal}; shutting down`);
  httpServer.close(async () => {
    if (database) await database.close().catch(() => {});
    process.exit(0);
  });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
