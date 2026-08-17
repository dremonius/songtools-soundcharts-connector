import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { z } from "zod";
import { SoundchartsClient } from "./soundcharts.mjs";
import { enrichArtists, resolveSpotifyTracks, refreshArtistsCurrent, collectPromotedTrackDistributors } from "./enrichment.mjs";

const PORT = Number.parseInt(process.env.PORT || "3000", 10);
const MCP_PATH = process.env.MCP_PATH || "/mcp";
const CONCURRENCY = Math.max(1, Math.min(100, Number.parseInt(process.env.SOUNDCHARTS_CONCURRENCY || "20", 10)));
const MCP_BEARER_TOKEN = process.env.MCP_BEARER_TOKEN || "";
const soundcharts = new SoundchartsClient();

function jsonResult(value) {
  return {
    content: [{ type: "text", text: JSON.stringify(value) }]
  };
}

function getServer() {
  const server = new McpServer({
    name: "songtools-soundcharts",
    version: "0.1.0"
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
    "resolve_spotify_tracks",
    {
      title: "Resolve Spotify tracks to Soundcharts artists",
      description: "Resolve up to 500 Spotify track IDs to Soundcharts song/artist IDs and representative-track distributor data. Uses artistName only to disambiguate collaborations.",
      inputSchema: {
        items: z.array(z.object({
          artistName: z.string().optional(),
          spotifyTrackId: z.string().min(1)
        })).min(1).max(500)
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: true }
    },
    async ({ items }) => jsonResult({
      results: await resolveSpotifyTracks(soundcharts, items, CONCURRENCY),
      quotaRemainingHeader: soundcharts.lastQuotaRemaining
    })
  );

  server.registerTool(
    "enrich_artists",
    {
      title: "Batch enrich Songtools artists",
      description: "Enrich up to 500 artists with Soundcharts artist ID, Spotify monthly listeners nearest Songtools Day 1 and Day 30, latest monthly listeners, total main-performer track count, and representative-track distributor. Provide spotifyTrackId for first-time resolution or cached soundchartsArtistId on refreshes.",
      inputSchema: {
        items: z.array(z.object({
          artistName: z.string().optional(),
          spotifyTrackId: z.string().optional(),
          soundchartsArtistId: z.string().uuid().optional(),
          firstSongtoolsDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/)
        }).refine((x) => Boolean(x.spotifyTrackId || x.soundchartsArtistId), {
          message: "Provide spotifyTrackId or soundchartsArtistId"
        })).min(1).max(500)
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: true }
    },
    async ({ items }) => jsonResult({
      results: await enrichArtists(soundcharts, items, CONCURRENCY),
      quotaRemainingHeader: soundcharts.lastQuotaRemaining
    })
  );

  server.registerTool(
    "refresh_artists_current",
    {
      title: "Refresh current artist metrics",
      description: "Refresh up to 500 already-resolved artists using cached Soundcharts artist IDs. Returns latest available Spotify monthly listeners and total main-performer track count without re-resolving tracks or re-fetching Day 1/Day 30 history.",
      inputSchema: {
        items: z.array(z.object({
          artistName: z.string().optional(),
          soundchartsArtistId: z.string().uuid()
        })).min(1).max(500)
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: true }
    },
    async ({ items }) => jsonResult({
      results: await refreshArtistsCurrent(soundcharts, items, CONCURRENCY),
      quotaRemainingHeader: soundcharts.lastQuotaRemaining
    })
  );

  server.registerTool(
    "collect_promoted_track_distributors",
    {
      title: "Collect distributors across promoted tracks",
      description: "For each artist, inspect the supplied Songtools-promoted Spotify track IDs and return all distinct Soundcharts distributor values. Use as a second pass when you need distributor coverage beyond the representative track. Distributor data is Soundcharts beta data.",
      inputSchema: {
        items: z.array(z.object({
          artistName: z.string().optional(),
          spotifyTrackIds: z.array(z.string().min(1)).min(1).max(100)
        })).min(1).max(200)
      },
      annotations: { readOnlyHint: true, destructiveHint: false, openWorldHint: true }
    },
    async ({ items }) => jsonResult({
      results: await collectPromotedTrackDistributors(soundcharts, items, CONCURRENCY),
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

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "songtools-soundcharts", version: "0.1.0", mcpPath: MCP_PATH });
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

app.listen(PORT, "0.0.0.0", (error) => {
  if (error) {
    console.error("Failed to start server", error);
    process.exit(1);
  }
  console.log(`Songtools Soundcharts connector listening on port ${PORT}, MCP endpoint ${MCP_PATH}`);
});
