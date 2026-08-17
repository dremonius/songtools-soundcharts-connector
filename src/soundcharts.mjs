const DEFAULT_BASE_URL = "https://customer.api.soundcharts.com";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function envInt(name, fallback) {
  const n = Number.parseInt(process.env[name] || "", 10);
  return Number.isFinite(n) ? n : fallback;
}

export class SoundchartsError extends Error {
  constructor(message, { status = null, body = null, quotaRemaining = null } = {}) {
    super(message);
    this.name = "SoundchartsError";
    this.status = status;
    this.body = body;
    this.quotaRemaining = quotaRemaining;
  }
}

export class SoundchartsClient {
  constructor() {
    this.baseUrl = (process.env.SOUNDCHARTS_BASE_URL || DEFAULT_BASE_URL).replace(/\/$/, "");
    this.clientId = process.env.SOUNDCHARTS_CLIENT_ID || "";
    this.clientSecret = process.env.SOUNDCHARTS_CLIENT_SECRET || "";
    this.teamId = process.env.SOUNDCHARTS_TEAM_ID || "";
    this.appId = process.env.SOUNDCHARTS_APP_ID || "";
    this.apiKey = process.env.SOUNDCHARTS_API_KEY || "";
    this.maxRetries = envInt("SOUNDCHARTS_MAX_RETRIES", 5);
    this.token = null;
    this.tokenExpiresAt = 0;
    this.lastQuotaRemaining = null;

    if (!(this.clientId && this.clientSecret) && !(this.appId && this.apiKey)) {
      throw new Error(
        "Missing Soundcharts credentials. Set SOUNDCHARTS_CLIENT_ID + SOUNDCHARTS_CLIENT_SECRET, or SOUNDCHARTS_APP_ID + SOUNDCHARTS_API_KEY."
      );
    }
  }

  async getAccessToken() {
    if (!(this.clientId && this.clientSecret)) return null;

    const now = Date.now();
    if (this.token && now < this.tokenExpiresAt - 60_000) return this.token;

    const form = new URLSearchParams({ grant_type: "client_credentials" });
    if (this.teamId) form.set("team_id", this.teamId);

    const basic = Buffer.from(`${this.clientId}:${this.clientSecret}`).toString("base64");
    const response = await fetch("https://account.soundcharts.com/oauth/token", {
      method: "POST",
      headers: {
        Authorization: `Basic ${basic}`,
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: form.toString()
    });

    const text = await response.text();
    let body = null;
    try { body = text ? JSON.parse(text) : null; } catch { body = text; }

    if (!response.ok || !body?.access_token) {
      throw new SoundchartsError("Unable to obtain Soundcharts access token", {
        status: response.status,
        body
      });
    }

    this.token = body.access_token;
    this.tokenExpiresAt = now + (Number(body.expires_in || 3600) * 1000);
    return this.token;
  }

  async authHeaders() {
    if (this.clientId && this.clientSecret) {
      const token = await this.getAccessToken();
      return { Authorization: `Bearer ${token}` };
    }
    return {
      "x-app-id": this.appId,
      "x-api-key": this.apiKey
    };
  }

  async request(path, { method = "GET", query = null, body = null } = {}) {
    const url = new URL(`${this.baseUrl}${path}`);
    if (query) {
      for (const [key, value] of Object.entries(query)) {
        if (value !== undefined && value !== null && value !== "") {
          url.searchParams.set(key, String(value));
        }
      }
    }

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      const headers = {
        Accept: "application/json",
        ...(await this.authHeaders())
      };
      if (body !== null) headers["Content-Type"] = "application/json";

      let response;
      try {
        response = await fetch(url, {
          method,
          headers,
          body: body === null ? undefined : JSON.stringify(body)
        });
      } catch (error) {
        if (attempt >= this.maxRetries) throw error;
        await sleep(Math.min(30_000, 500 * 2 ** attempt));
        continue;
      }

      const quotaRemaining = response.headers.get("x-quota-remaining");
      if (quotaRemaining !== null) this.lastQuotaRemaining = quotaRemaining;

      const text = await response.text();
      let payload = null;
      try { payload = text ? JSON.parse(text) : null; } catch { payload = text; }

      if (response.ok) return payload;

      if ((response.status === 429 || response.status >= 500) && attempt < this.maxRetries) {
        const retryAfter = Number(response.headers.get("retry-after") || 0);
        const delay = retryAfter > 0
          ? retryAfter * 1000
          : Math.min(30_000, 750 * 2 ** attempt + Math.floor(Math.random() * 250));
        await sleep(delay);
        continue;
      }

      throw new SoundchartsError(`Soundcharts request failed: ${response.status} ${response.statusText}`, {
        status: response.status,
        body: payload,
        quotaRemaining
      });
    }

    throw new SoundchartsError("Soundcharts request failed after retries");
  }

  getUsage() {
    return this.request("/api/v2/team/usage");
  }

  getSongBySpotifyTrackId(spotifyTrackId) {
    return this.request(`/api/v2.25/song/by-platform/spotify/${encodeURIComponent(spotifyTrackId)}`);
  }

  getSongByUuid(songUuid) {
    return this.request(`/api/v2.25/song/${encodeURIComponent(songUuid)}`);
  }

  getArtistByUuid(artistUuid) {
    return this.request(`/api/v2.9/artist/${encodeURIComponent(artistUuid)}`);
  }

  getArtistSongs(artistUuid, { offset = 0, limit = 1, mainPerformer = 1 } = {}) {
    return this.request(`/api/v2.21/artist/${encodeURIComponent(artistUuid)}/songs`, {
      query: { offset, limit, mainPerformer }
    });
  }

  getSpotifyMonthlyListeners(artistUuid, { startDate, endDate, sort = "asc", limit = 100 } = {}) {
    return this.request(`/api/v2/artist/${encodeURIComponent(artistUuid)}/streaming/spotify/listening`, {
      query: { startDate, endDate, sort, limit }
    });
  }
}
