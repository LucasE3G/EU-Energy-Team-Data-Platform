require('dotenv').config();
const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const https = require('https');
const { createClient } = require('@supabase/supabase-js');
const entsoeDomains = require('./entsoe_domains');

const PORT = 3000;

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || `localhost:${PORT}`}`);

  // API routes (server-side; keeps external API keys private)
  if (url.pathname.startsWith('/api/energy/')) {
    handleEnergyApi(req, res, url).catch((err) => {
      console.error('Energy API error:', err);
      writeJson(res, 500, { error: 'internal_error', message: err?.message || String(err) });
    });
    return;
  }

  let filePath = '.' + url.pathname;
  if (filePath === './') {
    filePath = './index.html';
  }

  const extname = String(path.extname(filePath)).toLowerCase();
  const mimeTypes = {
    '.html': 'text/html',
    '.js': 'text/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpg',
    '.gif': 'image/gif',
    '.svg': 'image/svg+xml',
    '.wav': 'audio/wav',
    '.mp4': 'video/mp4',
    '.woff': 'application/font-woff',
    '.ttf': 'application/font-ttf',
    '.eot': 'application/vnd.ms-fontobject',
    '.otf': 'application/font-otf',
    '.wasm': 'application/wasm'
  };

  const contentType = mimeTypes[extname] || 'application/octet-stream';

  fs.readFile(filePath, (error, content) => {
    if (error) {
      if (error.code === 'ENOENT') {
        res.writeHead(404, { 'Content-Type': 'text/html' });
        res.end('<h1>404 - File Not Found</h1>', 'utf-8');
      } else {
        res.writeHead(500);
        res.end(`Server Error: ${error.code}`, 'utf-8');
      }
    } else {
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(content, 'utf-8');
    }
  });
});

server.listen(PORT, () => {
  console.log(`🚀 Server running at http://localhost:${PORT}/`);
  console.log(`📝 Open your browser and navigate to http://localhost:${PORT}`);
});

function getSupabaseAdmin() {
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!supabaseUrl || !serviceRole) {
    throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env');
  }
  return createClient(supabaseUrl, serviceRole, {
    auth: { persistSession: false, autoRefreshToken: false },
  });
}

function writeJson(res, status, payload) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(JSON.stringify(payload));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => {
      data += chunk;
      if (data.length > 2_000_000) {
        reject(new Error('Payload too large'));
        req.destroy();
      }
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

function httpsGetJson(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, { method: 'GET', headers }, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        const status = res.statusCode || 0;
        if (status < 200 || status >= 300) {
          return reject(new Error(`HTTP ${status}: ${data.slice(0, 500)}`));
        }
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error(`Invalid JSON response: ${String(e?.message || e)}`));
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

function httpsGetText(url) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, { method: 'GET' }, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        const status = res.statusCode || 0;
        if (status < 200 || status >= 300) {
          return reject(new Error(`HTTP ${status}: ${data.slice(0, 500)}`));
        }
        resolve(data);
      });
    });
    req.on('error', reject);
    req.end();
  });
}

function httpsPostForm(url, headers = {}, formBody = '') {
  return new Promise((resolve, reject) => {
    const req = https.request(url, { method: 'POST', headers }, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        const status = res.statusCode || 0;
        if (status < 200 || status >= 300) {
          return reject(new Error(`HTTP ${status}: ${data.slice(0, 500)}`));
        }
        resolve(data);
      });
    });
    req.on('error', reject);
    req.write(formBody);
    req.end();
  });
}

function requireIngestAuth(req) {
  const token = process.env.INGEST_TOKEN;
  if (!token) {
    throw new Error('Missing INGEST_TOKEN in .env (required to call ingestion endpoint)');
  }
  const auth = String(req.headers['authorization'] || '');
  const expected = `Bearer ${token}`;
  if (auth !== expected) {
    const err = new Error('Unauthorized');
    err.statusCode = 401;
    throw err;
  }
}

async function handleEnergyApi(req, res, url) {
  if (url.pathname === '/api/energy/health') {
    writeJson(res, 200, { ok: true });
    return;
  }

  // EU gas implied demand (daily) by sector: household, industry, power
  if (url.pathname === '/api/energy/gas') {
    const supabase = getSupabaseAdmin();

    const country = (url.searchParams.get('country') || '').toUpperCase() || null;
    const start = url.searchParams.get('start');
    const end = url.searchParams.get('end');
    const sector = (url.searchParams.get('sector') || 'total').toLowerCase();

    const allowedSectors = new Set(['total', 'power', 'household', 'industry']);
    if (!allowedSectors.has(sector)) {
      writeJson(res, 400, { error: 'bad_request', message: 'Invalid sector. Use total|power|household|industry' });
      return;
    }

    let query = supabase
      .from('gas_demand_daily')
      .select(
        'country_code, gas_day, total_mwh, power_mwh, household_mwh, industry_mwh, source_total, source_power, source_split, method_version, quality_flag'
      )
      .order('gas_day', { ascending: true })
      .limit(5000);

    if (country) query = query.eq('country_code', country);
    if (start) query = query.gte('gas_day', start);
    if (end) query = query.lte('gas_day', end);

    const { data, error } = await query;
    if (error) {
      writeJson(res, 500, { error: 'db_error', message: error.message });
      return;
    }

    const rows = (data || []).map((r) => ({
      country_code: r.country_code,
      gas_day: r.gas_day,
      mwh:
        sector === 'total'
          ? r.total_mwh
          : sector === 'power'
            ? r.power_mwh
            : sector === 'household'
              ? r.household_mwh
              : r.industry_mwh,
      meta: {
        source_total: r.source_total,
        source_power: r.source_power,
        source_split: r.source_split,
        method_version: r.method_version,
        quality_flag: r.quality_flag,
      },
    }));

    writeJson(res, 200, { rows });
    return;
  }

  if (url.pathname === '/api/energy/latest') {
    const supabase = getSupabaseAdmin();

    const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 60), 1), 200);
    const zone = url.searchParams.get('zone');

    let query = supabase
      .from('energy_mix_snapshots')
      .select('id, zone_id, country_code, ts, renewable_percent, carbon_intensity_g_per_kwh, source')
      .order('ts', { ascending: false })
      .limit(limit);

    if (zone) {
      query = query.eq('zone_id', zone);
    }

    const { data, error } = await query;
    if (error) {
      writeJson(res, 500, { error: 'db_error', message: error.message });
      return;
    }

    // Return latest row per zone_id (if not filtered)
    if (!zone) {
      const seen = new Set();
      const rows = [];
      for (const r of data || []) {
        const key = r.zone_id || r.country_code || r.id;
        if (seen.has(key)) continue;
        seen.add(key);
        rows.push(r);
      }
      writeJson(res, 200, { rows });
      return;
    }

    writeJson(res, 200, { rows: data || [] });
    return;
  }

  if (url.pathname === '/api/energy/ingest') {
    if (req.method !== 'POST') {
      writeJson(res, 405, { error: 'method_not_allowed' });
      return;
    }

    try {
      requireIngestAuth(req);
    } catch (e) {
      writeJson(res, e.statusCode || 401, { error: 'unauthorized', message: e.message });
      return;
    }

    const token = process.env.ENTSOE_API_TOKEN;
    if (!token) {
      writeJson(res, 500, { error: 'missing_config', message: 'Missing ENTSOE_API_TOKEN in .env' });
      return;
    }

    // Optional body allows overriding zones
    let zones = null;
    try {
      const raw = await readBody(req);
      if (raw && raw.trim()) {
        const body = JSON.parse(raw);
        if (Array.isArray(body?.zones) && body.zones.length) zones = body.zones;
      }
    } catch (e) {
      // ignore body parse errors; fallback to defaults
    }

    zones = (zones || Object.keys(entsoeDomains)).map(String);

    const supabase = getSupabaseAdmin();
    const results = [];

    for (const zone of zones) {
      try {
        const domain = entsoeDomains[zone];
        if (!domain) throw new Error(`Missing ENTSO-E domain mapping for zone ${zone}`);

        // Query last ~3 hours to reliably get a recent point.
        const now = new Date();
        const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours(), 0, 0));
        const start = new Date(end.getTime() - 3 * 60 * 60 * 1000);

        const xml = await entsoeQueryGenerationPerType({
          securityToken: token,
          domain,
          periodStart: start,
          periodEnd: end,
        });

        const parsed = parseEntsoeGenerationPerTypeLatest(xml);
        const ts = parsed.ts || end.toISOString();
        const renewablePercent = parsed.renewablePercent;
        const carbonIntensity = parsed.carbonIntensityGPerKwh;

        const payload = {
          zone_id: zone,
          country_code: zone,
          ts,
          renewable_percent: renewablePercent,
          carbon_intensity_g_per_kwh: carbonIntensity,
          source: 'entsoe',
          raw: parsed.raw,
        };

        const { error } = await supabase.from('energy_mix_snapshots').upsert(payload, {
          onConflict: 'source,zone_id,ts',
        });

        if (error) throw new Error(error.message);

        results.push({ zone, ok: true, ts, renewablePercent, carbonIntensity });
      } catch (e) {
        results.push({ zone, ok: false, error: e?.message || String(e) });
      }
    }

    writeJson(res, 200, { ok: true, results });
    return;
  }

  // France-only ingestion using RTE Actual Generation API (15-min mix)
  if (url.pathname === '/api/energy/ingest-france-rte') {
    if (req.method !== 'POST') {
      writeJson(res, 405, { error: 'method_not_allowed' });
      return;
    }

    try {
      requireIngestAuth(req);
    } catch (e) {
      writeJson(res, e.statusCode || 401, { error: 'unauthorized', message: e.message });
      return;
    }

    const clientId = process.env.RTE_CLIENT_ID;
    const clientSecret = process.env.RTE_CLIENT_SECRET;
    if (!clientId || !clientSecret) {
      writeJson(res, 500, { error: 'missing_config', message: 'Missing RTE_CLIENT_ID or RTE_CLIENT_SECRET in .env' });
      return;
    }

    const supabase = getSupabaseAdmin();

    // Get access token (client credentials)
    const basic = Buffer.from(`${clientId}:${clientSecret}`, 'utf8').toString('base64');
    const tokenJsonText = await httpsPostForm(
      'https://digital.iservices.rte-france.com/token/oauth/',
      {
        Authorization: `Basic ${basic}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      ''
    );
    const tokenPayload = JSON.parse(tokenJsonText);
    const accessToken = tokenPayload?.access_token;
    if (!accessToken) {
      writeJson(res, 500, { error: 'auth_error', message: 'RTE token response missing access_token' });
      return;
    }

    // Fetch today-so-far 15-min generation mix (recommended “near real-time”)
    const mix = await httpsGetJson(
      'https://digital.iservices.rte-france.com/open_api/actual_generation/v1/generation_mix_15min_time_scale',
      { Authorization: `Bearer ${accessToken}` }
    );

    const { ts, renewablePercent, carbonIntensityGPerKwh, totals, raw } = parseRteGenerationMixLatest(mix);

    const payload = {
      zone_id: 'FR',
      country_code: 'FR',
      ts: ts || new Date().toISOString(),
      renewable_percent: renewablePercent,
      carbon_intensity_g_per_kwh: carbonIntensityGPerKwh,
      source: 'rte',
      raw: { ...raw, totals },
    };

    const { error } = await supabase.from('energy_mix_snapshots').upsert(payload, {
      onConflict: 'source,zone_id,ts',
    });
    if (error) {
      writeJson(res, 500, { error: 'db_error', message: error.message });
      return;
    }

    writeJson(res, 200, { ok: true, inserted: payload });
    return;
  }

  writeJson(res, 404, { error: 'not_found' });
}

function entsoeFormatYmdHm(date) {
  const pad = (n) => String(n).padStart(2, '0');
  return (
    String(date.getUTCFullYear()) +
    pad(date.getUTCMonth() + 1) +
    pad(date.getUTCDate()) +
    pad(date.getUTCHours()) +
    pad(date.getUTCMinutes())
  );
}

function parseRteGenerationMixLatest(mixJson) {
  const rows = Array.isArray(mixJson?.generation_mix_15min_time_scale)
    ? mixJson.generation_mix_15min_time_scale
    : [];

  // Aggregate latest value per (production_type, production_subtype)
  // We keep only “generation” sectors; exclude exchange/pumping.
  const excludedTypes = new Set(['EXCHANGE', 'PUMPING']);

  const byType = {};
  let bestTs = null;

  for (const series of rows) {
    const pType = series?.production_type;
    const pSubtype = series?.production_subtype;
    if (!pType || excludedTypes.has(pType)) continue;
    if (!Array.isArray(series?.values) || !series.values.length) continue;

    // Values appear ordered most-recent first per the control rule, but be defensive.
    let latest = null;
    for (const v of series.values) {
      if (!v?.start_date || typeof v?.value !== 'number') continue;
      if (!latest || Date.parse(v.start_date) > Date.parse(latest.start_date)) latest = v;
    }
    if (!latest) continue;

    const key = pSubtype && pSubtype !== 'TOTAL' ? `${pType}:${pSubtype}` : pType;
    byType[key] = (byType[key] || 0) + latest.value;

    const t = Date.parse(latest.start_date);
    if (Number.isFinite(t) && (!bestTs || t > Date.parse(bestTs))) bestTs = new Date(t).toISOString();
  }

  // Map to renewable vs non-renewable (simple, transparent rules)
  // Renewables: SOLAR, WIND, HYDRO (excluding pumped storage subtype), BIOENERGY
  // Non-renewable: NUCLEAR, FOSSIL_* (and any unknown treated as non-renewable later if needed)
  let renewableMw = 0;
  let nonRenewableMw = 0;

  for (const [k, v] of Object.entries(byType)) {
    const val = Number(v);
    if (!Number.isFinite(val)) continue;

    if (k.startsWith('HYDRO:HYDRO_PUMPED_STORAGE')) {
      // Storage; exclude from both buckets to avoid confusing sign conventions.
      continue;
    }

    if (k === 'SOLAR' || k.startsWith('WIND') || k.startsWith('HYDRO') || k.startsWith('BIOENERGY')) {
      renewableMw += val;
      continue;
    }

    if (k === 'NUCLEAR' || k.startsWith('FOSSIL_')) {
      nonRenewableMw += val;
      continue;
    }

    // Default: treat as non-renewable for now (keeps % conservative)
    nonRenewableMw += val;
  }

  const totalMw = renewableMw + nonRenewableMw;
  const renewablePercent = totalMw > 0 ? (renewableMw / totalMw) * 100 : null;

  return {
    ts: bestTs,
    renewablePercent,
    carbonIntensityGPerKwh: null, // Not provided directly by this API
    totals: { renewableMw, nonRenewableMw, totalMw },
    raw: { byType },
  };
}

async function entsoeQueryGenerationPerType({ securityToken, domain, periodStart, periodEnd }) {
  // ENTSO-E Transparency Platform API\n+  // DocumentType A75: Generation per type\n+  // ProcessType A16: Realised\n+  const params = new URLSearchParams({
    securityToken,
    documentType: 'A75',
    processType: 'A16',
    in_Domain: domain,
    periodStart: entsoeFormatYmdHm(periodStart),
    periodEnd: entsoeFormatYmdHm(periodEnd),
  });
  const url = `https://web-api.tp.entsoe.eu/api?${params.toString()}`;
  return await httpsGetText(url);
}

function pickAll(text, regex) {
  const out = [];
  let m;
  while ((m = regex.exec(text))) out.push(m);
  return out;
}

function parseEntsoeGenerationPerTypeLatest(xml) {
  // Minimal XML parsing via regex to avoid extra dependencies.
  // We extract each TimeSeries' psrType and all Point positions/quantities.

  const timeSeriesBlocks = pickAll(xml, /<TimeSeries[\s\S]*?<\/TimeSeries>/g);
  const byType = {};
  let bestTs = null;

  for (const tsBlockMatch of timeSeriesBlocks) {
    const block = tsBlockMatch[0];
    const psrType = (block.match(/<psrType>([^<]+)<\/psrType>/) || [])[1];
    if (!psrType) continue;

    const periodStart = (block.match(/<timeInterval>\s*<start>([^<]+)<\/start>/) || [])[1];
    const resolution = (block.match(/<resolution>([^<]+)<\/resolution>/) || [])[1];
    const startMs = periodStart ? Date.parse(periodStart) : NaN;

    const points = pickAll(block, /<Point>[\s\S]*?<\/Point>/g).map((pm) => pm[0]);
    let latestPos = -1;
    let latestQty = null;
    for (const p of points) {
      const pos = Number((p.match(/<position>([^<]+)<\/position>/) || [])[1]);
      const qty = Number((p.match(/<quantity>([^<]+)<\/quantity>/) || [])[1]);
      if (!Number.isFinite(pos) || !Number.isFinite(qty)) continue;
      if (pos > latestPos) {
        latestPos = pos;
        latestQty = qty;
      }
    }

    if (latestPos < 0 || latestQty == null) continue;
    byType[psrType] = (byType[psrType] || 0) + latestQty;

    // Try to compute timestamp for the latest point.
    if (Number.isFinite(startMs)) {
      const stepMinutes = resolution === 'PT15M' ? 15 : resolution === 'PT30M' ? 30 : 60;
      const ts = new Date(startMs + (latestPos - 1) * stepMinutes * 60 * 1000).toISOString();
      if (!bestTs || Date.parse(ts) > Date.parse(bestTs)) bestTs = ts;
    }
  }

  const renewablePsrTypes = new Set([
    'B01', // Biomass
    'B09', // Geothermal
    'B11', // Hydro Run-of-river and poundage
    'B12', // Hydro Water Reservoir
    'B13', // Marine
    'B15', // Other renewable
    'B16', // Solar
    'B17', // Waste (renewable fraction varies; treat as renewable-ish, adjust later)
    'B18', // Wind Offshore
    'B19', // Wind Onshore
  ]);

  const emissionFactors = {
    B02: 820, // Fossil Brown coal/Lignite
    B03: 490, // Fossil Coal-derived gas
    B04: 650, // Fossil Gas
    B05: 970, // Fossil Hard coal
    B06: 780, // Fossil Oil
    B07: 740, // Fossil Oil shale
    B08: 820, // Fossil Peat
    B10: 12,  // Hydro Pumped Storage (treat low; but often consumption/negative—handle cautiously)
    B14: 12,  // Nuclear (operational)
  };

  const total = Object.values(byType).reduce((a, b) => a + (Number.isFinite(b) ? b : 0), 0);
  const renewable = Object.entries(byType).reduce((a, [k, v]) => a + (renewablePsrTypes.has(k) ? v : 0), 0);

  const renewablePercent = total > 0 ? (renewable / total) * 100 : null;

  // Rough carbon intensity: weighted average using simple factors where known.
  let weighted = 0;
  let covered = 0;
  for (const [k, v] of Object.entries(byType)) {
    const ef = emissionFactors[k];
    if (!Number.isFinite(ef) || !Number.isFinite(v) || v <= 0) continue;
    weighted += ef * v;
    covered += v;
  }
  const carbonIntensityGPerKwh = covered > 0 ? weighted / covered : null;

  return {
    ts: bestTs,
    renewablePercent: Number.isFinite(renewablePercent) ? renewablePercent : null,
    carbonIntensityGPerKwh: Number.isFinite(carbonIntensityGPerKwh) ? carbonIntensityGPerKwh : null,
    raw: { byType },
  };
}
