// Professional Policy Platform Application
(function() {
    'use strict';
    
    // Prevent duplicate initialization
    if (window.appInitialized) {
        console.warn('App already initialized');
        return;
    }
    window.appInitialized = true;

let supabase;
let currentCountryId = null;
let currentCountryData = null;
let currentTableData = null;
let currentTableMetadata = null;
let currentViewMode = 'table';
let currentChart = null; // Store chart instance for cleanup
let energyFranceChart = null;
let energyFranceRange = 'day';
let energyFranceAutoRefresh = true;
let energyFranceAutoRefreshTimer = null;
let energyFranceChartLoadInFlight = null;
let energySelectedZone = null;
let energySelectedSource = null;

let energyEuChart = null;
let energyEuRange = 'day';
let energyEuAutoRefresh = true;
let energyEuAutoRefreshTimer = null;
let energyEuChartLoadInFlight = null;
let energyRealtimeChannel = null;
let energyRealtimeDebounce = null;

// Initialize app
document.addEventListener('DOMContentLoaded', async () => {
    try {
        console.log('Initializing app...');
        const config = await loadSupabaseConfig();
        console.log('Supabase URL:', config.url);

        if (!window.supabase) {
            console.error('Supabase library not loaded');
            showError('Supabase library failed to load. Please check your internet connection.');
            return;
        }

        // Avoid relying on third-party storage access (some browsers block it under Tracking Prevention).
        // This app uses anon-key read-only queries, so we do not need persisted sessions.
        const memoryStorage = {
            getItem: () => null,
            setItem: () => {},
            removeItem: () => {},
        };

        window.__supabaseUrl = config.url;
        window.__supabaseAnonKey = config.anonKey;
        supabase = window.supabase.createClient(config.url, config.anonKey, {
            auth: {
                persistSession: false,
                autoRefreshToken: false,
                detectSessionInUrl: false,
                storage: memoryStorage,
            },
        });
        console.log('✓ Supabase client created');
        
        // Test connection
        const { data: testData, error: testError } = await supabase.from('countries').select('count').limit(1);
        if (testError) {
            console.error('Supabase connection test failed:', testError);
            showError('Failed to connect to database: ' + testError.message);
            return;
        }
        console.log('✓ Supabase connection verified');
        
        // Setup navigation
        setupNavigation();

        // Chart.js hover crosshair for smart-meter charts
        if (window.Chart && !window.__energyCrosshairPluginRegistered) {
            window.__energyCrosshairPluginRegistered = true;
            Chart.register({
                id: 'energyCrosshair',
                afterDraw: (chart) => {
                    const tooltip = chart?.tooltip;
                    if (!tooltip || !tooltip._active || !tooltip._active.length) return;
                    const ctx = chart.ctx;
                    const x = tooltip._active[0].element.x;
                    const topY = chart.chartArea.top;
                    const bottomY = chart.chartArea.bottom;
                    ctx.save();
                    ctx.beginPath();
                    ctx.moveTo(x, topY);
                    ctx.lineTo(x, bottomY);
                    ctx.lineWidth = 1;
                    ctx.strokeStyle = 'rgba(38, 41, 88, 0.25)';
                    ctx.stroke();
                    ctx.restore();
                },
            });
        }

        // ── White background + E3G text watermark on every chart ────────
        // Two separate plugins:
        //   1. beforeDraw  — fill white so exported PNGs aren't transparent/black
        //   2. afterDraw   — draw text watermark (no external image on canvas to
        //                    avoid CORS canvas-taint that breaks toDataURL)
        if (window.Chart && !window.__e3gWatermarkPluginRegistered) {
            window.__e3gWatermarkPluginRegistered = true;
            Chart.register({
                id: 'e3gChartBg',
                beforeDraw(chart) {
                    const { ctx, width, height } = chart;
                    ctx.save();
                    ctx.fillStyle = '#ffffff';
                    ctx.fillRect(0, 0, width, height);
                    ctx.restore();
                },
            });
            Chart.register({
                id: 'e3gWatermark',
                afterDraw(chart) {
                    const { ctx, chartArea } = chart;
                    if (!chartArea) return;
                    ctx.save();
                    ctx.globalAlpha = 0.55;
                    ctx.font = '600 11px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
                    ctx.fillStyle = '#262958';
                    ctx.textBaseline = 'bottom';
                    ctx.textAlign = 'right';
                    ctx.fillText('E3G · EU Energy Team Data Platform', chartArea.right - 8, chartArea.bottom - 6);
                    ctx.restore();
                },
            });

            // Inject E3G logo into .chart-card (NOT .chart-container) so it
            // is never hidden behind the canvas stacking context.
            const logoUrl = 'https://rvxukmupuzxbrwicowyn.supabase.co/storage/v1/object/public/logo/logo.png';
            document.querySelectorAll('.chart-card').forEach(card => {
                if (card.querySelector('.chart-logo-overlay')) return;
                const img = document.createElement('img');
                img.className = 'chart-logo-overlay';
                img.src = logoUrl;
                img.alt = '';
                img.setAttribute('aria-hidden', 'true');
                card.appendChild(img);
            });

            // Pre-fetch logo as base64 data URL so export always has it
            // (avoids CORS timing issues at click time).
            window._e3gLogoDataUrl = null;
            fetch(logoUrl)
                .then(r => r.blob())
                .then(blob => new Promise(resolve => {
                    const reader = new FileReader();
                    reader.onload = () => resolve(reader.result);
                    reader.readAsDataURL(blob);
                }))
                .then(dataUrl => { window._e3gLogoDataUrl = dataUrl; })
                .catch(() => {});
        }

        // Country navigation must be loaded regardless of which page we'll
        // actually show (the sidebar is shared across pages).
        await loadCountryNavigation();

        // Restore the last page the user was on (persisted across reloads).
        // Falls back to the dashboard if nothing is stored or if the stored
        // page is invalid.
        // A URL in the address bar wins over the stored page: it is how someone
        // arrives from a shared link, and silently redirecting them to whatever
        // page they last visited would make links useless.
        const routed = readRouteFromHash();
        if (routed) {
            navigateToPage(routed.page, routed.countryId, {replaceHash: true});
        } else {
            const saved = readLastPageState();
            if (saved && saved.page && saved.page !== 'dashboard') {
                if (saved.page === 'country' && saved.countryId && saved.countryName) {
                    // Use navigateToCountry so breadcrumb + sidebar highlight + state
                    // are all restored consistently.
                    navigateToCountry(saved.countryId, saved.countryName);
                } else if (saved.page === 'country' && saved.countryId) {
                    // Older saved state without a countryName — route through
                    // navigateToPage; breadcrumb will update once loadCountryPage
                    // resolves the country record.
                    navigateToPage('country', saved.countryId);
                } else {
                    navigateToPage(saved.page);
                }
            } else {
                navigateToPage('home');
            }
        }

        // Back/forward and pasted links.
        window.addEventListener('hashchange', () => {
            const r = readRouteFromHash();
            if (r && r.page !== currentRoutePage) navigateToPage(r.page, r.countryId, {fromHash: true});
        });
    } catch (error) {
        console.error('Error initializing app:', error);
        console.error('Error details:', error.message, error.stack);
        showError('Failed to connect to database: ' + (error.message || 'Unknown error'));
    }
});

async function loadSupabaseConfig() {
    // Fast path: public config injected via `supabase_public_config.js`
    if (typeof SUPABASE_CONFIG !== 'undefined' && SUPABASE_CONFIG?.url && SUPABASE_CONFIG?.anonKey) {
        return { url: String(SUPABASE_CONFIG.url), anonKey: String(SUPABASE_CONFIG.anonKey) };
    }

    // Optional: fetch from serverless config endpoint (if enabled on the host).
    try {
        const res = await fetch('/api/config', { cache: 'no-store' });
        if (res.ok) {
            const json = await res.json();
            if (json?.url && json?.anonKey) return { url: String(json.url), anonKey: String(json.anonKey) };
        }
    } catch (_) {}

    throw new Error('Supabase config missing (expected `supabase_public_config.js` or /api/config)');
}

function setupEnergyRealtimeSubscription() {
    if (!supabase) return;
    // Clean up any existing channel
    try {
        if (energyRealtimeChannel) supabase.removeChannel(energyRealtimeChannel);
    } catch (_) {}
    energyRealtimeChannel = null;

    // Subscribe only while Energy Meter is active
    const pageActive = document.getElementById('energyMeterPage')?.classList.contains('active');
    if (!pageActive) return;

    // Subscribe to inserts for selected zone and EU aggregate (ENTSOE)
    const zone = energySelectedZone;
    const source = energySelectedSource || 'entsoe';

    const filters = [];
    if (zone) filters.push(`zone_id=eq.${zone}`);
    if (source) filters.push(`source=eq.${source}`);

    // Also listen for EU aggregate (entsoe)
    const euFilter = "zone_id=eq.EU,source=eq.entsoe";

    energyRealtimeChannel = supabase
        .channel('energy-meter-realtime')
        .on(
            'postgres_changes',
            { event: 'INSERT', schema: 'public', table: 'energy_mix_snapshots', filter: filters.join(',') },
            () => debounceEnergyRefresh()
        )
        .on(
            'postgres_changes',
            { event: 'INSERT', schema: 'public', table: 'energy_mix_snapshots', filter: euFilter },
            () => debounceEnergyRefresh()
        )
        .subscribe();
}

function debounceEnergyRefresh() {
    if (energyRealtimeDebounce) clearTimeout(energyRealtimeDebounce);
    energyRealtimeDebounce = setTimeout(() => {
        // Refresh the selected zone chart + EU chart quickly; table/map refresh happens via manual refresh or interval
        if (document.getElementById('energyMeterPage')?.classList.contains('active')) {
            if (energySelectedZone) loadEnergyRenewableShareChart(energySelectedZone, energyFranceRange, energySelectedSource);
            loadEnergyEuAggregateChart(energyEuRange);
        }
    }, 500);
}

function teardownEnergyRealtimeSubscription() {
    if (!supabase) return;
    try {
        if (energyRealtimeChannel) supabase.removeChannel(energyRealtimeChannel);
    } catch (_) {}
    energyRealtimeChannel = null;
}

// ── Sidebar collapse helpers ──────────────────────────────────────
function setSidebarCollapsed(collapsed) {
    document.body.classList.toggle('sidebar-collapsed', collapsed);
}

// Setup navigation handlers
function setupNavigation() {
    // Sidebar toggle (manual)
    document.getElementById('sidebarToggle')?.addEventListener('click', () => {
        setSidebarCollapsed(!document.body.classList.contains('sidebar-collapsed'));
    });
    // Reveal tab (shown on left edge when collapsed)
    document.getElementById('sidebarReveal')?.addEventListener('click', () => {
        setSidebarCollapsed(false);
    });
    
    // Page navigation
    document.querySelectorAll('[data-page]').forEach(el => {
        el.addEventListener('click', (e) => {
            e.preventDefault();
            const page = el.getAttribute('data-page');
            navigateToPage(page);
        });
    });
    
    // Tab navigation
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.getAttribute('data-tab');
            switchTab(tab);
        });
    });
    
    // Modal close
    document.getElementById('modalClose')?.addEventListener('click', closeModal);
    document.getElementById('measureModalClose')?.addEventListener('click', closeMeasureModal);

    // Close modal on outside click
    document.getElementById('dataModal')?.addEventListener('click', (e) => {
        if (e.target.id === 'dataModal') {
            closeModal();
        }
    });

    document.getElementById('measureModal')?.addEventListener('click', (e) => {
        if (e.target.id === 'measureModal') {
            closeMeasureModal();
        }
    });

    // Chart info modal
    document.getElementById('chartInfoClose')?.addEventListener('click', () => {
        document.getElementById('chartInfoModal')?.classList.remove('active');
    });
    document.getElementById('chartInfoModal')?.addEventListener('click', (e) => {
        if (e.target.id === 'chartInfoModal') {
            document.getElementById('chartInfoModal').classList.remove('active');
        }
    });
    document.querySelectorAll('.chart-info-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const chartId = btn.getAttribute('data-chart');
            if (chartId) showChartInfo(chartId);
        });
    });
    document.querySelectorAll('.chart-export-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const chartId = btn.getAttribute('data-chart');
            if (chartId) exportChart(chartId);
        });
    });

    // Contact form
    document.getElementById('contactForm')?.addEventListener('submit', (e) => {
        e.preventDefault();
        const name = (document.getElementById('contactName')?.value || '').trim();
        const email = (document.getElementById('contactEmail')?.value || '').trim();
        const subject = (document.getElementById('contactSubject')?.value || '').trim() || 'Message from EU Energy Data Platform';
        const message = (document.getElementById('contactMessage')?.value || '').trim();
        const body = encodeURIComponent(`From: ${name} <${email}>\n\n${message}`);
        const mailtoUrl = `mailto:lucas.deschenes@e3g.org?subject=${encodeURIComponent(subject)}&body=${body}`;
        window.location.href = mailtoUrl;
    });
    
    // Comparison table selector
    const comparisonSelect = document.getElementById('comparisonTableSelect');
    if (comparisonSelect) {
        comparisonSelect.addEventListener('change', (e) => {
            if (e.target.value) {
                loadCrossCountryComparison(e.target.value);
            }
        });
    }
}

// Remember the last page across reloads. We deliberately use localStorage so
// the preference survives browser restarts (most users expect "reload = stay
// where I was"). Wrapped in try/catch because some browsers with strict
// tracking prevention or private modes throw on any storage access.
const LAST_PAGE_STORAGE_KEY = 'app.lastPage';
const ELEC_METER_RANGES_KEY = 'app.elecMeterRanges';
const ELEC_METER_TAB_KEY = 'app.elecMeterTab';

function readElecMeterRanges() {
    try {
        const raw = localStorage.getItem(ELEC_METER_RANGES_KEY);
        if (!raw) return;
        const j = JSON.parse(raw);
        const ok = new Set(['day', 'week', 'month', '6m', '1y', '5y']);
        if (j.elecEuRange && ok.has(j.elecEuRange)) elecEuRange = j.elecEuRange;
        if (j.elecZoneRange && ok.has(j.elecZoneRange)) elecZoneRange = j.elecZoneRange;
    } catch (_) { /* ignore */ }
}

function saveElecMeterRanges() {
    try {
        localStorage.setItem(ELEC_METER_RANGES_KEY, JSON.stringify({
            elecEuRange,
            elecZoneRange,
        }));
    } catch (_) { /* ignore */ }
}

function readElecMeterTab() {
    try {
        const raw = (localStorage.getItem(ELEC_METER_TAB_KEY) || '').trim();
        const ok = new Set(['renewable', 'electricity', 'prices', 'demand', 'chart-builder']);
        if (raw && ok.has(raw)) return raw;
    } catch (_) { /* ignore */ }
    return null;
}
function saveElecMeterTab(tab) {
    try {
        if (!tab) return;
        localStorage.setItem(ELEC_METER_TAB_KEY, String(tab));
    } catch (_) { /* ignore */ }
}
function saveLastPageState(page, countryId = null, countryName = null) {
    try {
        // Preserve a previously-saved countryName when the caller didn't pass
        // one (e.g. navigateToPage is called directly without a name, but
        // navigateToCountry sets the name first).
        let preservedName = null;
        if (page === 'country' && !countryName) {
            try {
                const prev = JSON.parse(localStorage.getItem(LAST_PAGE_STORAGE_KEY) || 'null');
                if (prev && prev.page === 'country' && prev.countryId === countryId && prev.countryName) {
                    preservedName = prev.countryName;
                }
            } catch (_) {}
        }
        const payload = {
            page,
            countryId: countryId || null,
            countryName: countryName || preservedName || null,
            ts: Date.now(),
        };
        localStorage.setItem(LAST_PAGE_STORAGE_KEY, JSON.stringify(payload));
    } catch (_) { /* storage blocked; ignore */ }
}
function readLastPageState() {
    try {
        const raw = localStorage.getItem(LAST_PAGE_STORAGE_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed.page !== 'string') return null;
        const allowed = new Set(['dashboard', 'energy-meter', 'gas-meter', 'country', 'contact', 'about', 'terms']);
        if (!allowed.has(parsed.page)) return null;
        if (parsed.page === 'country' && !parsed.countryId) return null;
        return parsed;
    } catch (_) { return null; }
}

// ── Analytics helpers ─────────────────────────────────────────────
// Vercel Analytics (window.va) is loaded via script tag in index.html.
// Safe no-op when not available (local dev, ad-blockers, etc.).
function track(event, props) {
    try {
        if (typeof window.va === 'function') window.va('event', { name: event, data: props || {} });
    } catch (_) {}
}

// Navigate to page
// ── URL routing ────────────────────────────────────────────────────────────
// Every page is addressable as `#/<page>` (countries as `#/country/<id>`), so
// a page can be linked to, bookmarked and shared. Before this the app kept the
// current page only in localStorage, which meant every URL landed on whatever
// page that particular browser last had open.
const ROUTABLE_PAGES = new Set([
    'home', 'dashboard', 'energy-meter', 'gas-meter', 'heatwaves',
    'country-profile', 'contact', 'about', 'terms', 'country',
]);
let currentRoutePage = null;

function readRouteFromHash() {
    const raw = (location.hash || '').replace(/^#\/?/, '').trim();
    if (!raw) return null;
    const [page, id] = raw.split('/');
    if (!ROUTABLE_PAGES.has(page)) return null;
    return {page, countryId: page === 'country' ? (id || null) : null};
}

function writeRouteToHash(page, countryId, replace) {
    const hash = '#/' + page + (page === 'country' && countryId ? '/' + countryId : '');
    if (location.hash === hash) return;
    // replaceState on first load so the entry point does not leave a dead
    // history step behind the user's first Back press.
    if (replace && history.replaceState) history.replaceState(null, '', hash);
    else location.hash = hash;
}

function navigateToPage(page, countryId = null, opts = {}) {
    if (page !== 'energy-meter') {
        teardownEnergyRealtimeSubscription();
    }
    track('page_view', { page: countryId ? `country/${countryId}` : page });
    // Hide all pages
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));

    // Expand the sidebar when navigating away from chart builders
    if (page !== 'energy-meter' && page !== 'gas-meter') {
        setSidebarCollapsed(false);
    }

    // Layout mode: center info pages (Contact / About / Terms)
    const pageContent = document.querySelector('.page-content');
    if (pageContent) {
        const isInfo = page === 'contact' || page === 'about' || page === 'terms';
        pageContent.classList.toggle('info-page-layout', isInfo);
    }

    // Update navigation
    document.querySelectorAll('.nav-item, .country-nav-item').forEach(item => {
        item.classList.remove('active');
    });

    if (page === 'home') {
        document.getElementById('homePage')?.classList.add('active');
        document.getElementById('pageTitle').textContent = 'EU Energy Team Data Platform';
        return;
    }

    if (page === 'dashboard') {
        document.getElementById('dashboardPage').classList.add('active');
        document.querySelector('[data-page="dashboard"]').classList.add('active');
        document.getElementById('pageTitle').textContent = 'National renovation building plans';
        loadDashboard();
    } else if (page === 'energy-meter') {
        document.getElementById('energyMeterPage')?.classList.add('active');
        document.querySelector('[data-page="energy-meter"]')?.classList.add('active');
        document.getElementById('pageTitle').textContent = 'EU electricity meter';
        loadEnergyMeterPage();
    } else if (page === 'gas-meter') {
        document.getElementById('gasMeterPage')?.classList.add('active');
        document.querySelector('[data-page="gas-meter"]')?.classList.add('active');
        document.getElementById('pageTitle').textContent = 'EU gas meter';
        loadGasMeterPage();
    } else if (page === 'heatwaves') {
        document.getElementById('heatwavesPage')?.classList.add('active');
        document.querySelector('[data-page="heatwaves"]')?.classList.add('active');
        document.getElementById('pageTitle').textContent = 'Heatwaves';
        loadHeatwavesPage();
    } else if (page === 'country-profile') {
        document.getElementById('countryProfilePage')?.classList.add('active');
        document.querySelector('[data-page="country-profile"]')?.classList.add('active');
        document.getElementById('pageTitle').textContent = 'Country profile';
        loadCountryProfilePage();
    } else if (page === 'country' && countryId) {
        document.getElementById('countryPage').classList.add('active');
        document.getElementById('pageTitle').textContent = 'National renovation building plans';
        loadCountryPage(countryId);
    } else if (page === 'contact') {
        document.getElementById('contactPage')?.classList.add('active');
        document.querySelector('[data-page="contact"]')?.classList.add('active');
        document.getElementById('pageTitle').textContent = 'Contact';
    } else if (page === 'about') {
        document.getElementById('aboutPage')?.classList.add('active');
        document.querySelector('[data-page="about"]')?.classList.add('active');
        document.getElementById('pageTitle').textContent = 'About & Sitemap';
    } else if (page === 'terms') {
        document.getElementById('termsPage')?.classList.add('active');
        document.querySelector('[data-page="terms"]')?.classList.add('active');
        document.getElementById('pageTitle').textContent = 'Terms of Use';
    } else {
        // Unknown/invalid page -> fall back to dashboard so the app never
        // ends up blank.
        document.getElementById('dashboardPage').classList.add('active');
        document.querySelector('[data-page="dashboard"]').classList.add('active');
        document.getElementById('pageTitle').textContent = 'National renovation building plans';
        loadDashboard();
        saveLastPageState('dashboard');
        currentRoutePage = 'dashboard';
        if (!opts.fromHash) writeRouteToHash('dashboard', null, opts.replaceHash);
        return;
    }

    saveLastPageState(page, countryId);
    currentRoutePage = page;
    if (!opts.fromHash) writeRouteToHash(page, countryId, opts.replaceHash);
}

async function loadEnergyMeterPage() {
    // Bind the Renewable/Electricity tab switch once. The electricity-tab data
    // is loaded lazily on first click.
    setupElectricityMeterTabs();
    readElecMeterRanges();

    // Persist the electricity meter sub-tab across reloads. If the user was on
    // Prices/Demand/Generation, don't bounce them back to Renewables on refresh.
    const savedTab = readElecMeterTab();
    if (savedTab && savedTab !== 'renewable') {
        switchElectricityMeterTab(savedTab);
        return;
    }

    const statusEl = document.getElementById('energyMeterStatus');
    const tbody = document.getElementById('energyMeterTableBody');
    const refreshBtn = document.getElementById('energyRefreshBtn');
    const franceStatusEl = document.getElementById('energyFranceStatus');
    const rangeDayBtn = document.getElementById('energyRangeDayBtn');
    const rangeWeekBtn = document.getElementById('energyRangeWeekBtn');
    const rangeMonthBtn = document.getElementById('energyRangeMonthBtn');
    const range6mBtn = document.getElementById('energyRange6mBtn');
    const range1yBtn = document.getElementById('energyRange1yBtn');
    const range5yBtn = document.getElementById('energyRange5yBtn');
    const autoBtn = document.getElementById('energyAutoRefreshBtn');

    const euStatusEl = document.getElementById('energyEuStatus');
    const euDayBtn = document.getElementById('energyEuRangeDayBtn');
    const euWeekBtn = document.getElementById('energyEuRangeWeekBtn');
    const euMonthBtn = document.getElementById('energyEuRangeMonthBtn');
    const eu6mBtn = document.getElementById('energyEuRange6mBtn');
    const eu1yBtn = document.getElementById('energyEuRange1yBtn');
    const eu5yBtn = document.getElementById('energyEuRange5yBtn');
    const euAutoBtn = document.getElementById('energyEuAutoRefreshBtn');

    if (!tbody) return;

    const setStatus = (msg) => {
        if (statusEl) statusEl.textContent = msg || '';
    };
    const setFranceStatus = (msg) => {
        if (franceStatusEl) franceStatusEl.textContent = msg || '';
    };

    const renderLoading = () => {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color: var(--text-secondary); padding: 24px;">Loading...</td></tr>';
    };

    const fmtPct = (v) => (v == null ? '-' : `${Number(v).toFixed(1)}%`);
    const fmtNum = (v) => (v == null ? '-' : `${Math.round(Number(v))}`);
    const fmtTs = (v) => {
        if (!v) return '-';
        const d = new Date(v);
        if (Number.isNaN(d.getTime())) return String(v);
        return d.toLocaleString();
    };

    let latestRows = [];

    const load = async () => {
        try {
            setStatus('Fetching latest snapshot…');
            renderLoading();

            if (!supabase) {
                throw new Error('Supabase client not initialized.');
            }

            // Avoid querying the "latest per zone" view because it can time out on big backfills.
            // Instead, fetch a narrow recent window and dedupe.
            const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
            const { data, error } = await supabase
                .from('energy_mix_snapshots')
                .select('id, zone_id, country_code, ts, renewable_percent, carbon_intensity_g_per_kwh, source')
                .gte('ts', since)
                .order('ts', { ascending: false })
                .limit(2000);

            if (error) throw new Error(error.message);
            latestRows = Array.isArray(data) ? dedupeLatestByZone(data) : [];

            if (!latestRows.length) {
                tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color: var(--text-secondary); padding: 24px;">No data yet. Run ingestion (server) to populate snapshots.</td></tr>';
                setStatus('No snapshots found.');
                document.getElementById('energyLastUpdated').textContent = '-';
                document.getElementById('energyZones').textContent = '0';
                document.getElementById('energyAvgRenewable').textContent = '-';
                document.getElementById('energyAvgCO2').textContent = '-';
                return;
            }

            // Sort newest first for display
            latestRows.sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime());

            renderEnergyMap(latestRows);

            tbody.innerHTML = latestRows.map(r => {
                const zone = r.zone_id || r.country_code || '-';
                const ts = r.ts;
                const ren = r.renewable_percent;
                const co2 = r.carbon_intensity_g_per_kwh;
                const src = r.source || '-';
                return `
                    <tr class="energy-row" data-zone="${escapeHtml(String(zone))}" data-source="${escapeHtml(String(src))}">
                        <td>${escapeHtml(String(zone))}</td>
                        <td>${escapeHtml(fmtTs(ts))}</td>
                        <td>${escapeHtml(fmtPct(ren))}</td>
                        <td>${escapeHtml(fmtNum(co2))}</td>
                        <td>${escapeHtml(String(src))}</td>
                    </tr>
                `;
            }).join('');

            // Click-to-select (France chart defaults to FR)
            tbody.querySelectorAll('tr.energy-row').forEach(tr => {
                tr.addEventListener('click', () => {
                    const z = tr.getAttribute('data-zone');
                    const s = tr.getAttribute('data-source');
                    if (z) {
                        energySelectedZone = z;
                        energySelectedSource = s || null;
                        updateEnergyRangeButtonActive();
                        loadEnergyRenewableShareChart(z, energyFranceRange, energySelectedSource);
                        setupEnergyRealtimeSubscription();
                    }
                });
            });

            const newest = latestRows.reduce((acc, r) => {
                const t = new Date(r.ts).getTime();
                if (!Number.isFinite(t)) return acc;
                return Math.max(acc, t);
            }, 0);

            const avgRen = latestRows.reduce((acc, r) => acc + (Number.isFinite(Number(r.renewable_percent)) ? Number(r.renewable_percent) : 0), 0) / latestRows.length;
            const avgCO2 = latestRows.reduce((acc, r) => acc + (Number.isFinite(Number(r.carbon_intensity_g_per_kwh)) ? Number(r.carbon_intensity_g_per_kwh) : 0), 0) / latestRows.length;

            document.getElementById('energyLastUpdated').textContent = newest ? new Date(newest).toLocaleString() : '-';
            document.getElementById('energyZones').textContent = String(latestRows.length);
            document.getElementById('energyAvgRenewable').textContent = `${avgRen.toFixed(1)}%`;
            document.getElementById('energyAvgCO2').textContent = `${Math.round(avgCO2)} g/kWh`;

            setStatus(`Loaded ${latestRows.length} zones.`);
        } catch (err) {
            console.error('Electricity Meter load failed:', err);
            tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color: var(--error-color); padding: 24px;">Failed to load: ${escapeHtml(err.message || String(err))}</td></tr>`;
            setStatus('Failed to load.');
        }
    };

    if (refreshBtn && !refreshBtn.dataset.bound) {
        refreshBtn.dataset.bound = '1';
        refreshBtn.addEventListener('click', () => load());
    }

    // Selected zone chart controls
    const bindSelected = (btn, range) => {
        if (btn && !btn.dataset.bound) {
            btn.dataset.bound = '1';
            btn.addEventListener('click', () => {
                energyFranceRange = range;
                updateEnergyRangeButtonActive();
                if (energySelectedZone) loadEnergyRenewableShareChart(energySelectedZone, energyFranceRange, energySelectedSource);
            });
        }
    };
    bindSelected(rangeDayBtn, 'day');   // today (from midnight)
    bindSelected(rangeWeekBtn, 'week');
    bindSelected(rangeMonthBtn, 'month');
    bindSelected(range6mBtn, '6m');
    bindSelected(range1yBtn, '1y');
    bindSelected(range5yBtn, '5y');
    if (autoBtn && !autoBtn.dataset.bound) {
        autoBtn.dataset.bound = '1';
        autoBtn.addEventListener('click', () => {
            energyFranceAutoRefresh = !energyFranceAutoRefresh;
            autoBtn.textContent = energyFranceAutoRefresh ? 'Auto: On' : 'Auto: Off';
            setupEnergyFranceAutoRefresh();
        });
    }

    await load();

    // Default selection: FR entsoe (if present), otherwise first row.
    if (!energySelectedZone) {
        const frEntsoe = latestRows.find(r => (r.zone_id || r.country_code) === 'FR' && r.source === 'entsoe');
        const fallback = latestRows.find(r => (r.zone_id || r.country_code) && r.source);
        const pick = frEntsoe || fallback;
        if (pick) {
            energySelectedZone = pick.zone_id || pick.country_code;
            energySelectedSource = pick.source || null;
        }
    }

    updateEnergyRangeButtonActive();
    // Load selected zone chart after table
    if (energySelectedZone) {
        await loadEnergyRenewableShareChart(energySelectedZone, energyFranceRange, energySelectedSource);
    }
    setupEnergyFranceAutoRefresh();
    setupEnergyRealtimeSubscription();

    // EU chart controls
    const bindEu = (btn, range) => {
        if (btn && !btn.dataset.bound) {
            btn.dataset.bound = '1';
            btn.addEventListener('click', () => {
                energyEuRange = range;
                loadEnergyEuAggregateChart(energyEuRange);
            });
        }
    };
    bindEu(euDayBtn, 'day');
    bindEu(euWeekBtn, 'week');
    bindEu(euMonthBtn, 'month');
    bindEu(eu6mBtn, '6m');
    bindEu(eu1yBtn, '1y');
    bindEu(eu5yBtn, '5y');

    if (euAutoBtn && !euAutoBtn.dataset.bound) {
        euAutoBtn.dataset.bound = '1';
        euAutoBtn.addEventListener('click', () => {
            energyEuAutoRefresh = !energyEuAutoRefresh;
            euAutoBtn.textContent = energyEuAutoRefresh ? 'Auto: On' : 'Auto: Off';
            setupEnergyEuAutoRefresh();
        });
    }

    await loadEnergyEuAggregateChart(energyEuRange);
    setupEnergyEuAutoRefresh();
}

function clamp01(x) {
    if (!Number.isFinite(x)) return 0;
    return Math.max(0, Math.min(1, x));
}

function lerp(a, b, t) {
    return a + (b - a) * t;
}

function mixColorRedToGreen(pct) {
    // pct 0..100 → red→green (via orange-ish)
    const t = clamp01(Number(pct) / 100);
    // red (#ef4444) to green (#10b981) in RGB
    const r = Math.round(lerp(239, 16, t));
    const g = Math.round(lerp(68, 185, t));
    const b = Math.round(lerp(68, 129, t));
    return `rgb(${r}, ${g}, ${b})`;
}

function textColorForBg(pct) {
    const t = clamp01(Number(pct) / 100);
    // darker text on light colors; white-ish on dark-ish greens/reds
    return t > 0.55 ? 'rgba(255,255,255,0.95)' : 'rgba(15,23,42,0.92)';
}

function renderEnergyMap(latestRows) {
    const container = document.getElementById('energyMapContainer');
    if (!container) return;

    const rows = (latestRows || []).filter(r => (r.zone_id || r.country_code) && r.source === 'entsoe');
    if (!rows.length) {
        container.innerHTML = '<div class="chart-loading">No ENTSO‑E zone data yet.</div>';
        return;
    }

    // Try a real SVG map using GeoJSON; fallback to tile grid if fetch fails.
    renderEnergyGeoMap(container, rows).catch((e) => {
        console.warn('Geo map render failed, falling back to grid:', e);
        renderEnergyTileGrid(container, rows);
            mapFallbackNote(container, e);
    });
}

function zoneToCountryIso2(zoneId) {
    const z = String(zoneId || '').toUpperCase();
    if (z.startsWith('DK')) return 'DK';
    if (z.startsWith('SE')) return 'SE';
    if (z.startsWith('NO')) return 'NO';
    if (z === 'EU') return 'EU';
    // Basic assumption: zone is ISO2 already (AT, FR, ES, CH, GB, etc.)
    return z;
}

// Europe GeoJSON often uses ISO2 "UK"; ENTSO-E bidding zones use "GB". Map lookups must align.
function iso2GeoToDataKey(iso2) {
    const c = String(iso2 || '').toUpperCase();
    return c === 'UK' ? 'GB' : c;
}

function iso2GeoMatchesSelection(featureIso2, selectedIso2) {
    return iso2GeoToDataKey(featureIso2) === iso2GeoToDataKey(selectedIso2);
}

function pickZoneForCountry(rows, iso2) {
    const c = String(iso2 || '').toUpperCase();
    const match = c === 'UK' ? 'GB' : c;
    const candidates = rows.filter(r => zoneToCountryIso2(r.zone_id || r.country_code) === match);
    if (!candidates.length) return null;
    // Prefer "main" zones if present
    const preferred = {
        DK: ['DK1', 'DK2'],
        SE: ['SE3', 'SE2', 'SE4', 'SE1'],
        NO: ['NO1', 'NO2', 'NO3', 'NO4', 'NO5'],
    }[c];
    if (preferred) {
        for (const p of preferred) {
            const hit = candidates.find(r => String(r.zone_id || r.country_code).toUpperCase() === p);
            if (hit) return hit;
        }
    }
    // Else newest timestamp
    return candidates.sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime())[0];
}

function aggregateByCountry(rows) {
    const agg = new Map();
    for (const r of rows) {
        const iso2 = zoneToCountryIso2(r.zone_id || r.country_code);
        const pct = Number(r.renewable_percent);
        if (!Number.isFinite(pct)) continue;
        const prev = agg.get(iso2) || { sum: 0, n: 0, latestTs: null };
        prev.sum += pct;
        prev.n += 1;
        const t = r.ts ? new Date(r.ts).getTime() : NaN;
        if (Number.isFinite(t) && (!prev.latestTs || t > prev.latestTs)) prev.latestTs = t;
        agg.set(iso2, prev);
    }
    const out = {};
    agg.forEach((v, k) => {
        out[k] = { pct: v.n ? v.sum / v.n : null, latestTs: v.latestTs ? new Date(v.latestTs).toISOString() : null };
    });
    return out;
}

function renderEnergyTileGrid(container, rows) {
    const legend = `
        <div class="energy-map-legend">
            <span>Low renewables</span>
            <div class="energy-map-legend-bar"></div>
            <span>High renewables</span>
        </div>
    `;

    const tiles = rows
        .sort((a, b) => String(a.zone_id || a.country_code).localeCompare(String(b.zone_id || b.country_code)))
        .map(r => {
            const zone = String(r.zone_id || r.country_code);
            const pct = Number(r.renewable_percent);
            const bg = Number.isFinite(pct) ? mixColorRedToGreen(pct) : 'rgba(148,163,184,0.25)';
            const color = Number.isFinite(pct) ? textColorForBg(pct) : 'rgba(15,23,42,0.8)';
            const isActive = energySelectedZone === zone && (energySelectedSource || 'entsoe') === 'entsoe';
            const val = Number.isFinite(pct) ? `${pct.toFixed(1)}%` : '—';
            return `
                <div class="energy-map-tile ${isActive ? 'active' : ''}" data-zone="${escapeHtml(zone)}" style="background:${bg}; color:${color}">
                    <div class="energy-map-tile-code">${escapeHtml(zone)}</div>
                    <div class="energy-map-tile-value">${escapeHtml(val)}</div>
                </div>
            `;
        })
        .join('');

    container.innerHTML = `${legend}<div class="energy-map-grid">${tiles}</div>`;

    container.querySelectorAll('.energy-map-tile').forEach(el => {
        el.addEventListener('click', () => {
            const z = el.getAttribute('data-zone');
            if (!z) return;
            energySelectedZone = z;
            energySelectedSource = 'entsoe';
            updateEnergyRangeButtonActive();
            loadEnergyRenewableShareChart(z, energyFranceRange, 'entsoe');

            // Update active state
            container.querySelectorAll('.energy-map-tile').forEach(t => t.classList.remove('active'));
            el.classList.add('active');
        });
    });
}

let __energyEntsoeZonesGeoJsonPromise = null;
// A geo map that silently swaps itself for a tile grid looks like a design
// choice, not a failure: three maps sat broken for weeks because the only
// signal was a console warning nobody was looking at. Say it on the page.
function mapFallbackNote(container, err) {
    if (!container || container.querySelector?.('.map-fallback-note')) return;
    const note = document.createElement('div');
    note.className = 'map-fallback-note';
    note.textContent = `Map unavailable — showing tiles instead. ${err?.message || err || 'unknown error'}`;
    container.prepend(note);
}

// Countries with no data must still read as present-but-empty. The old value
// (rgba(148,163,184,0.18)) was 18% opacity on a white card, so the UK - which
// has had no ENTSO-E data since June 2021 - looked like it had been left off
// the map rather than simply having nothing to show.
const NO_DATA_FILL = 'rgba(148,163,184,0.26)';

function fetchEntsoeZonesGeoJsonOnce() {
    if (__energyEntsoeZonesGeoJsonPromise) return __energyEntsoeZonesGeoJsonPromise;
    // Electricity Maps zone geometry: the bidding-zone splits for DK/SE/NO.
    //
    // This is an OPTIONAL overlay — every consumer already guards with
    // `Array.isArray(zoneGeo?.features) ? … : []`. It used to reject on
    // failure, which rejected the Promise.all in all four map renderers and
    // dropped the entire map to the tile-grid fallback. It now resolves to
    // null so a missing overlay costs the overlay, not the map.
    //
    // The upstream path also moves: `geo/world.geojson` currently 404s on
    // jsDelivr, so the sources are tried in turn.
    // Sources, in order. The local file is optional: drop a zones GeoJSON at
    // that path and the overlay comes back automatically.
    //
    // Upstream is currently dead — electricitymaps moved the file, so
    // geo/world.geojson 404s on jsDelivr, and raw.githubusercontent rate-limits
    // with 429. Exactly the three maps that awaited this (renewable, carbon,
    // generation) were the three still falling back to the tile grid while
    // storage and flows, which never fetched it, rendered fine. The overlay is
    // cosmetic — the DK/SE/NO/GB bidding-zone split — so it must never sit on
    // the critical path for a base map again.
    const urls = [
        'assets/geo/entsoe-zones.geojson',
        'https://cdn.jsdelivr.net/gh/electricitymaps/electricitymaps-contrib@master/geo/world.geojson',
    ];
    __energyEntsoeZonesGeoJsonPromise = (async () => {
        for (const url of urls) {
            try {
                const r = await fetch(url);
                if (!r.ok) continue;
                const j = await r.json();
                // The SPA rewrite returns index.html for a missing asset with
                // HTTP 200, so check the shape rather than the status.
                if (!j || !Array.isArray(j.features) || !j.features.length) continue;
                return j;
            } catch (_) { /* try the next source */ }
        }
        console.info('Zone overlay GeoJSON unavailable — maps render without the DK/SE/NO/GB bidding-zone split.');
        return null;
    })();
    return __energyEntsoeZonesGeoJsonPromise;
}

let __energyEuropeCountriesGeoJsonPromise = null;
function fetchEuropeCountriesGeoJsonOnce() {
    if (__energyEuropeCountriesGeoJsonPromise) return __energyEuropeCountriesGeoJsonPromise;
    // Country outlines: the base layer every map needs.
    //
    // Served from the repo first. This used to be fetched straight from
    // raw.githubusercontent.com on every page load, so whenever GitHub
    // rate-limited the visitor's IP (HTTP 429) every map on the platform
    // silently dropped to the tile-grid fallback — with nothing in the UI to
    // say why. A ~1.6 MB file we control is not worth that exposure.
    const urls = [
        'assets/geo/europe-countries.geojson',
        'https://cdn.jsdelivr.net/gh/leakyMirror/map-of-europe@master/GeoJSON/europe.geojson',
        'https://raw.githubusercontent.com/leakyMirror/map-of-europe/master/GeoJSON/europe.geojson',
    ];
    __energyEuropeCountriesGeoJsonPromise = (async () => {
        let lastErr = null;
        for (const url of urls) {
            try {
                const r = await fetch(url);
                if (!r.ok) { lastErr = new Error(`GeoJSON HTTP ${r.status} from ${url}`); continue; }
                const j = await r.json();
                // vercel.json rewrites any unmatched path to index.html, so a
                // not-yet-deployed asset comes back as HTML with HTTP 200.
                // Check the shape rather than trusting the status code.
                if (!j || !Array.isArray(j.features) || !j.features.length) {
                    lastErr = new Error(`GeoJSON from ${url} has no features (wrong file served?)`);
                    continue;
                }
                return j;
            } catch (e) { lastErr = e; }
        }
        // Do not leave a rejected promise in the cache: one transient failure
        // would otherwise keep every map broken for the rest of the session.
        __energyEuropeCountriesGeoJsonPromise = null;
        throw lastErr || new Error('Country GeoJSON unavailable');
    })();
    return __energyEuropeCountriesGeoJsonPromise;
}

function projectLonLat(lon, lat, width, height, bounds = null, padding = 0) {
    // Simple equirectangular projection. If bounds are provided, fit to them.
    const b = bounds || { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };
    const w = Math.max(1, width - padding * 2);
    const h = Math.max(1, height - padding * 2);
    const x = padding + (Number(lon) - b.minLon) / (b.maxLon - b.minLon) * w;
    const y = padding + (b.maxLat - Number(lat)) / (b.maxLat - b.minLat) * h;
    return [x, y];
}

function polygonToPath(coords, width, height, bounds = null, padding = 0) {
    // coords: [ [lon,lat], ... ] ring
    let d = '';
    for (let i = 0; i < coords.length; i++) {
        const [lon, lat] = coords[i];
        const [x, y] = projectLonLat(lon, lat, width, height, bounds, padding);
        d += (i === 0 ? 'M' : 'L') + x.toFixed(2) + ' ' + y.toFixed(2) + ' ';
    }
    return d + 'Z';
}

function normalizeZoneNameToId(zoneName) {
    const z = String(zoneName || '').trim();
    if (!z) return null;
    // Common formats in electricitymaps geo: "SE-SE4", "DK-DK1", "NO-NO5", "FR", "DE", etc.
    const parts = z.split('-').filter(Boolean);
    const last = parts[parts.length - 1] || z;
    return String(last).toUpperCase();
}

function computeGeoJsonBounds(features) {
    let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
    const walk = (coords) => {
        if (!coords) return;
        if (Array.isArray(coords) && typeof coords[0] === 'number' && typeof coords[1] === 'number') {
            const lon = Number(coords[0]);
            const lat = Number(coords[1]);
            if (Number.isFinite(lon) && Number.isFinite(lat)) {
                minLon = Math.min(minLon, lon);
                maxLon = Math.max(maxLon, lon);
                minLat = Math.min(minLat, lat);
                maxLat = Math.max(maxLat, lat);
            }
            return;
        }
        if (Array.isArray(coords)) {
            for (const c of coords) walk(c);
        }
    };
    for (const f of features || []) {
        walk(f?.geometry?.coordinates);
    }
    if (!Number.isFinite(minLon) || !Number.isFinite(minLat) || !Number.isFinite(maxLon) || !Number.isFinite(maxLat)) {
        return { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };
    }
    const padLon = (maxLon - minLon) * 0.02;
    const padLat = (maxLat - minLat) * 0.02;
    return { minLon: minLon - padLon, maxLon: maxLon + padLon, minLat: minLat - padLat, maxLat: maxLat + padLat };
}

function coordsAnyPointInBbox(coords, bbox) {
    // bbox: { minLon, maxLon, minLat, maxLat }
    if (!coords) return false;
    if (Array.isArray(coords) && typeof coords[0] === 'number' && typeof coords[1] === 'number') {
        const lon = Number(coords[0]);
        const lat = Number(coords[1]);
        return Number.isFinite(lon) && Number.isFinite(lat) &&
            lon >= bbox.minLon && lon <= bbox.maxLon &&
            lat >= bbox.minLat && lat <= bbox.maxLat;
    }
    if (Array.isArray(coords)) {
        for (const c of coords) {
            if (coordsAnyPointInBbox(c, bbox)) return true;
        }
    }
    return false;
}

function geometryBounds(geometry) {
    let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
    const walk = (coords) => {
        if (!coords) return;
        if (Array.isArray(coords) && typeof coords[0] === 'number' && typeof coords[1] === 'number') {
            const lon = Number(coords[0]);
            const lat = Number(coords[1]);
            if (Number.isFinite(lon) && Number.isFinite(lat)) {
                minLon = Math.min(minLon, lon);
                maxLon = Math.max(maxLon, lon);
                minLat = Math.min(minLat, lat);
                maxLat = Math.max(maxLat, lat);
            }
            return;
        }
        if (Array.isArray(coords)) for (const c of coords) walk(c);
    };
    walk(geometry?.coordinates);
    if (!Number.isFinite(minLon) || !Number.isFinite(minLat) || !Number.isFinite(maxLon) || !Number.isFinite(maxLat)) {
        return null;
    }
    return { minLon, maxLon, minLat, maxLat };
}

function filterGeometryToBbox(geometry, bbox) {
    // Keep only Polygon/MultiPolygon parts that intersect the bbox (by point inclusion heuristic).
    if (!geometry?.type || !geometry?.coordinates) return null;
    const type = geometry.type;
    const coords = geometry.coordinates;
    if (type === 'Polygon') {
        // coords: [ ring1, ring2... ]
        if (!coordsAnyPointInBbox(coords?.[0], bbox)) return null;
        // Guard: sometimes a polygon barely intersects Europe but extends far outside (e.g. huge country polygon).
        // If the polygon's bounds are far outside the bbox, drop it instead of shrinking the whole map.
        const b = geometryBounds(geometry);
        if (b) {
            const margin = 6; // degrees
            if (
                b.maxLon > bbox.maxLon + margin || b.minLon < bbox.minLon - margin ||
                b.maxLat > bbox.maxLat + margin || b.minLat < bbox.minLat - margin
            ) {
                return null;
            }
        }
        return geometry;
    }
    if (type === 'MultiPolygon') {
        // coords: [ polygon, polygon... ] where polygon: [ ring1, ring2... ]
        const kept = [];
        for (const poly of coords) {
            if (coordsAnyPointInBbox(poly?.[0], bbox)) kept.push(poly);
        }
        if (!kept.length) return null;
        const out = { type: 'MultiPolygon', coordinates: kept };
        const b = geometryBounds(out);
        if (b) {
            const margin = 6;
            if (
                b.maxLon > bbox.maxLon + margin || b.minLon < bbox.minLon - margin ||
                b.maxLat > bbox.maxLat + margin || b.minLat < bbox.minLat - margin
            ) {
                return null;
            }
        }
        return out;
    }
    return null;
}

async function renderEnergyGeoMap(container, rows) {
    // Hybrid map:
    // - Base layer: country polygons (aligned, complete)
    // - Overlay: bidding zones for DK/SE/NO (granularity where users expect it)
    const [countryGeo, zoneGeo] = await Promise.all([
        fetchEuropeCountriesGeoJsonOnce(),
        // .catch here as well as inside: a cosmetic overlay must never be
        // able to reject the Promise.all and take the base map down with it.
        fetchEntsoeZonesGeoJsonOnce().catch(() => null),
    ]);
    const hasZoneOverlay = Array.isArray(zoneGeo?.features) && zoneGeo.features.length > 0;

    const byCountry = aggregateByCountry(rows);
    const byZone = {};
    for (const r of rows || []) {
        const z = String(r.zone_id || r.country_code || '').toUpperCase();
        const pct = Number(r.renewable_percent);
        if (!z || !Number.isFinite(pct)) continue;
        byZone[z] = pct;
    }

    const width = 1400;
    const height = 860;
    const padding = 10;
    // Keep the proven Europe framing (stable alignment)
    const bounds = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };

    const selectedZone = String(energySelectedZone || '').toUpperCase();
    const selectedIso2 = zoneToCountryIso2(selectedZone);
    const selectedLabel = selectedZone ? selectedZone : '—';
    const selectedPct =
        selectedZone && Object.prototype.hasOwnProperty.call(byZone, selectedZone)
            ? byZone[selectedZone]
            : (selectedIso2 && byCountry[selectedIso2]?.pct);

    container.innerHTML = `
        <div class="energy-map-shell">
            <div class="energy-map-top">
                <div class="energy-map-top-left">
                    <div class="energy-map-title">Renewable share map</div>
                    <div class="energy-map-subtitle">Countries + bidding zones for DK/SE/NO (click to chart)</div>
                </div>
                <div class="energy-map-top-right">
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Selected</div>
                        <div class="energy-map-chip-value">${escapeHtml(selectedLabel)}</div>
                    </div>
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Renewables</div>
                        <div class="energy-map-chip-value">${Number.isFinite(selectedPct) ? `${selectedPct.toFixed(1)}%` : '—'}</div>
                    </div>
                </div>
            </div>
            <div class="energy-map-legend energy-map-legend--premium">
                <span>Low</span>
                <div class="energy-map-legend-bar"></div>
                <span>High</span>
            </div>
            <div class="energy-map-stage">
                <svg class="energy-geo-map" viewBox="0 0 ${width} ${height}" role="img" aria-label="Renewable share map"></svg>
            </div>
        </div>
    `;

    const svg = container.querySelector('svg.energy-geo-map');
    if (!svg) return;

    let tooltip = document.querySelector('.energy-map-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.className = 'energy-map-tooltip';
        tooltip.style.display = 'none';
        document.body.appendChild(tooltip);
    }

    // Base countries (exclude DK/SE/NO because we'll overlay their zones)
    const countryFeatures = Array.isArray(countryGeo?.features) ? countryGeo.features : [];
    for (const f of countryFeatures) {
        const iso2 = String(f?.properties?.ISO2 || '').toUpperCase();
        if (!iso2) continue;
        if (iso2 === 'RU' || iso2 === 'BY') continue;
        // Only cede these to the bidding-zone overlay if that overlay actually
        // loaded; otherwise DK/SE/NO would be drawn by nobody and vanish.
        if (hasZoneOverlay && (iso2 === 'DK' || iso2 === 'SE' || iso2 === 'NO')) continue;
        // GB rendered via zone overlay below (same as DK/SE/NO); skip base layer to avoid double-draw
        // GB/UK were dropped from the country layer here because the overlay was
        // meant to draw them. The load, storage and flows maps never did this,
        // which is why the UK appears on those and vanished from these three.
        // Only cede it when the overlay is actually available.
        if (hasZoneOverlay && (iso2 === 'GB' || iso2 === 'UK')) continue;

        const dataKey = iso2GeoToDataKey(iso2);
        const val = byCountry[dataKey]?.pct;
        const fill = Number.isFinite(val) ? mixColorRedToGreen(val) : NO_DATA_FILL;

        const geom = f.geometry;
        if (!geom) continue;
        const type = geom.type;
        const coords = geom.coordinates;

        const paths = [];
        if (type === 'Polygon') {
            // first ring is outer
            paths.push(polygonToPath(coords[0], width, height, bounds, padding));
        } else if (type === 'MultiPolygon') {
            for (const poly of coords) {
                if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
            }
        } else {
            continue;
        }

        const d = paths.join(' ');
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', d);
        path.setAttribute('fill', fill);
        path.setAttribute('data-iso2', iso2);
        path.style.cursor = 'pointer';
        if (selectedIso2 && iso2GeoMatchesSelection(iso2, selectedIso2)) {
            path.classList.add('is-selected');
        }

        path.addEventListener('mouseenter', () => {
            const pct = byCountry[dataKey]?.pct;
            tooltip.style.display = 'block';
            tooltip.textContent = `${iso2} — ${Number.isFinite(pct) ? pct.toFixed(1) + '%' : '—'}`;
        });
        path.addEventListener('mousemove', (e) => {
            tooltip.style.left = `${e.clientX}px`;
            tooltip.style.top = `${e.clientY}px`;
        });
        path.addEventListener('mouseleave', () => {
            tooltip.style.display = 'none';
        });
        path.addEventListener('click', () => {
            const picked = pickZoneForCountry(rows, iso2);
            if (!picked) return;
            energySelectedZone = String(picked.zone_id || picked.country_code);
            energySelectedSource = 'entsoe';
            updateEnergyRangeButtonActive();
            loadEnergyRenewableShareChart(energySelectedZone, energyFranceRange, 'entsoe');
            setupEnergyRealtimeSubscription();
            // Re-render map so selection chips + highlight update immediately
            renderEnergyGeoMap(container, rows).catch(() => {});
        });

        svg.appendChild(path);
    }

    // Overlay bidding zones for DK/SE/NO/GB (GB = single national zone)
    const europeBbox = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };
    const zoneFeaturesAll = Array.isArray(zoneGeo?.features) ? zoneGeo.features : [];
    const overlayZones = new Set(['DK1', 'DK2', 'SE1', 'SE2', 'SE3', 'SE4', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'GB']);
    for (const f of zoneFeaturesAll) {
        const zoneId = normalizeZoneNameToId(f?.properties?.zoneName);
        if (!zoneId || !overlayZones.has(zoneId)) continue;
        const geom = filterGeometryToBbox(f?.geometry, europeBbox);
        if (!geom) continue;

        const val = byZone[zoneId];
        const fill = Number.isFinite(val) ? mixColorRedToGreen(val) : NO_DATA_FILL;

        const type = geom.type;
        const coords = geom.coordinates;
        const paths = [];
        if (type === 'Polygon') {
            paths.push(polygonToPath(coords[0], width, height, bounds, padding));
        } else if (type === 'MultiPolygon') {
            for (const poly of coords) {
                if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
            }
        } else {
            continue;
        }

        const d = paths.join(' ');
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', d);
        path.setAttribute('fill', fill);
        path.setAttribute('data-zone', zoneId);
        path.style.cursor = 'pointer';
        path.classList.add('bz-overlay');
        if (selectedZone && zoneId === selectedZone) path.classList.add('is-selected');

        path.addEventListener('mouseenter', () => {
            const pct = byZone[zoneId];
            tooltip.style.display = 'block';
            tooltip.textContent = `${zoneId} — ${Number.isFinite(pct) ? pct.toFixed(1) + '%' : '—'}`;
        });
        path.addEventListener('mousemove', (e) => {
            tooltip.style.left = `${e.clientX}px`;
            tooltip.style.top = `${e.clientY}px`;
        });
        path.addEventListener('mouseleave', () => {
            tooltip.style.display = 'none';
        });
        path.addEventListener('click', () => {
            energySelectedZone = zoneId;
            energySelectedSource = 'entsoe';
            updateEnergyRangeButtonActive();
            loadEnergyRenewableShareChart(energySelectedZone, energyFranceRange, 'entsoe');
            setupEnergyRealtimeSubscription();
            renderEnergyGeoMap(container, rows).catch(() => {});
        });

        svg.appendChild(path);
    }

    // Clean up tooltip if leaving page
    const page = document.getElementById('energyMeterPage');
    const obs = new MutationObserver(() => {
        if (!page?.classList.contains('active')) {
            tooltip.style.display = 'none';
        }
    });
    if (page) obs.observe(page, { attributes: true, attributeFilter: ['class'] });
}

function dedupeLatestByZone(rows) {
    const seen = new Set();
    const out = [];
    for (const r of rows) {
        const key = r.zone_id || r.country_code || r.id;
        if (seen.has(key)) continue;
        seen.add(key);
        out.push(r);
    }
    return out;
}

function rangeToSinceIso(range) {
    const now = Date.now();
    if (range === 'day') {
        const d = new Date();
        d.setHours(0, 0, 0, 0);
        return d.toISOString();
    }
    if (range === 'week') return new Date(now - 7 * 24 * 60 * 60 * 1000).toISOString();
    if (range === 'month') return new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString();
    if (range === '6m') return new Date(now - 182 * 24 * 60 * 60 * 1000).toISOString();
    if (range === '1y') return new Date(now - 365 * 24 * 60 * 60 * 1000).toISOString();
    if (range === '5y') return new Date(now - 5 * 365 * 24 * 60 * 60 * 1000).toISOString();
    return new Date(now - 24 * 60 * 60 * 1000).toISOString();
}

function euRangeToSinceIso(range) {
    const now = Date.now();
    if (range === 'week') return new Date(now - 7 * 24 * 60 * 60 * 1000).toISOString();
    if (range === 'month') return new Date(now - 30 * 24 * 60 * 60 * 1000).toISOString();
    if (range === '6m') return new Date(now - 182 * 24 * 60 * 60 * 1000).toISOString();
    if (range === '1y') return new Date(now - 365 * 24 * 60 * 60 * 1000).toISOString();
    if (range === '5y') return new Date(now - 5 * 365 * 24 * 60 * 60 * 1000).toISOString();
    return new Date(now - 24 * 60 * 60 * 1000).toISOString(); // day
}

async function loadEnergyRenewableShareChart(zone, range, source = null) {
    // Prevent overlapping refreshes (auto-refresh + button clicks)
    if (energyFranceChartLoadInFlight) {
        return await energyFranceChartLoadInFlight;
    }

    energyFranceChartLoadInFlight = (async () => {
    const statusEl = document.getElementById('energyFranceStatus');
    const titleEl = document.getElementById('energyFranceChartTitle');
    const canvas = document.getElementById('energyFranceChart');
    if (!canvas) return;

    const setStatus = (msg) => {
        if (statusEl) statusEl.textContent = msg || '';
    };

    try {
        if (!supabase) throw new Error('Supabase client not initialized.');

        const since = rangeToSinceIso(range);
        setStatus(`Loading ${zone} history (${range})…`);
        if (titleEl) titleEl.textContent = `${zone} — Renewable share (%)${source ? ` [${source}]` : ''}`;

        const useWeekly = range === '5y';
        const useDaily = range === '6m' || range === '1y';
        const table = useWeekly ? 'energy_mix_weekly' : useDaily ? 'energy_mix_daily' : 'energy_mix_snapshots';

        const maxPoints =
            useWeekly ? 400 : // ~7.7 years of weekly points
            useDaily ? 900 :  // ~2.4 years of daily points
            2000;

        let query = supabase
            .from(table)
            .select('ts, renewable_percent, source')
            .eq('zone_id', zone)
            .gte('ts', since)
            .order('ts', { ascending: false })
            .limit(maxPoints);

        if (source) query = query.eq('source', source);

        const { data, error } = await query;

        if (error) throw new Error(error.message);
        const rows = (Array.isArray(data) ? data : []).reverse();

        const points = rows
            .filter(r => r.ts && Number.isFinite(Number(r.renewable_percent)))
            .map(r => ({ ts: r.ts, y: Number(r.renewable_percent) }));

        // Chart.js "time" scale requires a date adapter; to keep this dependency-free,
        // we render a category axis with formatted timestamps.
        const labels = points.map(p => {
            const d = new Date(p.ts);
            if (Number.isNaN(d.getTime())) return String(p.ts);
            if (useWeekly || useDaily) return d.toLocaleDateString();
            return d.toLocaleString();
        });
        const series = points.map(p => p.y);

        if (!points.length) {
            setStatus(`No data for ${zone} in selected range yet.`);
        } else {
            const last = points[points.length - 1];
            const lastD = new Date(last.ts);
            setStatus(`Last: ${last.y.toFixed(1)}% @ ${Number.isNaN(lastD.getTime()) ? last.ts : lastD.toLocaleString()}`);
        }

        const ctx = canvas.getContext('2d');
        if (!ctx) return;

        // Destroy any existing chart bound to this canvas (Chart.js keeps a registry).
        const existing = Chart.getChart(canvas);
        if (existing) existing.destroy();
        if (energyFranceChart) {
            try { energyFranceChart.destroy(); } catch (_) {}
            energyFranceChart = null;
        }

        const pointRadius = series.length <= 2 ? 3 : 0;
        energyFranceChart = new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [{
                    label: 'Renewable share (%)',
                    data: series,
                    borderColor: '#10b981',
                    backgroundColor: 'rgba(16, 185, 129, 0.14)',
                    fill: true,
                    tension: 0.25,
                    pointRadius,
                    borderWidth: 2,
                }],
                labels,
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                parsing: true,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: true, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } },
                    tooltip: {
                        backgroundColor: 'rgba(15, 23, 42, 0.92)',
                        titleColor: '#fff',
                        bodyColor: '#fff',
                        padding: 10,
                        displayColors: false,
                        callbacks: {
                            label: (ctx) => `${Number(ctx.parsed.y).toFixed(1)}%`,
                        },
                    },
                },
                scales: {
                    x: {
                        type: 'category',
                        ticks: { maxRotation: 0 },
                        grid: { display: false },
                    },
                    y: {
                        suggestedMin: 0,
                        suggestedMax: 100,
                        ticks: { callback: (v) => `${v}%` },
                        grid: { color: 'rgba(148, 163, 184, 0.25)' },
                    },
                },
            },
        });
    } catch (err) {
        console.error('Energy chart load failed:', err);
        setStatus(`Failed: ${err.message || String(err)}`);
    }
    })();

    try {
        return await energyFranceChartLoadInFlight;
    } finally {
        energyFranceChartLoadInFlight = null;
    }
}

function setupEnergyFranceAutoRefresh() {
    if (energyFranceAutoRefreshTimer) {
        clearInterval(energyFranceAutoRefreshTimer);
        energyFranceAutoRefreshTimer = null;
    }
    if (!energyFranceAutoRefresh) return;

    energyFranceAutoRefreshTimer = setInterval(() => {
        const pageActive = document.getElementById('energyMeterPage')?.classList.contains('active');
        if (!pageActive) return;
        if (!energySelectedZone) return;
        loadEnergyRenewableShareChart(energySelectedZone, energyFranceRange, energySelectedSource);
    }, 60_000);
}

function updateEnergyRangeButtonActive() {
    const map = {
        'day': 'energyRangeDayBtn',
        'week': 'energyRangeWeekBtn',
        'month': 'energyRangeMonthBtn',
        '6m': 'energyRange6mBtn',
        '1y': 'energyRange1yBtn',
        '5y': 'energyRange5yBtn',
    };
    Object.entries(map).forEach(([range, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        if (energyFranceRange === range) el.classList.add('active');
        else el.classList.remove('active');
    });
}

async function loadEnergyEuAggregateChart(range) {
    if (energyEuChartLoadInFlight) return await energyEuChartLoadInFlight;

    energyEuChartLoadInFlight = (async () => {
        const statusEl = document.getElementById('energyEuStatus');
        const titleEl = document.getElementById('energyEuChartTitle');
        const canvas = document.getElementById('energyEuChart');
        if (!canvas) return;

        const setStatus = (msg) => {
            if (statusEl) statusEl.textContent = msg || '';
        };

        try {
            if (!supabase) throw new Error('Supabase client not initialized.');

            const since = euRangeToSinceIso(range);
            setStatus(`Loading EU history (${range})…`);
            if (titleEl) titleEl.textContent = `EU — Renewable share (%)`;

            const useWeekly = range === '5y';
            const useDaily = range === '6m' || range === '1y';
            const use15m = range === 'day' || range === 'week' || range === 'month';

            // Always use computed EU aggregate (materialized) so ranges are consistent
            const table = useWeekly
                ? 'energy_eu_weekly_mv'
                : useDaily
                ? 'energy_eu_daily_mv'
                : use15m
                ? 'energy_eu_15m_mv'
                : 'energy_eu_15m_mv';

            const maxPoints =
                useWeekly ? 400 :
                useDaily ? 900 :
                range === 'month' ? 3200 : 2000;

            const { data, error } = await supabase
                .from(table)
                .select('ts, renewable_percent')
                .gte('ts', since)
                .order('ts', { ascending: false })
                .limit(maxPoints);

            if (error) throw new Error(error.message);
            const rows = (Array.isArray(data) ? data : []).reverse();

            const points = rows
                .filter(r => r.ts && Number.isFinite(Number(r.renewable_percent)))
                .map(r => ({ ts: r.ts, y: Number(r.renewable_percent) }));

            const labels = points.map(p => {
                const d = new Date(p.ts);
                if (Number.isNaN(d.getTime())) return String(p.ts);
                if (useWeekly || useDaily) return d.toLocaleDateString();
                return d.toLocaleString();
            });
            const series = points.map(p => p.y);

            if (!points.length) {
                setStatus('No EU data yet. Schedule the ENTSO‑E ingest function.');
            } else {
                const last = points[points.length - 1];
                const lastD = new Date(last.ts);
                setStatus(`Last: ${last.y.toFixed(1)}% @ ${Number.isNaN(lastD.getTime()) ? last.ts : lastD.toLocaleString()}`);
            }

            const ctx = canvas.getContext('2d');
            if (!ctx) return;

            const existing = Chart.getChart(canvas);
            if (existing) existing.destroy();
            if (energyEuChart) {
                try { energyEuChart.destroy(); } catch (_) {}
                energyEuChart = null;
            }

            const euPointRadius = series.length <= 2 ? 3 : 0;
            energyEuChart = new Chart(ctx, {
                type: 'line',
                data: {
                    datasets: [{
                        label: 'EU renewable share (%)',
                        data: series,
                        borderColor: '#2563eb',
                        backgroundColor: 'rgba(37, 99, 235, 0.12)',
                        fill: true,
                        tension: 0.25,
                        pointRadius: euPointRadius,
                        borderWidth: 2,
                    }],
                    labels,
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                parsing: true,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { display: true, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } } },
                    scales: {
                        x: { type: 'category', ticks: { maxRotation: 0 } },
                        y: { suggestedMin: 0, suggestedMax: 100, ticks: { callback: (v) => `${v}%` } },
                    },
                },
            });
        } catch (err) {
            console.error('EU chart load failed:', err);
            const statusEl2 = document.getElementById('energyEuStatus');
            if (statusEl2) statusEl2.textContent = `Failed: ${err.message || String(err)}`;
        }
    })();

    try {
        return await energyEuChartLoadInFlight;
    } finally {
        energyEuChartLoadInFlight = null;
    }
}

function setupEnergyEuAutoRefresh() {
    if (energyEuAutoRefreshTimer) {
        clearInterval(energyEuAutoRefreshTimer);
        energyEuAutoRefreshTimer = null;
    }
    if (!energyEuAutoRefresh) return;

    energyEuAutoRefreshTimer = setInterval(() => {
        const pageActive = document.getElementById('energyMeterPage')?.classList.contains('active');
        if (!pageActive) return;
        loadEnergyEuAggregateChart(energyEuRange);
    }, 60_000);
}

// =========================
// Electricity tab (total generation, MW)
// Reads the same energy_mix_snapshots table as the Renewable tab but pulls
// raw->>'totalMw' (per zone) and raw->>'euTotalMw' (EU aggregate row).
// =========================

let elecEuChart = null;
let elecEuRange = '1y';
let elecEuMode = 'intraday';
let elecEuAggRange = 'month';
let elecEuChartLoadInFlight = null;

let elecZoneChart = null;
let elecZoneRange = 'day';
let elecZoneMode = 'intraday';
let elecZoneAggRange = 'month';
let elecZoneChartLoadInFlight = null;

let elecTabInited = false;
let elecLatestRows = [];            // latest rows per zone with totalMw extracted
let elecSelectedZone = null;        // separate from renewable selection so the tabs don't fight
let elecSelectedSource = null;

// Demand (load) tab
let loadEuChart = null;
let loadEuRange = '1y';
let loadEuMode = 'intraday'; // 'intraday' | 'aggregate'
let loadEuAggRange = 'month'; // 'day' | 'week' | 'month' | 'year'
let loadEuChartLoadInFlight = null;

let loadZoneChart = null;
let loadZoneRange = 'day';
let loadZoneMode = 'intraday';
let loadZoneAggRange = 'month';
let loadZoneChartLoadInFlight = null;

let loadTabInited = false;
let loadLatestRows = [];
let loadSelectedZone = null;
let loadSelectedSource = null;

// Prices tab (day-ahead)
let priceEuChart = null;
let priceEuRange = '1y';
let priceEuChartLoadInFlight = null;

let priceZoneChart = null;
let priceZoneRange = 'day';
let priceZoneChartLoadInFlight = null;

let priceTabInited = false;
let priceLatestRows = []; // latest avg price per zone (last 24h)
let priceSelectedZone = null;
let priceMapWindow = '24h'; // '24h' | '30d'

// Carbon intensity tab
let carbonTabInited = false;
let carbonRange = 'week';
let carbonChart = null;
let carbonMixChart = null;
let carbonChartLoadInFlight = null;

// Carbon intensity by country
let carbonZoneSelected = null;
let carbonZoneChart = null;
let carbonZoneRange = 'week';
let carbonZoneMode = 'intraday';
let carbonZoneAggRange = 'month';
let carbonZoneLoadInFlight = null;
let carbonCountryDataLoaded = false;

// Price vs renewables scatter tab
let priceGenTabInited = false;
let priceGenRange = 'month';
let priceGenChart = null;
let priceGenLoadInFlight = null;

// Chart builder tab (multi-series, per-series metric)
let chartBuilderTabInited = false;
let chartBuilderChart = null;
let cbComposerMetric = 'renewable'; // metric applied to next country click
let cbComposerGenFilter = null;     // psr_type group key when generation is active (null = total)
let cbRange = 'day'; // 'day' | 'week' | 'month' | '6m' | '1y' | '5y'
let cbAggMode = 'auto'; // 'auto' | 'intraday' | 'daily' | 'weekly'
let cbSelected = []; // [{ id, country, metric, psrFilter, color, visible }]

function fmtMwShort(mw) {
    const n = Number(mw);
    if (!Number.isFinite(n)) return '-';
    const abs = Math.abs(n);
    if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(2)} TW`;
    if (abs >= 1_000) return `${(n / 1_000).toFixed(1)} GW`;
    return `${Math.round(n)} MW`;
}

function fmtEurPerMwh(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return '-';
    return `${n.toFixed(1)} €/MWh`;
}

function setupElectricityMeterTabs() {
    const tabButtons = document.querySelectorAll('#energyMeterPage .em-tab-btn');
    if (!tabButtons.length) return;
    tabButtons.forEach(btn => {
        if (btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            const target = btn.getAttribute('data-em-tab');
            switchElectricityMeterTab(target);
        });
    });
}

function switchElectricityMeterTab(target) {
    saveElecMeterTab(target || 'renewable');
    // Collapse sidebar for chart builder; restore for all other tabs
    if (target !== 'chart-builder') setSidebarCollapsed(false);
    const buttons = document.querySelectorAll('#energyMeterPage .em-tab-btn');
    buttons.forEach(b => {
        b.classList.toggle('active', b.getAttribute('data-em-tab') === target);
    });
    const panes = document.querySelectorAll('#energyMeterPage .em-tab-pane');
    panes.forEach(p => p.classList.remove('active'));
    if (target === 'electricity') {
        document.getElementById('electricityEmTab')?.classList.add('active');
        if (!elecTabInited) {
            elecTabInited = true;
            initElectricityTabControls();
        }
        loadElectricityTabData();
    } else if (target === 'prices') {
        document.getElementById('pricesEmTab')?.classList.add('active');
        if (!priceTabInited) {
            priceTabInited = true;
            initPriceTabControls();
        }
        loadPriceTabData();
    } else if (target === 'demand') {
        document.getElementById('demandEmTab')?.classList.add('active');
        if (!loadTabInited) {
            loadTabInited = true;
            initLoadTabControls();
        }
        loadDemandTabData();
    } else if (target === 'carbon') {
        document.getElementById('carbonEmTab')?.classList.add('active');
        if (!carbonTabInited) {
            carbonTabInited = true;
            initCarbonTabControls();
        }
        loadCarbonTabData();
    } else if (target === 'price-gen') {
        document.getElementById('priceGenEmTab')?.classList.add('active');
        if (!priceGenTabInited) {
            priceGenTabInited = true;
            initPriceGenTabControls();
        }
        loadPriceGenTabData();
    } else if (target === 'flows') {
        document.getElementById('flowsEmTab')?.classList.add('active');
        if (!flowsTabInited) {
            flowsTabInited = true;
            initFlowsTabControls();
        }
        loadFlowsMap(flowsRange);
    } else if (target === 'chart-builder') {
        document.getElementById('chartBuilderEmTab')?.classList.add('active');
        setSidebarCollapsed(true);
        if (!chartBuilderTabInited) {
            chartBuilderTabInited = true;
            initChartBuilderControls();
        }
        loadChartBuilder();
    } else {
        document.getElementById('renewableEmTab')?.classList.add('active');
        // Every other tab above re-fetches when you switch to it. Renewable had
        // no loader here at all, because its fetch lives inline in
        // loadEnergyMeterPage — so its data only ever appeared on a full page
        // load, and clicking back to the tab showed whatever was left on screen.
        // Re-running that function is safe: every listener it attaches is
        // guarded with dataset.bound, and it only delegates back here for
        // non-renewable tabs, so this cannot recurse.
        loadEnergyMeterPage();
    }
}

function initPriceTabControls() {
    const bindEu = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            priceEuRange = range;
            updatePriceEuRangeButtonActive();
            loadPriceEuChart(range);
        });
    };
    bindEu('priceEuRangeDayBtn', 'day');
    bindEu('priceEuRangeWeekBtn', 'week');
    bindEu('priceEuRangeMonthBtn', 'month');
    bindEu('priceEuRange6mBtn', '6m');
    bindEu('priceEuRange1yBtn', '1y');
    bindEu('priceEuRange5yBtn', '5y');

    const bindZone = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            priceZoneRange = range;
            updatePriceZoneRangeButtonActive();
            if (priceSelectedZone) loadPriceZoneChart(priceSelectedZone, range);
        });
    };
    bindZone('priceZoneRangeDayBtn', 'day');
    bindZone('priceZoneRangeWeekBtn', 'week');
    bindZone('priceZoneRangeMonthBtn', 'month');
    bindZone('priceZoneRange6mBtn', '6m');
    bindZone('priceZoneRange1yBtn', '1y');
    bindZone('priceZoneRange5yBtn', '5y');

    const refreshBtn = document.getElementById('priceRefreshBtn');
    if (refreshBtn && !refreshBtn.dataset.bound) {
        refreshBtn.dataset.bound = '1';
        refreshBtn.addEventListener('click', () => loadPriceTabData(true));
    }

    const w24 = document.getElementById('priceMapWindow24hBtn');
    if (w24 && !w24.dataset.bound) {
        w24.dataset.bound = '1';
        w24.addEventListener('click', () => {
            priceMapWindow = '24h';
            updatePriceMapWindowButtons();
            loadPriceTabData();
        });
    }
    const w30 = document.getElementById('priceMapWindow30dBtn');
    if (w30 && !w30.dataset.bound) {
        w30.dataset.bound = '1';
        w30.addEventListener('click', () => {
            priceMapWindow = '30d';
            updatePriceMapWindowButtons();
            loadPriceTabData();
        });
    }
}

function updatePriceMapWindowButtons() {
    document.getElementById('priceMapWindow24hBtn')?.classList.toggle('active', priceMapWindow === '24h');
    document.getElementById('priceMapWindow30dBtn')?.classList.toggle('active', priceMapWindow === '30d');
    const title = document.getElementById('priceMapTitle');
    if (title) title.textContent = `Price map (${priceMapWindow === '30d' ? 'last 30d avg' : 'last 24h avg'})`;
    const label = document.getElementById('priceEuAvgLabel');
    if (label) label.textContent = `EU avg (${priceMapWindow === '30d' ? 'last 30d' : 'last 24h'})`;
}

// ─── Shared aggregate helpers ────────────────────────────────────────────────

function tsPeriodKey(ts, period) {
    const d = new Date(ts);
    if (period === 'day') return d.toISOString().slice(0, 10);
    if (period === 'week') {
        const monday = d.getTime() - ((d.getUTCDay() + 6) % 7) * 86400000;
        return new Date(monday).toISOString().slice(0, 10);
    }
    if (period === 'month') return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
    return String(d.getUTCFullYear());
}

function tsPeriodLabel(key, period) {
    if (period === 'day') return new Date(key).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
    if (period === 'week') return 'W/' + new Date(key).toLocaleDateString(undefined, { day: 'numeric', month: 'short' });
    if (period === 'month') {
        const [yr, mo] = key.split('-');
        return new Date(+yr, +mo - 1, 1).toLocaleDateString(undefined, { month: 'short', year: '2-digit' });
    }
    return key;
}

function aggSincePeriod(period) {
    const d = new Date();
    if (period === 'day') d.setFullYear(d.getFullYear() - 1);
    else if (period === 'week') d.setFullYear(d.getFullYear() - 3);
    else if (period === 'month') d.setFullYear(d.getFullYear() - 10);
    else d.setFullYear(d.getFullYear() - 20);
    return d.toISOString().slice(0, 10);
}

function setChartModePanels(prefix, mode) {
    document.getElementById(`${prefix}IntradayBtns`)?.style.setProperty('display', mode === 'intraday' ? '' : 'none');
    document.getElementById(`${prefix}AggregateBtns`)?.style.setProperty('display', mode === 'aggregate' ? '' : 'none');
    document.getElementById(`${prefix}ModeIntradayBtn`)?.classList.toggle('active', mode === 'intraday');
    document.getElementById(`${prefix}ModeAggBtn`)?.classList.toggle('active', mode === 'aggregate');
}

// ─── EU Generation aggregate mode ────────────────────────────────────────────

function setElecEuMode(mode) {
    elecEuMode = mode;
    setChartModePanels('elecEu', mode);
    updateElecEuAggRangeButtonActive();
    loadElecEuTotalChart(mode === 'aggregate' ? elecEuAggRange : elecEuRange);
}

function updateElecEuAggRangeButtonActive() {
    ['week','month','year'].forEach(r => {
        const key = r.charAt(0).toUpperCase() + r.slice(1);
        document.getElementById(`elecEuAgg${key}Btn`)?.classList.toggle('active', r === elecEuAggRange);
    });
}

// ─── Zone Generation aggregate mode ──────────────────────────────────────────

function setElecZoneMode(mode) {
    elecZoneMode = mode;
    setChartModePanels('elecZone', mode);
    updateElecZoneAggRangeButtonActive();
    if (elecSelectedZone) loadElecZoneTotalChart(elecSelectedZone, mode === 'aggregate' ? elecZoneAggRange : elecZoneRange, elecSelectedSource);
}

function updateElecZoneAggRangeButtonActive() {
    ['week','month','year'].forEach(r => {
        const key = r.charAt(0).toUpperCase() + r.slice(1);
        document.getElementById(`elecZoneAgg${key}Btn`)?.classList.toggle('active', r === elecZoneAggRange);
    });
}

// ─── Zone Demand aggregate mode ───────────────────────────────────────────────

function setLoadZoneMode(mode) {
    loadZoneMode = mode;
    setChartModePanels('loadZone', mode);
    updateLoadZoneAggRangeButtonActive();
    if (loadSelectedZone) loadLoadZoneChart(loadSelectedZone, mode === 'aggregate' ? loadZoneAggRange : loadZoneRange, loadSelectedSource);
}

function updateLoadZoneAggRangeButtonActive() {
    ['day','week','month','year'].forEach(r => {
        const key = r.charAt(0).toUpperCase() + r.slice(1);
        document.getElementById(`loadZoneAgg${key}Btn`)?.classList.toggle('active', r === loadZoneAggRange);
    });
}

// ─── Carbon Zone aggregate mode ───────────────────────────────────────────────

function setCarbonZoneMode(mode) {
    carbonZoneMode = mode;
    setChartModePanels('carbonZone', mode);
    updateCarbonZoneAggRangeButtonActive();
    if (carbonZoneSelected) loadCarbonZoneChart(carbonZoneSelected, mode === 'aggregate' ? carbonZoneAggRange : carbonZoneRange);
}

function updateCarbonZoneAggRangeButtonActive() {
    ['week','month','year'].forEach(r => {
        const key = r.charAt(0).toUpperCase() + r.slice(1);
        document.getElementById(`carbonZoneAgg${key}Btn`)?.classList.toggle('active', r === carbonZoneAggRange);
    });
}

// ─── Gas EU aggregate mode ────────────────────────────────────────────────────

function setGasEuMode(mode) {
    gasEuMode = mode;
    setChartModePanels('gasEu', mode);
    updateGasEuAggRangeButtonActive();
    loadGasEuAggregateChart(mode === 'aggregate' ? gasEuAggRange : gasEuRange);
}

function updateGasEuAggRangeButtonActive() {
    ['week','month','year'].forEach(r => {
        const key = r.charAt(0).toUpperCase() + r.slice(1);
        document.getElementById(`gasEuAgg${key}Btn`)?.classList.toggle('active', r === gasEuAggRange);
    });
}

// ─────────────────────────────────────────────────────────────────────────────

function initElectricityTabControls() {
    const bindEu = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            elecEuRange = range;
            saveElecMeterRanges();
            updateElecEuRangeButtonActive();
            loadElecEuTotalChart(range);
        });
    };
    bindEu('elecEuRangeDayBtn', 'day');
    bindEu('elecEuRangeWeekBtn', 'week');
    bindEu('elecEuRangeMonthBtn', 'month');
    bindEu('elecEuRange6mBtn', '6m');
    bindEu('elecEuRange1yBtn', '1y');
    bindEu('elecEuRange5yBtn', '5y');

    const bindEuAgg = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => { elecEuAggRange = range; updateElecEuAggRangeButtonActive(); loadElecEuTotalChart(range); });
    };
    bindEuAgg('elecEuAggWeekBtn', 'week');
    bindEuAgg('elecEuAggMonthBtn', 'month');
    bindEuAgg('elecEuAggYearBtn', 'year');
    const elecEuModeI = document.getElementById('elecEuModeIntradayBtn');
    if (elecEuModeI && !elecEuModeI.dataset.bound) { elecEuModeI.dataset.bound='1'; elecEuModeI.addEventListener('click', () => setElecEuMode('intraday')); }
    const elecEuModeA = document.getElementById('elecEuModeAggBtn');
    if (elecEuModeA && !elecEuModeA.dataset.bound) { elecEuModeA.dataset.bound='1'; elecEuModeA.addEventListener('click', () => setElecEuMode('aggregate')); }
    setChartModePanels('elecEu', elecEuMode);

    const bindZone = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            elecZoneRange = range;
            saveElecMeterRanges();
            updateElecZoneRangeButtonActive();
            if (elecSelectedZone) loadElecZoneTotalChart(elecSelectedZone, range, elecSelectedSource);
        });
    };
    bindZone('elecZoneRangeDayBtn', 'day');
    bindZone('elecZoneRangeWeekBtn', 'week');
    bindZone('elecZoneRangeMonthBtn', 'month');
    bindZone('elecZoneRange6mBtn', '6m');
    bindZone('elecZoneRange1yBtn', '1y');
    bindZone('elecZoneRange5yBtn', '5y');

    const bindZoneAgg = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => { elecZoneAggRange = range; updateElecZoneAggRangeButtonActive(); if (elecSelectedZone) loadElecZoneTotalChart(elecSelectedZone, range, elecSelectedSource); });
    };
    bindZoneAgg('elecZoneAggWeekBtn', 'week');
    bindZoneAgg('elecZoneAggMonthBtn', 'month');
    bindZoneAgg('elecZoneAggYearBtn', 'year');
    const elecZoneModeI = document.getElementById('elecZoneModeIntradayBtn');
    if (elecZoneModeI && !elecZoneModeI.dataset.bound) { elecZoneModeI.dataset.bound='1'; elecZoneModeI.addEventListener('click', () => setElecZoneMode('intraday')); }
    const elecZoneModeA = document.getElementById('elecZoneModeAggBtn');
    if (elecZoneModeA && !elecZoneModeA.dataset.bound) { elecZoneModeA.dataset.bound='1'; elecZoneModeA.addEventListener('click', () => setElecZoneMode('aggregate')); }
    setChartModePanels('elecZone', elecZoneMode);

    const refreshBtn = document.getElementById('elecRefreshBtn');
    if (refreshBtn && !refreshBtn.dataset.bound) {
        refreshBtn.dataset.bound = '1';
        refreshBtn.addEventListener('click', () => loadElectricityTabData(true));
    }
}

function setLoadEuMode(mode) {
    loadEuMode = mode;
    document.getElementById('loadEuIntradayBtns')?.style.setProperty('display', mode === 'intraday' ? '' : 'none');
    document.getElementById('loadEuAggregateBtns')?.style.setProperty('display', mode === 'aggregate' ? '' : 'none');
    document.getElementById('loadEuModeIntradayBtn')?.classList.toggle('active', mode === 'intraday');
    document.getElementById('loadEuModeAggBtn')?.classList.toggle('active', mode === 'aggregate');
    if (mode === 'aggregate') updateLoadEuAggRangeButtonActive();
    else updateLoadEuRangeButtonActive();
    loadLoadEuChart(mode === 'aggregate' ? loadEuAggRange : loadEuRange);
}

function updateLoadEuAggRangeButtonActive() {
    ['day','week','month','year'].forEach(r => {
        const key = r.charAt(0).toUpperCase() + r.slice(1);
        document.getElementById(`loadEuAgg${key}Btn`)?.classList.toggle('active', r === loadEuAggRange);
    });
}

function initLoadTabControls() {
    const bindEu = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            loadEuRange = range;
            updateLoadEuRangeButtonActive();
            loadLoadEuChart(range);
        });
    };
    bindEu('loadEuRangeDayBtn', 'day');
    bindEu('loadEuRangeWeekBtn', 'week');
    bindEu('loadEuRangeMonthBtn', 'month');
    bindEu('loadEuRange6mBtn', '6m');
    bindEu('loadEuRange1yBtn', '1y');
    bindEu('loadEuRange5yBtn', '5y');

    const bindAgg = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            loadEuAggRange = range;
            updateLoadEuAggRangeButtonActive();
            loadLoadEuChart(range);
        });
    };
    bindAgg('loadEuAggDayBtn', 'day');
    bindAgg('loadEuAggWeekBtn', 'week');
    bindAgg('loadEuAggMonthBtn', 'month');
    bindAgg('loadEuAggYearBtn', 'year');

    const modeIntraday = document.getElementById('loadEuModeIntradayBtn');
    if (modeIntraday && !modeIntraday.dataset.bound) {
        modeIntraday.dataset.bound = '1';
        modeIntraday.addEventListener('click', () => setLoadEuMode('intraday'));
    }
    const modeAgg = document.getElementById('loadEuModeAggBtn');
    if (modeAgg && !modeAgg.dataset.bound) {
        modeAgg.dataset.bound = '1';
        modeAgg.addEventListener('click', () => setLoadEuMode('aggregate'));
    }
    setLoadEuMode(loadEuMode);

    const bindZone = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            loadZoneRange = range;
            updateLoadZoneRangeButtonActive();
            if (loadSelectedZone) loadLoadZoneChart(loadSelectedZone, range, loadSelectedSource);
        });
    };
    bindZone('loadZoneRangeDayBtn', 'day');
    bindZone('loadZoneRangeWeekBtn', 'week');
    bindZone('loadZoneRangeMonthBtn', 'month');
    bindZone('loadZoneRange6mBtn', '6m');
    bindZone('loadZoneRange1yBtn', '1y');
    bindZone('loadZoneRange5yBtn', '5y');

    const bindZoneAgg = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => { loadZoneAggRange = range; updateLoadZoneAggRangeButtonActive(); if (loadSelectedZone) loadLoadZoneChart(loadSelectedZone, range, loadSelectedSource); });
    };
    bindZoneAgg('loadZoneAggDayBtn', 'day');
    bindZoneAgg('loadZoneAggWeekBtn', 'week');
    bindZoneAgg('loadZoneAggMonthBtn', 'month');
    bindZoneAgg('loadZoneAggYearBtn', 'year');
    const loadZoneModeI = document.getElementById('loadZoneModeIntradayBtn');
    if (loadZoneModeI && !loadZoneModeI.dataset.bound) { loadZoneModeI.dataset.bound='1'; loadZoneModeI.addEventListener('click', () => setLoadZoneMode('intraday')); }
    const loadZoneModeA = document.getElementById('loadZoneModeAggBtn');
    if (loadZoneModeA && !loadZoneModeA.dataset.bound) { loadZoneModeA.dataset.bound='1'; loadZoneModeA.addEventListener('click', () => setLoadZoneMode('aggregate')); }
    setChartModePanels('loadZone', loadZoneMode);

    const refreshBtn = document.getElementById('loadRefreshBtn');
    if (refreshBtn && !refreshBtn.dataset.bound) {
        refreshBtn.dataset.bound = '1';
        refreshBtn.addEventListener('click', () => loadDemandTabData(true));
    }
}

function updateElecEuRangeButtonActive() {
    const map = {
        day: 'elecEuRangeDayBtn',
        week: 'elecEuRangeWeekBtn',
        month: 'elecEuRangeMonthBtn',
        '6m': 'elecEuRange6mBtn',
        '1y': 'elecEuRange1yBtn',
        '5y': 'elecEuRange5yBtn',
    };
    Object.entries(map).forEach(([range, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        if (elecEuRange === range) el.classList.add('active');
        else el.classList.remove('active');
    });
}

function updateElecZoneRangeButtonActive() {
    const map = {
        day: 'elecZoneRangeDayBtn',
        week: 'elecZoneRangeWeekBtn',
        month: 'elecZoneRangeMonthBtn',
        '6m': 'elecZoneRange6mBtn',
        '1y': 'elecZoneRange1yBtn',
        '5y': 'elecZoneRange5yBtn',
    };
    Object.entries(map).forEach(([range, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        if (elecZoneRange === range) el.classList.add('active');
        else el.classList.remove('active');
    });
}

function updateLoadEuRangeButtonActive() {
    const map = {
        day: 'loadEuRangeDayBtn',
        week: 'loadEuRangeWeekBtn',
        month: 'loadEuRangeMonthBtn',
        '6m': 'loadEuRange6mBtn',
        '1y': 'loadEuRange1yBtn',
        '5y': 'loadEuRange5yBtn',
    };
    Object.entries(map).forEach(([range, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.toggle('active', loadEuRange === range);
    });
}

function updateLoadZoneRangeButtonActive() {
    const map = {
        day: 'loadZoneRangeDayBtn',
        week: 'loadZoneRangeWeekBtn',
        month: 'loadZoneRangeMonthBtn',
        '6m': 'loadZoneRange6mBtn',
        '1y': 'loadZoneRange1yBtn',
        '5y': 'loadZoneRange5yBtn',
    };
    Object.entries(map).forEach(([range, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.toggle('active', loadZoneRange === range);
    });
}

async function loadDemandTabData(forceRefresh = false) {
    const statusEl = document.getElementById('loadMeterStatus');
    const tbody = document.getElementById('loadMeterTableBody');
    const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg || ''; };
    if (!tbody) return;

    try {
        if (!supabase) throw new Error('Supabase client not initialized.');

        setStatus('Fetching latest demand snapshot…');
        tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; color: var(--text-secondary); padding: 24px;">Loading...</td></tr>';

        // Load data updates can lag by a day or two depending on ingestion/backfill cadence.
        // Use a wider window and then dedupe to "latest per zone".
        const since = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString();
        const { data, error } = await supabase
            .from('electricity_load_snapshots')
            .select('id, zone_id, country_code, ts, load_mw, source')
            .eq('source', 'entsoe')
            .gte('ts', since)
            .order('ts', { ascending: false })
            .limit(2000);
        if (error) throw new Error(error.message);

        const rows = Array.isArray(data) ? data : [];
        const latestByZone = dedupeLatestByZone(rows);
        latestByZone.sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime());
        loadLatestRows = latestByZone;

        if (!latestByZone.length) {
            tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; color: var(--text-secondary); padding: 24px;">No demand data yet. Run ENTSO‑E load ingestion.</td></tr>';
            setStatus('No demand snapshots found in the last 14 days.');
            document.getElementById('loadLastUpdated').textContent = '-';
            document.getElementById('loadZones').textContent = '0';
            document.getElementById('loadEuTotal').textContent = '-';
            document.getElementById('loadAvgZone').textContent = '-';
            return;
        }

        const newest = latestByZone.reduce((acc, r) => {
            const t = new Date(r.ts).getTime();
            return Number.isFinite(t) ? Math.max(acc, t) : acc;
        }, 0);
        const loads = latestByZone.map(r => Number(r.load_mw)).filter(Number.isFinite);
        const avgZoneMw = loads.length ? loads.reduce((a, b) => a + b, 0) / loads.length : null;
        const euTotal = loads.length ? loads.reduce((a, b) => a + b, 0) : null;

        document.getElementById('loadLastUpdated').textContent = newest ? new Date(newest).toLocaleString() : '-';
        document.getElementById('loadZones').textContent = String(latestByZone.length);
        document.getElementById('loadAvgZone').textContent = fmtMwShort(avgZoneMw);
        document.getElementById('loadEuTotal').textContent = fmtMwShort(euTotal);

        tbody.innerHTML = latestByZone.map(r => {
            const zone = r.zone_id || r.country_code || '-';
            const tsStr = r.ts ? new Date(r.ts).toLocaleString() : '-';
            const mw = Number(r.load_mw);
            return `
                <tr class="load-row" data-zone="${escapeHtml(String(zone))}" data-source="${escapeHtml(String(r.source || '-'))}">
                    <td>${escapeHtml(String(zone))}</td>
                    <td>${escapeHtml(tsStr)}</td>
                    <td>${escapeHtml(Number.isFinite(mw) ? Math.round(mw).toLocaleString() : '-')}</td>
                    <td>${escapeHtml(String(r.source || '-'))}</td>
                </tr>
            `;
        }).join('');

        tbody.querySelectorAll('tr.load-row').forEach(tr => {
            tr.addEventListener('click', () => {
                const z = tr.getAttribute('data-zone');
                const s = tr.getAttribute('data-source');
                if (!z) return;
                loadSelectedZone = z;
                loadSelectedSource = s || null;
                loadLoadZoneChart(z, loadZoneRange, loadSelectedSource);
            });
        });

        // Default selection mirrors renewable/electricity selection if possible
        if (!loadSelectedZone) {
            const fr = latestByZone.find(r => (r.zone_id || r.country_code) === 'FR');
            const pick = (energySelectedZone && latestByZone.find(r => (r.zone_id || r.country_code) === energySelectedZone))
                || (elecSelectedZone && latestByZone.find(r => (r.zone_id || r.country_code) === elecSelectedZone))
                || fr
                || latestByZone[0];
            if (pick) {
                loadSelectedZone = pick.zone_id || pick.country_code;
                loadSelectedSource = pick.source || 'entsoe';
            }
        }

        renderLoadMap(latestByZone);
        updateLoadEuRangeButtonActive();
        updateLoadZoneRangeButtonActive();
        await loadLoadEuChart(loadEuRange);
        if (loadSelectedZone) await loadLoadZoneChart(loadSelectedZone, loadZoneRange, loadSelectedSource);

        setStatus(`Loaded ${latestByZone.length} zones.`);
    } catch (err) {
        console.error('Demand tab load failed:', err);
        tbody.innerHTML = `<tr><td colspan="4" style="text-align:center; color: var(--error-color); padding: 24px;">Failed to load: ${escapeHtml(err.message || String(err))}</td></tr>`;
        setStatus('Failed to load.');
    }
}

function renderLoadMap(latestRows) {
    const container = document.getElementById('loadMapContainer');
    if (!container) return;
    const rows = (latestRows || []).filter(r => (r.zone_id || r.country_code) && Number.isFinite(Number(r.load_mw)));
    if (!rows.length) {
        container.innerHTML = '<div class="chart-loading">No ENTSO‑E load data yet.</div>';
        return;
    }
    renderLoadGeoMap(container, rows).catch((e) => {
        console.warn('Load geo map render failed, falling back to tile grid:', e);
        renderLoadTileGrid(container, rows);
            mapFallbackNote(container, e);
    });
}

function renderLoadTileGrid(container, rows) {
    const values = rows.map(r => Number(r.load_mw)).filter(Number.isFinite);
    const max = values.length ? Math.max(...values) : 1;
    const legend = `
        <div class="energy-map-legend">
            <span>Low demand</span>
            <div class="energy-map-legend-bar" style="background: linear-gradient(90deg, rgba(219,234,254,1), rgba(2,132,199,1));"></div>
            <span>High demand</span>
        </div>
    `;
    const tiles = rows
        .sort((a, b) => String(a.zone_id || a.country_code).localeCompare(String(b.zone_id || b.country_code)))
        .map(r => {
            const zone = String(r.zone_id || r.country_code);
            const mw = Number(r.load_mw);
            const t = Number.isFinite(mw) && max > 0 ? Math.max(0, Math.min(1, mw / max)) : 0;
            const bg = Number.isFinite(mw) ? gasBlueScale(t) : 'rgba(148,163,184,0.25)';
            const color = Number.isFinite(mw) ? gasBlueTextForBg(t) : 'rgba(15,23,42,0.8)';
            const isActive = String(loadSelectedZone || '').toUpperCase() === zone.toUpperCase();
            const val = Number.isFinite(mw) ? fmtMwShort(mw) : '—';
            return `
                <div class="energy-map-tile ${isActive ? 'active' : ''}" data-zone="${escapeHtml(zone)}" style="background:${bg}; color:${color}">
                    <div class="energy-map-tile-code">${escapeHtml(zone)}</div>
                    <div class="energy-map-tile-value">${escapeHtml(val)}</div>
                </div>
            `;
        })
        .join('');
    container.innerHTML = `${legend}<div class="energy-map-grid">${tiles}</div>`;
    container.querySelectorAll('.energy-map-tile').forEach(el => {
        el.addEventListener('click', () => {
            const z = el.getAttribute('data-zone');
            if (!z) return;
            loadSelectedZone = z;
            loadSelectedSource = 'entsoe';
            updateLoadZoneRangeButtonActive();
            loadLoadZoneChart(z, loadZoneRange, 'entsoe');
            container.querySelectorAll('.energy-map-tile').forEach(t => t.classList.remove('active'));
            el.classList.add('active');
        });
    });
}

function aggregateLoadMw(rows) {
    const byZone = {};
    for (const r of rows) {
        const z = String(r.zone_id || r.country_code || '').toUpperCase();
        const mw = Number(r.load_mw);
        if (!z || !Number.isFinite(mw)) continue;
        byZone[z] = mw;
    }
    const byCountry = {};
    for (const r of rows) {
        const iso2 = zoneToCountryIso2(r.zone_id || r.country_code);
        const mw = Number(r.load_mw);
        if (!Number.isFinite(mw)) continue;
        const prev = byCountry[iso2] || { sum: 0, latestTs: null };
        prev.sum += mw;
        const t = r.ts ? new Date(r.ts).getTime() : NaN;
        if (Number.isFinite(t) && (!prev.latestTs || t > prev.latestTs)) prev.latestTs = t;
        byCountry[iso2] = prev;
    }
    const byCountryOut = {};
    Object.entries(byCountry).forEach(([iso2, v]) => {
        byCountryOut[iso2] = { mw: v.sum, latestTs: v.latestTs ? new Date(v.latestTs).toISOString() : null };
    });
    return { byZone, byCountry: byCountryOut };
}

async function renderLoadGeoMap(container, rows) {
    const [countryGeo, zoneGeo] = await Promise.all([
        fetchEuropeCountriesGeoJsonOnce(),
        // .catch here as well as inside: a cosmetic overlay must never be
        // able to reject the Promise.all and take the base map down with it.
        fetchEntsoeZonesGeoJsonOnce().catch(() => null),
    ]);
    const hasZoneOverlay = Array.isArray(zoneGeo?.features) && zoneGeo.features.length > 0;

    const { byZone, byCountry } = aggregateLoadMw(rows);
    const countryMax = Math.max(0, ...Object.values(byCountry).map(v => Number(v.mw)).filter(Number.isFinite));
    const zoneMax = Math.max(0, ...Object.values(byZone).map(v => Number(v)).filter(Number.isFinite));

    const width = 1400;
    const height = 860;
    const padding = 10;
    const bounds = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };

    const selectedZone = String(loadSelectedZone || '').toUpperCase();
    const selectedIso2 = zoneToCountryIso2(selectedZone);
    const selectedLabel = selectedZone ? selectedZone : '—';
    const selectedMw =
        selectedZone && Object.prototype.hasOwnProperty.call(byZone, selectedZone)
            ? byZone[selectedZone]
            : (selectedIso2 && byCountry[iso2GeoToDataKey(selectedIso2)]?.mw);

    container.innerHTML = `
        <div class="energy-map-shell">
            <div class="energy-map-top">
                <div class="energy-map-top-left">
                    <div class="energy-map-title">Electricity demand map</div>
                    <div class="energy-map-subtitle">Countries + bidding zones for DK/SE/NO (click to chart)</div>
                </div>
                <div class="energy-map-top-right">
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Selected</div>
                        <div class="energy-map-chip-value">${escapeHtml(selectedLabel)}</div>
                    </div>
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Demand</div>
                        <div class="energy-map-chip-value">${Number.isFinite(selectedMw) ? escapeHtml(fmtMwShort(selectedMw)) : '—'}</div>
                    </div>
                </div>
            </div>
            <div class="energy-map-legend energy-map-legend--premium">
                <span>Low</span>
                <div class="energy-map-legend-bar" style="background: linear-gradient(90deg, rgba(219,234,254,1), rgba(2,132,199,1));"></div>
                <span>High</span>
            </div>
            <div class="energy-map-stage">
                <svg class="energy-geo-map" viewBox="0 0 ${width} ${height}" role="img" aria-label="Electricity demand map"></svg>
            </div>
        </div>
    `;

    const svg = container.querySelector('svg.energy-geo-map');
    if (!svg) return;

    let tooltip = document.querySelector('.energy-map-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.className = 'energy-map-tooltip';
        tooltip.style.display = 'none';
        document.body.appendChild(tooltip);
    }

    const countryFeatures = Array.isArray(countryGeo?.features) ? countryGeo.features : [];
    for (const f of countryFeatures) {
        const iso2 = String(f?.properties?.ISO2 || '').toUpperCase();
        if (!iso2) continue;
        if (iso2 === 'RU' || iso2 === 'BY') continue;
        // Only cede these to the bidding-zone overlay if that overlay actually
        // loaded; otherwise DK/SE/NO would be drawn by nobody and vanish.
        if (hasZoneOverlay && (iso2 === 'DK' || iso2 === 'SE' || iso2 === 'NO')) continue;

        const dataKey = iso2GeoToDataKey(iso2);
        const mw = byCountry[dataKey]?.mw;
        const t = Number.isFinite(mw) && countryMax > 0 ? Math.max(0, Math.min(1, mw / countryMax)) : null;
        const fill = t == null ? NO_DATA_FILL : gasBlueScale(t);

        const geom = f.geometry;
        if (!geom) continue;
        const type = geom.type;
        const coords = geom.coordinates;

        const paths = [];
        if (type === 'Polygon') {
            paths.push(polygonToPath(coords[0], width, height, bounds, padding));
        } else if (type === 'MultiPolygon') {
            for (const poly of coords) if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
        } else continue;

        const d = paths.join(' ');
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', d);
        path.setAttribute('fill', fill);
        path.setAttribute('data-iso2', iso2);
        path.style.cursor = 'pointer';
        if (selectedIso2 && iso2GeoMatchesSelection(iso2, selectedIso2)) path.classList.add('is-selected');

        path.addEventListener('mouseenter', () => {
            const v = byCountry[dataKey]?.mw;
            tooltip.style.display = 'block';
            tooltip.textContent = `${iso2} — ${Number.isFinite(v) ? fmtMwShort(v) : '—'}`;
        });
        path.addEventListener('mousemove', (e) => {
            tooltip.style.left = `${e.clientX}px`;
            tooltip.style.top = `${e.clientY}px`;
        });
        path.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
        path.addEventListener('click', () => {
            const picked = pickZoneForCountry(rows, iso2);
            if (!picked) return;
            loadSelectedZone = String(picked.zone_id || picked.country_code);
            loadSelectedSource = 'entsoe';
            updateLoadZoneRangeButtonActive();
            loadLoadZoneChart(loadSelectedZone, loadZoneRange, 'entsoe');
            renderLoadGeoMap(container, rows).catch(() => {});
        });

        svg.appendChild(path);
    }

    // Overlay DK/SE/NO bidding zones (same geometry as generation map)
    const europeBbox = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };
    const zoneFeaturesAll = Array.isArray(zoneGeo?.features) ? zoneGeo.features : [];
    const overlayZones = new Set(['DK1', 'DK2', 'SE1', 'SE2', 'SE3', 'SE4', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5']);
    for (const f of zoneFeaturesAll) {
        const zoneId = normalizeZoneNameToId(f?.properties?.zoneName);
        if (!zoneId || !overlayZones.has(zoneId)) continue;
        const geom = filterGeometryToBbox(f?.geometry, europeBbox);
        if (!geom) continue;

        const mw = byZone[zoneId];
        const t = Number.isFinite(mw) && zoneMax > 0 ? Math.max(0, Math.min(1, mw / zoneMax)) : null;
        const fill = t == null ? NO_DATA_FILL : gasBlueScale(t);

        const type = geom.type;
        const coords = geom.coordinates;
        const paths = [];
        if (type === 'Polygon') {
            paths.push(polygonToPath(coords[0], width, height, bounds, padding));
        } else if (type === 'MultiPolygon') {
            for (const poly of coords) if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
        } else continue;

        const d = paths.join(' ');
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', d);
        path.setAttribute('fill', fill);
        path.setAttribute('data-zone', zoneId);
        path.style.cursor = 'pointer';
        path.classList.add('bz-overlay');
        if (selectedZone && zoneId === selectedZone) path.classList.add('is-selected');

        path.addEventListener('mouseenter', () => {
            const v = byZone[zoneId];
            tooltip.style.display = 'block';
            tooltip.textContent = `${zoneId} — ${Number.isFinite(v) ? fmtMwShort(v) : '—'}`;
        });
        path.addEventListener('mousemove', (e) => {
            tooltip.style.left = `${e.clientX}px`;
            tooltip.style.top = `${e.clientY}px`;
        });
        path.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
        path.addEventListener('click', () => {
            loadSelectedZone = zoneId;
            loadSelectedSource = 'entsoe';
            updateLoadZoneRangeButtonActive();
            loadLoadZoneChart(loadSelectedZone, loadZoneRange, 'entsoe');
            renderLoadGeoMap(container, rows).catch(() => {});
        });

        svg.appendChild(path);
    }
}

async function loadLoadEuChart(range) {
    if (loadEuChartLoadInFlight) return await loadEuChartLoadInFlight;
    loadEuChartLoadInFlight = (async () => {
        const statusEl = document.getElementById('loadEuStatus');
        const titleEl = document.getElementById('loadEuChartTitle');
        const canvas = document.getElementById('loadEuChart');
        if (!canvas) return;
        const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg || ''; };

        try {
            if (!supabase) throw new Error('Supabase client not initialized.');

            if (loadEuMode === 'aggregate') {
                await loadLoadEuChartAggregate(range, canvas, titleEl, setStatus);
                return;
            }

            const since = euRangeToSinceIso(range);
            const useRaw = range === 'day' || range === 'week';
            const useWeekly = range === '5y';
            const fmtVal = useRaw ? fmtMwShort : fmtGWh;
            const unit = useRaw ? 'MW' : 'GWh';

            setStatus(`Loading EU demand (${range})…`);
            if (titleEl) titleEl.textContent = `EU — Total electricity demand (${unit})`;

            let points;
            if (useRaw) {
                // electricity_eu_load_15m_mv aggregates per zone per hour before
                // summing, which is what stops zones on different reporting
                // frequencies (hourly vs 15-min vs 30-min) from producing spikes
                // at timestamps where only a subset has reported.
                //
                // This used to query electricity_load_snapshots for a zone_id of
                // 'EU' first and treat the MV as a fallback, but no ingest has
                // ever written an 'EU' zone row — the table holds per-zone rows
                // only — so that path always returned empty and cost a wasted
                // paged query. The MV is the real source.
                const mvRows = await gasFetchAllPaged(() =>
                    supabase.from('electricity_eu_load_15m_mv')
                        .select('ts, load_mw')
                        .gte('ts', since)
                        .order('ts', { ascending: true })
                , 1000, 100_000);
                // EU demand is always > 150 GW — filter near-zero values which are reporting gaps
                const EU_MIN_MW = 150_000;
                points = mvRows
                    .filter(r => r.ts && Number(r.load_mw) > EU_MIN_MW)
                    .map(r => ({ ts: r.ts, y: Number(r.load_mw) }));
            } else {
                // Online behaviour: use precomputed energy (MWh) tables and display as GWh.
                const table = useWeekly ? 'electricity_eu_load_weekly_mwh' : 'electricity_eu_load_daily_mwh';
                const { data, error } = await supabase
                    .from(table)
                    .select('ts, consumption_mwh')
                    .gte('ts', since)
                    .order('ts', { ascending: false })
                    .limit(useWeekly ? 400 : 900);
                if (error) throw new Error(error.message);
                points = (Array.isArray(data) ? data : []).reverse()
                    .filter(r => r.ts && Number.isFinite(Number(r.consumption_mwh)))
                    .map(r => ({ ts: r.ts, y: Number(r.consumption_mwh) }));
            }

            const labels = points.map(p => {
                const d = new Date(p.ts);
                if (Number.isNaN(d.getTime())) return String(p.ts);
                return useRaw ? d.toLocaleString() : d.toLocaleDateString();
            });
            const series = points.map(p => p.y);

            const ctx = canvas.getContext('2d');
            if (!ctx) return;
            const existing = Chart.getChart(canvas);
            if (existing) existing.destroy();
            if (loadEuChart) { try { loadEuChart.destroy(); } catch (_) {} loadEuChart = null; }

            loadEuChart = new Chart(ctx, {
                type: 'line',
                data: {
                    datasets: [{
                        label: `EU total demand (${unit})`,
                        data: series,
                        borderColor: '#0ea5e9',
                        backgroundColor: 'rgba(14, 165, 233, 0.12)',
                        fill: true,
                        tension: 0.25,
                        pointRadius: series.length <= 2 ? 3 : 0,
                        borderWidth: 2,
                    }],
                    labels,
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    parsing: true,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { display: true, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } } },
                    scales: {
                        x: { type: 'category', ticks: { maxRotation: 0 }, grid: { display: false } },
                        y: { beginAtZero: true, ticks: { callback: (v) => fmtVal(Number(v)) } },
                    },
                },
            });
        } catch (err) {
            console.error('EU load chart failed:', err);
            setStatus(`Failed: ${err.message || String(err)}`);
        }
    })();
    try { return await loadEuChartLoadInFlight; }
    finally { loadEuChartLoadInFlight = null; }
}

async function loadLoadEuChartAggregate(period, canvas, titleEl, setStatus) {
    setStatus(`Loading EU demand aggregate (${period})…`);
    // Fetch daily data — we'll group in JS for week/month/year
    const since = (() => {
        const d = new Date();
        if (period === 'day') d.setFullYear(d.getFullYear() - 1);
        else if (period === 'week') d.setFullYear(d.getFullYear() - 3);
        else d.setFullYear(d.getFullYear() - 10);
        return d.toISOString().slice(0, 10);
    })();

    const { data, error } = await supabase
        .from('electricity_eu_load_daily_mwh')
        .select('ts, consumption_mwh')
        .gte('ts', since)
        .order('ts', { ascending: true })
        .limit(4000);
    if (error) throw new Error(error.message);

    const rows = (Array.isArray(data) ? data : []).filter(r => r.ts && Number.isFinite(Number(r.consumption_mwh)));

    // Group rows by period key
    const grouped = new Map();
    for (const r of rows) {
        const d = new Date(r.ts);
        let key;
        if (period === 'day') {
            key = r.ts.slice(0, 10);
        } else if (period === 'week') {
            // ISO week key: YYYY-Www
            const jan4 = new Date(Date.UTC(d.getUTCFullYear(), 0, 4));
            const w = Math.ceil(((d - jan4) / 86400000 + jan4.getUTCDay() + 1) / 7);
            key = `${d.getUTCFullYear()}-W${String(w).padStart(2, '0')}`;
        } else if (period === 'month') {
            key = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
        } else {
            key = String(d.getUTCFullYear());
        }
        const prev = grouped.get(key) || 0;
        grouped.set(key, prev + Number(r.consumption_mwh));
    }

    const keys = [...grouped.keys()].sort();
    const labels = keys.map(k => {
        if (period === 'day') return new Date(k).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
        if (period === 'week') return k;
        if (period === 'month') {
            const [yr, mo] = k.split('-');
            return new Date(Number(yr), Number(mo) - 1, 1).toLocaleDateString(undefined, { month: 'short', year: '2-digit' });
        }
        return k;
    });
    const values = keys.map(k => grouped.get(k)); // MWh, fmtGWh handles display

    const unit = 'GWh';
    if (titleEl) titleEl.textContent = `EU — Total electricity demand per ${period} (${unit})`;
    setStatus('');

    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();
    if (loadEuChart) { try { loadEuChart.destroy(); } catch (_) {} loadEuChart = null; }

    loadEuChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: `EU demand per ${period} (${unit})`,
                data: values,
                backgroundColor: 'rgba(14,165,233,0.7)',
                borderColor: '#0ea5e9',
                borderWidth: 1,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { display: true, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } } },
            scales: {
                x: { type: 'category', ticks: { maxRotation: 45 }, grid: { display: false } },
                y: { beginAtZero: false, ticks: { callback: v => fmtGWh(Number(v)) } },
            },
        },
    });
}

async function loadLoadZoneChart(zone, range, source = null) {
    if (loadZoneChartLoadInFlight) return await loadZoneChartLoadInFlight;
    loadZoneChartLoadInFlight = (async () => {
        const statusEl = document.getElementById('loadZoneStatus');
        const titleEl = document.getElementById('loadZoneChartTitle');
        const canvas = document.getElementById('loadZoneChart');
        if (!canvas) return;
        const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg || ''; };

        try {
            if (!supabase) throw new Error('Supabase client not initialized.');

            if (loadZoneMode === 'aggregate') {
                await loadLoadZoneChartAggregate(zone, range, canvas, titleEl, setStatus);
                return;
            }

            const since = rangeToSinceIso(range);
            const useWeekly = range === '5y';
            const useDaily = range === '6m' || range === '1y';
            const table = useWeekly ? 'electricity_load_weekly_mwh' : useDaily ? 'electricity_load_daily_mwh' : 'electricity_load_snapshots';
            const valueCol = (useWeekly || useDaily) ? 'consumption_mwh' : 'load_mw';
            const fmtVal = (useWeekly || useDaily) ? fmtGWh : fmtMwShort;
            const unit = (useWeekly || useDaily) ? 'GWh' : 'MW';
            const maxPoints = useWeekly ? 400 : useDaily ? 900 : 2000;

            setStatus(`Loading ${zone} demand (${range})…`);
            if (titleEl) titleEl.textContent = `${zone} — Electricity demand (${unit})${source ? ` [${source}]` : ''}`;

            let q = supabase
                .from(table)
                .select(`ts, ${valueCol}`)
                .eq('zone_id', zone)
                .gte('ts', since)
                .order('ts', { ascending: false })
                .limit(maxPoints);
            if (source && !useWeekly && !useDaily) q = q.eq('source', source);

            const { data, error } = await q;
            if (error) throw new Error(error.message);
            const rows = (Array.isArray(data) ? data : []).reverse();
            const points = rows
                .filter(r => r.ts && Number.isFinite(Number(r[valueCol])))
                .map(r => ({ ts: r.ts, y: Number(r[valueCol]) }));

            const labels = points.map(p => {
                const d = new Date(p.ts);
                if (Number.isNaN(d.getTime())) return String(p.ts);
                if (useWeekly || useDaily) return d.toLocaleDateString();
                return d.toLocaleString();
            });
            const series = points.map(p => p.y);

            const ctx = canvas.getContext('2d');
            if (!ctx) return;
            const existing = Chart.getChart(canvas);
            if (existing) existing.destroy();
            if (loadZoneChart) { try { loadZoneChart.destroy(); } catch (_) {} loadZoneChart = null; }

            loadZoneChart = new Chart(ctx, {
                type: 'line',
                data: {
                    datasets: [{
                        label: `Demand (${unit})`,
                        data: series,
                        borderColor: '#0284c7',
                        backgroundColor: 'rgba(2, 132, 199, 0.12)',
                        fill: true,
                        tension: 0.25,
                        pointRadius: series.length <= 2 ? 3 : 0,
                        borderWidth: 2,
                    }],
                    labels,
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    parsing: true,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { display: true, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } } },
                    scales: {
                        x: { type: 'category', ticks: { maxRotation: 0 }, grid: { display: false } },
                        y: { beginAtZero: true, ticks: { callback: (v) => fmtVal(Number(v)) } },
                    },
                },
            });
        } catch (err) {
            console.error('Zone load chart failed:', err);
            setStatus(`Failed: ${err.message || String(err)}`);
        }
    })();
    try { return await loadZoneChartLoadInFlight; }
    finally { loadZoneChartLoadInFlight = null; }
}

async function loadLoadZoneChartAggregate(zone, period, canvas, titleEl, setStatus) {
    setStatus(`Loading ${zone} demand aggregate (${period})…`);
    const since = aggSincePeriod(period);
    const { data, error } = await supabase.from('electricity_load_daily_mwh')
        .select('ts, consumption_mwh').eq('zone_id', zone).gte('ts', since)
        .order('ts', { ascending: true }).limit(4000);
    if (error) throw new Error(error.message);
    const rows = (Array.isArray(data) ? data : []).filter(r => r.ts && Number.isFinite(Number(r.consumption_mwh)));

    const grouped = new Map();
    for (const r of rows) {
        const key = tsPeriodKey(r.ts, period);
        grouped.set(key, (grouped.get(key) || 0) + Number(r.consumption_mwh));
    }
    const keys = [...grouped.keys()].sort();
    const labels = keys.map(k => tsPeriodLabel(k, period));
    const values = keys.map(k => grouped.get(k)); // MWh, fmtGWh handles display

    if (titleEl) titleEl.textContent = `${zone} — Demand per ${period} (GWh)`;
    setStatus('');
    const ctx = canvas.getContext('2d');
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();
    if (loadZoneChart) { try { loadZoneChart.destroy(); } catch (_) {} loadZoneChart = null; }
    loadZoneChart = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets: [{ label: `Demand per ${period} (GWh)`, data: values, backgroundColor: 'rgba(2,132,199,0.7)', borderColor: '#0284c7', borderWidth: 1 }] },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { display: true, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } } },
            scales: {
                x: { type: 'category', ticks: { maxRotation: 45 }, grid: { display: false } },
                y: { beginAtZero: false, ticks: { callback: v => fmtGWh(Number(v)) } },
            },
        },
    });
}

function updatePriceEuRangeButtonActive() {
    const map = {
        day: 'priceEuRangeDayBtn',
        week: 'priceEuRangeWeekBtn',
        month: 'priceEuRangeMonthBtn',
        '6m': 'priceEuRange6mBtn',
        '1y': 'priceEuRange1yBtn',
        '5y': 'priceEuRange5yBtn',
    };
    Object.entries(map).forEach(([range, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.toggle('active', priceEuRange === range);
    });
}

function updatePriceZoneRangeButtonActive() {
    const map = {
        day: 'priceZoneRangeDayBtn',
        week: 'priceZoneRangeWeekBtn',
        month: 'priceZoneRangeMonthBtn',
        '6m': 'priceZoneRange6mBtn',
        '1y': 'priceZoneRange1yBtn',
        '5y': 'priceZoneRange5yBtn',
    };
    Object.entries(map).forEach(([range, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.toggle('active', priceZoneRange === range);
    });
}

function priceRangeToSinceIso(range) {
    const now = Date.now();
    const days =
        range === 'day' ? 1 :
        range === 'week' ? 7 :
        range === 'month' ? 31 :
        range === '6m' ? 183 :
        range === '1y' ? 365 :
        range === '5y' ? 365 * 5 : 365;
    return new Date(now - days * 24 * 60 * 60 * 1000).toISOString();
}

function cbRangeToSinceIso(range) {
    return rangeToSinceIso(range);
}

const CB_COUNTRIES = [
    'EU',
    'AT','BE','BG','CH','CY','CZ','DE','DK','EE','ES','FI','FR','GB','GR','HR','HU','IE','IT',
    'LT','LU','LV','MT','NL','NO','PL','PT','RO','SE','SI','SK'
];

// Country names + flag helper (shared by gas + electricity chart builders)
const CB_COUNTRY_NAMES = {
    AT:'Austria', BE:'Belgium', BG:'Bulgaria', CH:'Switzerland', CY:'Cyprus',
    CZ:'Czechia', DE:'Germany', DK:'Denmark', EE:'Estonia', ES:'Spain',
    FI:'Finland', FR:'France', GB:'United Kingdom', GR:'Greece', HR:'Croatia',
    HU:'Hungary', IE:'Ireland', IT:'Italy', LT:'Lithuania', LU:'Luxembourg',
    LV:'Latvia', MT:'Malta', NL:'Netherlands', NO:'Norway', PL:'Poland',
    PT:'Portugal', RO:'Romania', SE:'Sweden', SI:'Slovenia', SK:'Slovakia',
    UK:'United Kingdom', EL:'Greece',
    EU:'European Union', EU27:'European Union (27)',
};

function cbCountryFlag(isoCode) {
    const code = (isoCode === 'UK' ? 'GB' : isoCode === 'EL' ? 'GR' : isoCode).toUpperCase();
    if (code.length !== 2) return '';
    try {
        return String.fromCodePoint(...[...code].map(c => 0x1F1E6 + c.charCodeAt(0) - 65));
    } catch (_) { return ''; }
}

function cbElecMetricTitle(metric) {
    return { renewable: 'Renewable', generation: 'Generation', demand: 'Demand', prices: 'Prices' }[metric] || metric;
}

// Returns the display label for a series (used in sidebar, chart tooltip, and export legend).
function cbSeriesLabel(s) {
    let metricLabel;
    if (s.metric === 'generation' && s.psrFilter) {
        metricLabel = ELEC_TYPE_GROUPS.find(g => g.key === s.psrFilter)?.label ?? s.psrFilter;
    } else {
        metricLabel = cbElecMetricTitle(s.metric);
    }
    return `${s.country} · ${metricLabel}`;
}

// ─── Carbon intensity tab ────────────────────────────────────────────────────

// IPCC lifecycle median emission factors, gCO₂eq/kWh, keyed by ELEC_TYPE_GROUPS key.
const CARBON_FACTORS = {
    wind: 11, solar: 45, hydro: 24, nuclear: 12,
    gas: 490, coal: 820, biomass: 230,
    other_ren: 40, other_fossil: 650, other: 500,
};

function initCarbonTabControls() {
    const bind = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            carbonRange = range;
            updateCarbonRangeBtnActive();
            loadCarbonChart(range);
        });
    };
    bind('carbonRangeDayBtn', 'day');
    bind('carbonRangeWeekBtn', 'week');
    bind('carbonRangeMonthBtn', 'month');
    bind('carbonRange6mBtn', '6m');
    bind('carbonRange1yBtn', '1y');
    bind('carbonRange5yBtn', '5y');

    const bindZone = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            carbonZoneRange = range;
            updateCarbonZoneRangeBtnActive();
            if (carbonZoneSelected) loadCarbonZoneChart(carbonZoneSelected, range);
        });
    };
    bindZone('carbonZoneRangeDayBtn', 'day');
    bindZone('carbonZoneRangeWeekBtn', 'week');
    bindZone('carbonZoneRangeMonthBtn', 'month');
    bindZone('carbonZoneRange6mBtn', '6m');
    bindZone('carbonZoneRange1yBtn', '1y');

    const bindZoneAgg = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => { carbonZoneAggRange = range; updateCarbonZoneAggRangeButtonActive(); if (carbonZoneSelected) loadCarbonZoneChart(carbonZoneSelected, range); });
    };
    bindZoneAgg('carbonZoneAggWeekBtn', 'week');
    bindZoneAgg('carbonZoneAggMonthBtn', 'month');
    bindZoneAgg('carbonZoneAggYearBtn', 'year');
    const carbonZoneModeI = document.getElementById('carbonZoneModeIntradayBtn');
    if (carbonZoneModeI && !carbonZoneModeI.dataset.bound) { carbonZoneModeI.dataset.bound='1'; carbonZoneModeI.addEventListener('click', () => setCarbonZoneMode('intraday')); }
    const carbonZoneModeA = document.getElementById('carbonZoneModeAggBtn');
    if (carbonZoneModeA && !carbonZoneModeA.dataset.bound) { carbonZoneModeA.dataset.bound='1'; carbonZoneModeA.addEventListener('click', () => setCarbonZoneMode('aggregate')); }
    setChartModePanels('carbonZone', carbonZoneMode);
}

function updateCarbonRangeBtnActive() {
    ['day', 'week', 'month', '6m', '1y', '5y'].forEach(r => {
        const id = `carbonRange${r.charAt(0).toUpperCase() + r.slice(1)}Btn`;
        document.getElementById(id)?.classList.toggle('active', r === carbonRange);
    });
}

function loadCarbonTabData() {
    updateCarbonRangeBtnActive();
    loadCarbonChart(carbonRange);
    if (!carbonCountryDataLoaded) {
        carbonCountryDataLoaded = true;
        loadCarbonCountryMap();
    }
}

// Compute carbon intensity (gCO₂/kWh) for each timestamp from generation rows.
// rows: [{ ts, psr_type, mw }]  (MW average over each interval)
function computeCarbonIntensity(rows) {
    const byTs = new Map(); // ts → { totalMw, co2Mw }
    for (const r of rows) {
        if (!r.ts || !r.psr_type || !Number.isFinite(Number(r.mw))) continue;
        const mw = Number(r.mw);
        if (mw <= 0) continue;
        let factor = null;
        for (const g of ELEC_TYPE_GROUPS) {
            if (g.types.includes(r.psr_type)) { factor = CARBON_FACTORS[g.key] ?? null; break; }
        }
        if (factor === null) continue;
        const prev = byTs.get(r.ts) || { totalMw: 0, co2Mw: 0 };
        byTs.set(r.ts, { totalMw: prev.totalMw + mw, co2Mw: prev.co2Mw + mw * factor });
    }
    return Array.from(byTs.entries())
        .sort((a, b) => a[0] < b[0] ? -1 : 1)
        .map(([ts, { totalMw, co2Mw }]) => ({
            ts,
            intensity: totalMw > 0 ? co2Mw / totalMw : null,
        }))
        .filter(p => p.intensity !== null);
}

async function loadCarbonChart(range) {
    if (carbonChartLoadInFlight) return await carbonChartLoadInFlight;
    carbonChartLoadInFlight = (async () => {
        const statusEl = document.getElementById('carbonStatus');
        const titleEl = document.getElementById('carbonChartTitle');
        const canvas = document.getElementById('carbonIntensityChart');
        if (!canvas) return;
        const setStatus = msg => { if (statusEl) statusEl.textContent = msg || ''; };

        try {
            if (!supabase) throw new Error('Supabase client not initialized.');
            const since = euRangeToSinceIso(range);
            const useWeekly = range === '5y';
            const useDaily = range === '6m' || range === '1y';
            const useRaw = !useDaily && !useWeekly;

            setStatus(`Loading carbon intensity (${range})…`);

            let rows;
            if (useRaw) {
                rows = await gasFetchAllPaged(() =>
                    supabase.from('electricity_eu_generation_15m_mv')
                        .select('ts, psr_type, mw').gte('ts', since).order('ts', { ascending: true })
                , 1000, 200_000);
            } else {
                const table = useWeekly ? 'electricity_eu_generation_weekly_mwh' : 'electricity_eu_generation_daily_mwh';
                rows = await gasFetchAllPaged(() =>
                    supabase.from(table)
                        .select('ts, psr_type, production_mwh').gte('ts', since).order('ts', { ascending: true })
                , 1000, 100_000);
                rows = rows.map(r => ({ ...r, mw: r.production_mwh })); // factor calc works on any proportional quantity
            }

            const points = computeCarbonIntensity(rows);
            if (!points.length) { setStatus('No data.'); return; }

            const latestIntensity = points[points.length - 1].intensity;
            const avgIntensity = points.reduce((s, p) => s + p.intensity, 0) / points.length;

            // Low-carbon share from latest snapshot (nuclear + renewables)
            const LOW_CARBON_PSR = new Set(['B14','B18','B19','B16','B10','B11','B12','B09','B13','B15','B01','B17']);
            const latestTs = points[points.length - 1].ts;
            const latestRows = rows.filter(r => r.ts === latestTs);
            const totalMwLatest = latestRows.reduce((s, r) => s + (Number(r.mw) || 0), 0);
            const cleanMwLatest = latestRows
                .filter(r => LOW_CARBON_PSR.has(r.psr_type))
                .reduce((s, r) => s + (Number(r.mw) || 0), 0);
            const cleanPct = totalMwLatest > 0 ? (cleanMwLatest / totalMwLatest * 100) : null;

            document.getElementById('carbonLatestIntensity').textContent = `${Math.round(latestIntensity)} gCO₂/kWh`;
            document.getElementById('carbonAvgIntensity').textContent = `${Math.round(avgIntensity)} gCO₂/kWh`;
            document.getElementById('carbonCleanShare').textContent = cleanPct !== null ? `${cleanPct.toFixed(1)}%` : '-';

            const labels = points.map(p => {
                const d = new Date(p.ts);
                return Number.isNaN(d.getTime()) ? p.ts : (useRaw ? d.toLocaleString() : d.toLocaleDateString());
            });
            const intensityData = points.map(p => Math.round(p.intensity));

            const ctx = canvas.getContext('2d');
            if (carbonChart) { try { carbonChart.destroy(); } catch (_) {} carbonChart = null; }
            carbonChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels,
                    datasets: [{
                        label: 'Carbon intensity (gCO₂/kWh)',
                        data: intensityData,
                        borderColor: '#78716c',
                        backgroundColor: 'rgba(120,113,108,0.10)',
                        fill: true,
                        tension: 0.3,
                        pointRadius: 0,
                        borderWidth: 2,
                    }],
                },
                options: {
                    responsive: true, maintainAspectRatio: false, parsing: true,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { type: 'category', ticks: { maxRotation: 0, maxTicksLimit: 10 }, grid: { display: false } },
                        y: { beginAtZero: true, ticks: { callback: v => `${v}` } },
                    },
                },
            });
            if (titleEl) titleEl.textContent = `EU — Carbon intensity (gCO₂/kWh) — ${range}`;
            setStatus('');

            // Generation mix donut for latest hour
            loadCarbonMixChart(latestRows);
        } catch (err) {
            console.error('Carbon intensity chart failed:', err);
            setStatus(`Failed: ${err.message || String(err)}`);
        }
    })();
    try { return await carbonChartLoadInFlight; } finally { carbonChartLoadInFlight = null; }
}

function loadCarbonMixChart(latestRows) {
    const mixCanvas = document.getElementById('carbonMixChart');
    if (!mixCanvas) return;
    const byGroup = Object.create(null);
    for (const r of latestRows) {
        const mw = Number(r.mw);
        if (!Number.isFinite(mw) || mw <= 0) continue;
        for (const g of ELEC_TYPE_GROUPS) {
            if (g.types.includes(r.psr_type)) {
                byGroup[g.key] = (byGroup[g.key] || 0) + mw;
                break;
            }
        }
    }
    const groups = ELEC_TYPE_GROUPS.filter(g => byGroup[g.key] > 0);
    if (carbonMixChart) { try { carbonMixChart.destroy(); } catch (_) {} carbonMixChart = null; }
    carbonMixChart = new Chart(mixCanvas.getContext('2d'), {
        type: 'doughnut',
        data: {
            labels: groups.map(g => g.label),
            datasets: [{ data: groups.map(g => Math.round(byGroup[g.key])), backgroundColor: groups.map(g => g.color), borderWidth: 1 }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { position: 'right', labels: { boxWidth: 12, font: { size: 11 } } } },
        },
    });
}

// ─── Carbon intensity by country ─────────────────────────────────────────────

// Map intensity (gCO₂/kWh) to a color with a power curve so the gradient
// spreads across the realistic EU range: ~30 g (FR nuclear) → green,
// ~150 g (DE gas mix) → orange, ~400 g+ (PL coal) → red.
function carbonIntensityColor(intensity) {
    const capped = Math.min(400, Math.max(0, Number(intensity)));
    const pct = Math.pow(capped / 400, 0.6) * 100;
    return mixColorRedToGreen(100 - pct);
}

function updateCarbonZoneRangeBtnActive() {
    ['day', 'week', 'month', '6m', '1y'].forEach(r => {
        const key = r === '6m' ? '6m' : r === '1y' ? '1y' : r.charAt(0).toUpperCase() + r.slice(1);
        document.getElementById(`carbonZoneRange${key}Btn`)?.classList.toggle('active', r === carbonZoneRange);
    });
}

async function loadCarbonCountryMap() {
    const statusEl = document.getElementById('carbonCountryStatus');
    const mapEl = document.getElementById('carbonCountryMap');
    const tableBody = document.getElementById('carbonCountryTableBody');
    if (!mapEl) return;
    const setStatus = msg => { if (statusEl) statusEl.textContent = msg || ''; };

    try {
        if (!supabase) return;
        setStatus('Loading country generation data…');
        // Step 1: find the latest timestamp — O(1) index seek with the (source, ts DESC) index
        const { data: peekData, error: peekErr } = await supabase
            .from('electricity_generation_snapshots')
            .select('ts')
            .eq('source', 'entsoe')
            .order('ts', { ascending: false })
            .limit(1);
        if (peekErr) throw new Error(peekErr.message);
        if (!peekData?.length) { setStatus('No zone generation data available.'); return; }
        const latestTs = peekData[0].ts;
        const oneHourBefore = new Date(new Date(latestTs).getTime() - 60 * 60 * 1000).toISOString();

        // Step 2: fetch all snapshots in the last hour — bounded range on indexed ts, fast
        const { data: snapRows, error: snapErr } = await supabase
            .from('electricity_generation_snapshots')
            .select('ts, zone_id, psr_type, mw')
            .eq('source', 'entsoe')
            .neq('zone_id', 'EU')
            .gte('ts', oneHourBefore)
            .lte('ts', latestTs)
            .limit(15000);
        if (snapErr) throw new Error(snapErr.message);
        if (!snapRows?.length) { setStatus('No zone generation data available.'); return; }

        // Group rows by zone → snapshot timestamp → psr_type rows
        const LOW_CARBON = new Set(['B14','B18','B19','B16','B10','B11','B12','B09','B13','B15','B01','B17']);
        const zoneSnaps = new Map(); // zone → Map<ts, rows[]>
        for (const r of snapRows) {
            const z = String(r.zone_id || '').toUpperCase();
            if (!z || z === 'EU') continue;
            if (!zoneSnaps.has(z)) zoneSnaps.set(z, new Map());
            const tsMap = zoneSnaps.get(z);
            if (!tsMap.has(r.ts)) tsMap.set(r.ts, []);
            tsMap.get(r.ts).push(r);
        }

        // For each zone: compute intensity per snapshot, then average across the hour
        const zoneIntensity = new Map();
        for (const [zone, tsMap] of zoneSnaps) {
            const snapIntensities = [];
            let sumCleanMw = 0, sumTotalMw = 0, newestTs = '';
            for (const [ts, rows] of tsMap) {
                const pts = computeCarbonIntensity(rows.map(r => ({ ts: r.ts, psr_type: r.psr_type, mw: r.mw })));
                if (pts.length) snapIntensities.push(pts[pts.length - 1].intensity);
                sumTotalMw += rows.reduce((s, r) => s + (Number(r.mw) || 0), 0);
                sumCleanMw += rows.filter(r => LOW_CARBON.has(r.psr_type)).reduce((s, r) => s + (Number(r.mw) || 0), 0);
                if (ts > newestTs) newestTs = ts;
            }
            if (!snapIntensities.length) continue;
            const avgIntensity = snapIntensities.reduce((s, v) => s + v, 0) / snapIntensities.length;
            zoneIntensity.set(zone, {
                intensity: avgIntensity,
                cleanPct: sumTotalMw > 0 ? sumCleanMw / sumTotalMw * 100 : null,
                ts: newestTs,
            });
        }

        if (!zoneIntensity.size) { setStatus('Could not compute zone intensities.'); return; }

        // Geo map (falls back to tile grid if GeoJSON unavailable)
        await renderCarbonGeoMap(mapEl, zoneIntensity).catch(e => {
            mapFallbackNote(mapEl, e);
            console.warn('Carbon geo map failed, using tile grid:', e);
            renderCarbonTileGrid(mapEl, zoneIntensity);
        });

        // Table
        if (tableBody) {
            const fmt = d => {
                if (!d) return '—';
                const dt = new Date(d);
                return Number.isNaN(dt.getTime()) ? d : dt.toLocaleString();
            };
            const sorted = Array.from(zoneIntensity.entries()).sort((a, b) => (b[1].intensity ?? 0) - (a[1].intensity ?? 0));
            tableBody.innerHTML = sorted.map(([zone, { intensity, cleanPct, ts }]) => `
                <tr style="cursor:pointer;" onclick="carbonZoneSelected='${zone}'; document.getElementById('carbonZoneSection').style.display=''; updateCarbonZoneRangeBtnActive(); loadCarbonZoneChart('${zone}', carbonZoneRange);">
                    <td><strong>${escapeHtml(zone)}</strong></td>
                    <td>${Number.isFinite(intensity) ? Math.round(intensity) + ' g' : '—'}</td>
                    <td>${cleanPct != null ? cleanPct.toFixed(1) + '%' : '—'}</td>
                    <td style="font-size:0.8rem; color:var(--text-secondary);">${fmt(ts)}</td>
                </tr>
            `).join('');
            document.getElementById('carbonCountryTableSection')?.style.setProperty('display', '');
        }

        setStatus('');
    } catch (err) {
        console.error('Carbon country map failed:', err);
        setStatus(`Failed: ${err.message || String(err)}`);
    }
}

function renderCarbonTileGrid(mapEl, zoneIntensity) {
    const sorted = Array.from(zoneIntensity.entries()).sort((a, b) => (b[1].intensity ?? 0) - (a[1].intensity ?? 0));
    const tiles = sorted.map(([zone, { intensity }]) => {
        const bg = Number.isFinite(intensity) ? carbonIntensityColor(intensity) : 'rgba(148,163,184,0.25)';
        const pct = Math.min(100, Math.max(0, intensity / 600 * 100));
        const color = textColorForBg(100 - pct);
        return `<div class="energy-map-tile ${carbonZoneSelected === zone ? 'active' : ''}" data-carbon-zone="${escapeHtml(zone)}" style="background:${bg}; color:${color}">
            <div class="energy-map-tile-code">${escapeHtml(zone)}</div>
            <div class="energy-map-tile-value">${Number.isFinite(intensity) ? Math.round(intensity) + 'g' : '—'}</div>
        </div>`;
    }).join('');
    mapEl.innerHTML = `<div class="energy-map-grid">${tiles}</div>`;
    mapEl.querySelectorAll('.energy-map-tile[data-carbon-zone]').forEach(el => {
        el.addEventListener('click', () => {
            const z = el.getAttribute('data-carbon-zone');
            if (!z) return;
            carbonZoneSelected = z;
            mapEl.querySelectorAll('.energy-map-tile').forEach(t => t.classList.remove('active'));
            el.classList.add('active');
            document.getElementById('carbonZoneSection')?.style.setProperty('display', '');
            updateCarbonZoneRangeBtnActive();
            loadCarbonZoneChart(z, carbonZoneRange);
        });
    });
}

async function renderCarbonGeoMap(container, zoneIntensity) {
    const [countryGeo, zoneGeo] = await Promise.all([
        fetchEuropeCountriesGeoJsonOnce(),
        // .catch here as well as inside: a cosmetic overlay must never be
        // able to reject the Promise.all and take the base map down with it.
        fetchEntsoeZonesGeoJsonOnce().catch(() => null),
    ]);
    const hasZoneOverlay = Array.isArray(zoneGeo?.features) && zoneGeo.features.length > 0;

    // Build lookup maps
    const byZone = {};
    const countrySum = new Map();
    for (const [zone, { intensity }] of zoneIntensity) {
        if (!Number.isFinite(intensity)) continue;
        byZone[zone] = intensity;
        const iso2 = zoneToCountryIso2(zone);
        const prev = countrySum.get(iso2) || { sum: 0, n: 0 };
        prev.sum += intensity; prev.n += 1;
        countrySum.set(iso2, prev);
    }
    const byCountry = {};
    countrySum.forEach((v, k) => { byCountry[k] = v.n ? v.sum / v.n : null; });

    const width = 1400, height = 860, padding = 10;
    const bounds = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };
    const selectedZone = String(carbonZoneSelected || '').toUpperCase();
    const selectedIso2 = zoneToCountryIso2(selectedZone);
    const selectedIntensity = (selectedZone && byZone[selectedZone] != null)
        ? byZone[selectedZone]
        : (selectedIso2 && byCountry[selectedIso2] != null ? byCountry[selectedIso2] : null);

    container.innerHTML = `
        <div class="energy-map-shell">
            <div class="energy-map-top">
                <div class="energy-map-top-left">
                    <div class="energy-map-title">Carbon intensity map</div>
                    <div class="energy-map-subtitle">Click a country to chart its carbon intensity over time</div>
                </div>
                <div class="energy-map-top-right">
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Selected</div>
                        <div class="energy-map-chip-value">${escapeHtml(selectedZone || '—')}</div>
                    </div>
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Intensity</div>
                        <div class="energy-map-chip-value">${Number.isFinite(selectedIntensity) ? Math.round(selectedIntensity) + ' g' : '—'}</div>
                    </div>
                </div>
            </div>
            <div class="energy-map-legend energy-map-legend--premium">
                <span>Low carbon</span>
                <div class="energy-map-legend-bar" style="background: linear-gradient(to right, #10b981, #f59e0b, #ef4444);"></div>
                <span>High carbon</span>
            </div>
            <div class="energy-map-stage">
                <svg class="energy-geo-map" viewBox="0 0 ${width} ${height}" role="img" aria-label="Carbon intensity map"></svg>
            </div>
        </div>`;

    const svg = container.querySelector('svg.energy-geo-map');
    if (!svg) return;

    let tooltip = document.querySelector('.energy-map-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.className = 'energy-map-tooltip';
        tooltip.style.display = 'none';
        document.body.appendChild(tooltip);
    }

    function selectZone(zone) {
        carbonZoneSelected = zone;
        document.getElementById('carbonZoneSection')?.style.setProperty('display', '');
        updateCarbonZoneRangeBtnActive();
        loadCarbonZoneChart(zone, carbonZoneRange);
        renderCarbonGeoMap(container, zoneIntensity).catch(() => {});
    }

    // Base country layer (skip DK/SE/NO/GB — drawn via zone overlay)
    const countryFeatures = Array.isArray(countryGeo?.features) ? countryGeo.features : [];
    for (const f of countryFeatures) {
        const iso2 = String(f?.properties?.ISO2 || '').toUpperCase();
        if (!iso2 || iso2 === 'RU' || iso2 === 'BY') continue;
        // Same as the other renderers, plus GB/UK which this map also ceded to
        // the overlay — which is why the United Kingdom was absent from the
        // carbon map entirely rather than drawn in the no-data fill.
        if (hasZoneOverlay && (iso2 === 'DK' || iso2 === 'SE' || iso2 === 'NO'
            || iso2 === 'GB' || iso2 === 'UK')) continue;
        const dataKey = iso2GeoToDataKey(iso2);
        const val = byCountry[dataKey];
        const fill = Number.isFinite(val) ? carbonIntensityColor(val) : NO_DATA_FILL;
        const geom = f.geometry;
        if (!geom) continue;
        const paths = [];
        if (geom.type === 'Polygon') {
            paths.push(polygonToPath(geom.coordinates[0], width, height, bounds, padding));
        } else if (geom.type === 'MultiPolygon') {
            for (const poly of geom.coordinates) if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
        } else continue;
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', paths.join(' '));
        path.setAttribute('fill', fill);
        path.setAttribute('data-iso2', iso2);
        path.style.cursor = 'pointer';
        if (selectedIso2 && iso2GeoMatchesSelection(iso2, selectedIso2)) path.classList.add('is-selected');
        path.addEventListener('mouseenter', () => {
            tooltip.style.display = 'block';
            tooltip.textContent = `${dataKey} — ${Number.isFinite(val) ? Math.round(val) + ' g CO₂/kWh' : '—'}`;
        });
        path.addEventListener('mousemove', e => { tooltip.style.left = `${e.clientX}px`; tooltip.style.top = `${e.clientY}px`; });
        path.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
        path.addEventListener('click', () => {
            const candidates = [...zoneIntensity.keys()].filter(z => zoneToCountryIso2(z) === dataKey);
            if (!candidates.length) return;
            selectZone(candidates[0]);
        });
        svg.appendChild(path);
    }

    // Zone overlay for DK/SE/NO/GB
    const europeBbox = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };
    const overlayZones = new Set(['DK1','DK2','SE1','SE2','SE3','SE4','NO1','NO2','NO3','NO4','NO5','GB']);
    for (const f of (Array.isArray(zoneGeo?.features) ? zoneGeo.features : [])) {
        const zoneId = normalizeZoneNameToId(f?.properties?.zoneName);
        if (!zoneId || !overlayZones.has(zoneId)) continue;
        const geom = filterGeometryToBbox(f?.geometry, europeBbox);
        if (!geom) continue;
        const val = byZone[zoneId];
        const fill = Number.isFinite(val) ? carbonIntensityColor(val) : NO_DATA_FILL;
        const paths = [];
        if (geom.type === 'Polygon') {
            paths.push(polygonToPath(geom.coordinates[0], width, height, bounds, padding));
        } else if (geom.type === 'MultiPolygon') {
            for (const poly of geom.coordinates) if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
        } else continue;
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', paths.join(' '));
        path.setAttribute('fill', fill);
        path.setAttribute('data-zone', zoneId);
        path.style.cursor = 'pointer';
        path.classList.add('bz-overlay');
        if (selectedZone && zoneId === selectedZone) path.classList.add('is-selected');
        path.addEventListener('mouseenter', () => {
            tooltip.style.display = 'block';
            tooltip.textContent = `${zoneId} — ${Number.isFinite(val) ? Math.round(val) + ' g CO₂/kWh' : '—'}`;
        });
        path.addEventListener('mousemove', e => { tooltip.style.left = `${e.clientX}px`; tooltip.style.top = `${e.clientY}px`; });
        path.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
        path.addEventListener('click', () => selectZone(zoneId));
        svg.appendChild(path);
    }
}

async function loadCarbonZoneChart(zone, range) {
    if (carbonZoneLoadInFlight) return await carbonZoneLoadInFlight;
    carbonZoneLoadInFlight = (async () => {
        const statusEl = document.getElementById('carbonZoneStatus');
        const titleEl = document.getElementById('carbonZoneChartTitle');
        const canvas = document.getElementById('carbonZoneChart');
        if (!canvas) return;
        const setStatus = msg => { if (statusEl) statusEl.textContent = msg || ''; };

        try {
            if (!supabase) return;

            if (carbonZoneMode === 'aggregate') {
                await loadCarbonZoneChartAggregate(zone, range, canvas, titleEl, setStatus);
                return;
            }

            const since = euRangeToSinceIso(range);
            const useDaily = range === '6m' || range === '1y';
            const useWeekly = false; // zone data only available daily at best for long ranges

            setStatus(`Loading ${zone} carbon intensity (${range})…`);
            if (titleEl) titleEl.textContent = `${zone} — Carbon intensity (gCO₂/kWh)`;
            document.getElementById('carbonZoneSectionTitle').textContent = `${zone} — Carbon intensity`;

            let rows;
            if (useDaily) {
                const dailyRows = await gasFetchAllPaged(() =>
                    supabase.from('electricity_generation_daily_mwh')
                        .select('ts, psr_type, production_mwh')
                        .eq('zone_id', zone)
                        .gte('ts', since)
                        .order('ts', { ascending: true })
                , 1000, 50_000);
                rows = dailyRows.map(r => ({ ts: r.ts, psr_type: r.psr_type, mw: r.production_mwh }));
            } else {
                rows = await gasFetchAllPaged(() =>
                    supabase.from('electricity_generation_snapshots')
                        .select('ts, psr_type, mw')
                        .eq('zone_id', zone)
                        .eq('source', 'entsoe')
                        .gte('ts', since)
                        .order('ts', { ascending: true })
                , 1000, 100_000);
            }

            const points = computeCarbonIntensity(rows);
            if (!points.length) { setStatus('No generation data for this zone.'); return; }

            const labels = points.map(p => {
                const d = new Date(p.ts);
                return Number.isNaN(d.getTime()) ? p.ts : (useDaily ? d.toLocaleDateString() : d.toLocaleString());
            });

            const ctx = canvas.getContext('2d');
            if (carbonZoneChart) { try { carbonZoneChart.destroy(); } catch (_) {} carbonZoneChart = null; }
            carbonZoneChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels,
                    datasets: [{
                        label: `${zone} carbon intensity (gCO₂/kWh)`,
                        data: points.map(p => Math.round(p.intensity)),
                        borderColor: '#78716c',
                        backgroundColor: 'rgba(120,113,108,0.10)',
                        fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2,
                    }],
                },
                options: {
                    responsive: true, maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { type: 'category', ticks: { maxRotation: 0, maxTicksLimit: 10 }, grid: { display: false } },
                        y: { beginAtZero: true, ticks: { callback: v => `${v}` } },
                    },
                },
            });
            setStatus('');
        } catch (err) {
            console.error('Carbon zone chart failed:', err);
            setStatus(`Failed: ${err.message || String(err)}`);
        }
    })();
    try { return await carbonZoneLoadInFlight; } finally { carbonZoneLoadInFlight = null; }
}

async function loadCarbonZoneChartAggregate(zone, period, canvas, titleEl, setStatus) {
    setStatus(`Loading ${zone} carbon aggregate (${period})…`);
    const since = aggSincePeriod(period);
    const rows = await gasFetchAllPaged(() =>
        supabase.from('electricity_generation_daily_mwh')
            .select('ts, psr_type, production_mwh').eq('zone_id', zone).gte('ts', since).order('ts', { ascending: true })
    , 1000, 50_000);

    // Group by period: accumulate generation per (period, psr_type), then compute intensity
    const periodGen = new Map(); // key → Map(psr_type → total_mwh)
    for (const r of rows) {
        if (!r.ts || !r.psr_type) continue;
        const key = tsPeriodKey(r.ts, period);
        if (!periodGen.has(key)) periodGen.set(key, new Map());
        const byType = periodGen.get(key);
        const v = Number(r.production_mwh);
        if (Number.isFinite(v)) byType.set(r.psr_type, (byType.get(r.psr_type) || 0) + v);
    }

    const keys = [...periodGen.keys()].sort();
    const labels = keys.map(k => tsPeriodLabel(k, period));
    const intensities = keys.map(k => {
        const byType = periodGen.get(k);
        let totalMwh = 0, co2Mwh = 0;
        for (const [psr, mwh] of byType) {
            const group = ELEC_TYPE_GROUPS.find(g => g.types.includes(psr));
            const factor = group ? (CARBON_FACTORS[group.key] ?? null) : null;
            if (factor !== null) { totalMwh += mwh; co2Mwh += mwh * factor; }
        }
        return totalMwh > 0 ? Math.round(co2Mwh / totalMwh) : null;
    });

    if (titleEl) titleEl.textContent = `${zone} — Carbon intensity per ${period} (gCO₂/kWh)`;
    setStatus('');
    const ctx = canvas.getContext('2d');
    if (carbonZoneChart) { try { carbonZoneChart.destroy(); } catch (_) {} carbonZoneChart = null; }
    carbonZoneChart = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets: [{ label: `Carbon intensity (gCO₂/kWh)`, data: intensities, backgroundColor: 'rgba(120,113,108,0.7)', borderColor: '#78716c', borderWidth: 1 }] },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { display: false } },
            scales: {
                x: { type: 'category', ticks: { maxRotation: 45 }, grid: { display: false } },
                y: { beginAtZero: true, ticks: { callback: v => `${v}` } },
            },
        },
    });
}

// ─── Price vs renewables scatter tab ─────────────────────────────────────────

const RENEWABLE_PSR_SET = new Set(['B16', 'B18', 'B19', 'B10', 'B11', 'B12']); // solar, wind, hydro

function initPriceGenTabControls() {
    const bind = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            priceGenRange = range;
            updatePriceGenRangeBtnActive();
            loadPriceGenChart(range);
        });
    };
    bind('priceGenRangeWeekBtn', 'week');
    bind('priceGenRangeMonthBtn', 'month');
    bind('priceGenRange6mBtn', '6m');
    bind('priceGenRange1yBtn', '1y');
}

function updatePriceGenRangeBtnActive() {
    ['week', 'month', '6m', '1y'].forEach(r => {
        const key = r === '6m' ? '6m' : r === '1y' ? '1y' : r.charAt(0).toUpperCase() + r.slice(1);
        document.getElementById(`priceGenRange${key}Btn`)?.classList.toggle('active', r === priceGenRange);
    });
}

function loadPriceGenTabData() {
    updatePriceGenRangeBtnActive();
    loadPriceGenChart(priceGenRange);
}

async function loadPriceGenChart(range) {
    if (priceGenLoadInFlight) return await priceGenLoadInFlight;
    priceGenLoadInFlight = (async () => {
        const statusEl = document.getElementById('priceGenStatus');
        const titleEl = document.getElementById('priceGenChartTitle');
        const canvas = document.getElementById('priceGenScatterChart');
        if (!canvas) return;
        const setStatus = msg => { if (statusEl) statusEl.textContent = msg || ''; };

        try {
            if (!supabase) throw new Error('Supabase client not initialized.');
            const since = euRangeToSinceIso(range);
            const useDaily = range === '6m' || range === '1y';

            setStatus(`Loading price & generation (${range})…`);

            let prices, genRows;
            if (useDaily) {
                [prices, genRows] = await Promise.all([
                    gasFetchAllPaged(() =>
                        supabase.from('electricity_eu_price_daily_mv')
                            .select('ts, price_eur_per_mwh').gte('ts', since).order('ts', { ascending: true })
                    , 1000, 10_000),
                    gasFetchAllPaged(() =>
                        supabase.from('electricity_eu_generation_daily_mwh')
                            .select('ts, psr_type, production_mwh').gte('ts', since).order('ts', { ascending: true })
                    , 1000, 100_000),
                ]);
            } else {
                [prices, genRows] = await Promise.all([
                    gasFetchAllPaged(() =>
                        supabase.from('electricity_eu_price_hourly_mv')
                            .select('ts, price_eur_per_mwh').gte('ts', since).order('ts', { ascending: true })
                    , 1000, 50_000),
                    gasFetchAllPaged(() =>
                        supabase.from('electricity_eu_generation_15m_mv')
                            .select('ts, psr_type, mw').gte('ts', since).order('ts', { ascending: true })
                    , 1000, 200_000),
                ]);
                // Aggregate 15m generation to hourly for price join
                const hourlyGen = new Map(); // "YYYY-MM-DDTHH" UTC hour key → { renMw, totalMw }
                for (const r of genRows) {
                    const key = r.ts.slice(0, 13); // e.g. "2026-05-21T14" — always UTC from Supabase
                    const mw = Number(r.mw) || 0;
                    const prev = hourlyGen.get(key) || { renMw: 0, totalMw: 0 };
                    prev.totalMw += mw;
                    if (RENEWABLE_PSR_SET.has(r.psr_type)) prev.renMw += mw;
                    hourlyGen.set(key, prev);
                }
                genRows = Array.from(hourlyGen.entries()).map(([ts, { renMw, totalMw }]) => ({ ts, renMw, totalMw }));
                prices = prices.map(r => ({
                    ts: r.ts.slice(0, 13), // normalize to same UTC hour key
                    price: Number(r.price_eur_per_mwh),
                }));

                // Build scatter points — both keyed by UTC hour string "YYYY-MM-DDTHH"
                const priceMap = new Map(prices.map(r => [r.ts, r.price]));
                const points = genRows
                    .map(r => {
                        const price = priceMap.get(r.ts);
                        const share = r.totalMw > 0 ? (r.renMw / r.totalMw) * 100 : null;
                        return (price != null && share !== null) ? { x: share, y: price } : null;
                    })
                    .filter(Boolean);

                renderPriceGenScatter(canvas, titleEl, setStatus, points, range);
                return;
            }

            // Daily path: both tables are keyed by day (YYYY-MM-DD or ISO date)
            const priceMap = new Map(
                prices.map(r => [String(r.ts).slice(0, 10), Number(r.price_eur_per_mwh)])
            );
            // Aggregate gen to daily renewable share
            const dailyGen = new Map(); // day → { renMwh, totalMwh }
            for (const r of genRows) {
                const day = String(r.ts).slice(0, 10);
                const mwh = Number(r.production_mwh) || 0;
                const prev = dailyGen.get(day) || { renMwh: 0, totalMwh: 0 };
                prev.totalMwh += mwh;
                if (RENEWABLE_PSR_SET.has(r.psr_type)) prev.renMwh += mwh;
                dailyGen.set(day, prev);
            }
            const points = Array.from(dailyGen.entries())
                .map(([day, { renMwh, totalMwh }]) => {
                    const price = priceMap.get(day);
                    const share = totalMwh > 0 ? (renMwh / totalMwh) * 100 : null;
                    return (price != null && share !== null) ? { x: share, y: price } : null;
                })
                .filter(Boolean);

            renderPriceGenScatter(canvas, titleEl, setStatus, points, range);
        } catch (err) {
            console.error('Price vs renewables chart failed:', err);
            setStatus(`Failed: ${err.message || String(err)}`);
        }
    })();
    try { return await priceGenLoadInFlight; } finally { priceGenLoadInFlight = null; }
}

function renderPriceGenScatter(canvas, titleEl, setStatus, points, range) {
    if (!points.length) { setStatus('No data.'); return; }

    const avgPrice = points.reduce((s, p) => s + p.y, 0) / points.length;
    const avgShare = points.reduce((s, p) => s + p.x, 0) / points.length;

    // Pearson correlation coefficient
    const meanX = avgShare, meanY = avgPrice;
    let num = 0, dX = 0, dY = 0;
    for (const p of points) {
        const dx = p.x - meanX, dy = p.y - meanY;
        num += dx * dy; dX += dx * dx; dY += dy * dy;
    }
    const corr = (dX > 0 && dY > 0) ? num / Math.sqrt(dX * dY) : 0;

    document.getElementById('priceGenCorrelation').textContent = corr.toFixed(2);
    document.getElementById('priceGenAvgPrice').textContent = `€${Math.round(avgPrice)}`;
    document.getElementById('priceGenAvgRenShare').textContent = `${avgShare.toFixed(1)}%`;

    const ctx = canvas.getContext('2d');
    if (priceGenChart) { try { priceGenChart.destroy(); } catch (_) {} priceGenChart = null; }
    priceGenChart = new Chart(ctx, {
        type: 'scatter',
        data: {
            datasets: [{
                label: 'Price (€/MWh) vs Renewable share (%)',
                data: points,
                backgroundColor: 'rgba(34,197,94,0.35)',
                borderColor: 'rgba(34,197,94,0.7)',
                borderWidth: 1,
                pointRadius: 3,
            }],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: item => `Renewables: ${item.parsed.x.toFixed(1)}%  Price: €${Math.round(item.parsed.y)}/MWh`,
                    },
                },
            },
            scales: {
                x: {
                    title: { display: true, text: 'Renewable share (%)' },
                    ticks: { callback: v => `${v}%` },
                },
                y: {
                    title: { display: true, text: 'Price (€/MWh)' },
                    ticks: { callback: v => `€${v}` },
                },
            },
        },
    });
    if (titleEl) titleEl.textContent = `EU — Price vs renewable share — ${range} (r = ${corr.toFixed(2)})`;
    setStatus('');
}

// ─── Gas storage tab ─────────────────────────────────────────────────────────

let gasStorageTabInited = false;
let gasStorageRange = '1y';
let gasStorageFillChart = null;
let gasStorageFlowChart = null;
let gasStorageLoadInFlight = null;

// Gas storage by country
const GAS_STORAGE_COUNTRIES = [
    'AT','BE','BG','CZ','DE','DK','ES','FR','HR','HU',
    'IT','LT','LV','NL','PL','PT','RO','SE','SI','SK',
];
let storageCountrySelected = null;
let storageCountryChart = null;
let storageCountryRange = '1y';
let storageCountryLoadInFlight = null;
let storageCountryGridInited = false;

async function loadGasStorageTab() {
    if (gasStorageLoadInFlight) return await gasStorageLoadInFlight;
    gasStorageLoadInFlight = (async () => {
        if (!gasStorageTabInited) {
            gasStorageTabInited = true;
            initGasStorageControls();
            await initStorageCountryGrid();
        }
        await loadGasStorageChart(gasStorageRange);
    })();
    try { return await gasStorageLoadInFlight; } finally { gasStorageLoadInFlight = null; }
}

function initGasStorageControls() {
    const bind = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            gasStorageRange = range;
            updateStorageRangeBtnActive();
            loadGasStorageChart(range);
        });
    };
    bind('storageRange3mBtn', '3m');
    bind('storageRange6mBtn', '6m');
    bind('storageRange1yBtn', '1y');
    bind('storageRange2yBtn', '2y');
    bind('storageRange5yBtn', '5y');
}

function updateStorageRangeBtnActive() {
    ['3m', '6m', '1y', '2y', '5y'].forEach(r => {
        const id = `storageRange${r.charAt(0).toUpperCase() + r.slice(1)}Btn`;
        document.getElementById(id)?.classList.toggle('active', r === gasStorageRange);
    });
}

function gasStorageRangeToSince(range) {
    const d = new Date();
    const map = { '3m': 90, '6m': 180, '1y': 365, '2y': 730, '5y': 1825 };
    d.setDate(d.getDate() - (map[range] ?? 365));
    return d.toISOString().slice(0, 10);
}

async function loadGasStorageChart(range) {
    const statusEl = document.getElementById('storageStatus');
    const fillTitleEl = document.getElementById('storageFillChartTitle');
    const flowTitleEl = document.getElementById('storageFlowChartTitle');
    const fillCanvas = document.getElementById('storageFillChart');
    const flowCanvas = document.getElementById('storageFlowChart');
    if (!fillCanvas || !flowCanvas) return;
    const setStatus = msg => { if (statusEl) statusEl.textContent = msg || ''; };
    updateStorageRangeBtnActive();

    try {
        if (!supabase) throw new Error('Supabase client not initialized.');
        const since = gasStorageRangeToSince(range);
        setStatus(`Loading gas storage (${range})…`);

        const rows = await gasFetchAllPaged(() =>
            supabase.from('gas_storage_eu_daily')
                .select('gas_day, gas_in_storage_twh, full_pct, trend_pct, injection_twh, withdrawal_twh')
                .gte('gas_day', since)
                .order('gas_day', { ascending: true })
        , 1000, 10_000);

        if (!rows.length) {
            setStatus('No storage data. Run gas_ingest_storage_eu to backfill, or check GIE_AGSI_API_KEY.');
            document.getElementById('storageApiKeyWarning')?.style.setProperty('display', 'inline');
            return;
        }

        const latest = rows[rows.length - 1];
        document.getElementById('storageLatestFill').textContent =
            latest.full_pct != null ? `${Number(latest.full_pct).toFixed(1)}%` : '-';
        document.getElementById('storageLatestDay').textContent = latest.gas_day ?? '-';
        const trend = latest.trend_pct;
        if (document.getElementById('storageLatestTrend')) {
            document.getElementById('storageLatestTrend').textContent =
                trend != null ? `${trend > 0 ? '+' : ''}${Number(trend).toFixed(2)}%` : '-';
        }
        document.getElementById('storageLatestTwh').textContent =
            latest.gas_in_storage_twh != null ? `${Number(latest.gas_in_storage_twh).toFixed(0)} TWh` : '-';

        const labels = rows.map(r => r.gas_day);
        const fillData = rows.map(r => r.full_pct != null ? Number(Number(r.full_pct).toFixed(2)) : null);
        const injData = rows.map(r => r.injection_twh != null ? Number(Number(r.injection_twh).toFixed(3)) : null);
        const wdData = rows.map(r => r.withdrawal_twh != null ? -Math.abs(Number(r.withdrawal_twh)) : null);

        if (gasStorageFillChart) { try { gasStorageFillChart.destroy(); } catch (_) {} gasStorageFillChart = null; }
        gasStorageFillChart = new Chart(fillCanvas.getContext('2d'), {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    label: 'Fill level (%)',
                    data: fillData,
                    borderColor: '#0ea5e9',
                    backgroundColor: 'rgba(14,165,233,0.12)',
                    fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2,
                }],
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: { legend: { display: false } },
                scales: {
                    x: { type: 'category', ticks: { maxRotation: 0, maxTicksLimit: 10 }, grid: { display: false } },
                    y: { min: 0, ticks: { callback: v => `${v}%` } },
                },
            },
        });

        if (gasStorageFlowChart) { try { gasStorageFlowChart.destroy(); } catch (_) {} gasStorageFlowChart = null; }
        gasStorageFlowChart = new Chart(flowCanvas.getContext('2d'), {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    { label: 'Injection (TWh/day)', data: injData, backgroundColor: 'rgba(34,197,94,0.7)', borderWidth: 0 },
                    { label: 'Withdrawal (TWh/day)', data: wdData, backgroundColor: 'rgba(239,68,68,0.7)', borderWidth: 0 },
                ],
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: { legend: { display: true, position: 'top', labels: { boxWidth: 12, font: { size: 11 } } } },
                scales: {
                    x: { type: 'category', ticks: { maxRotation: 0, maxTicksLimit: 10 }, grid: { display: false } },
                    y: { ticks: { callback: v => `${v} TWh` } },
                },
            },
        });

        if (fillTitleEl) fillTitleEl.textContent = `EU — Gas storage fill level (%) — ${range}`;
        if (flowTitleEl) flowTitleEl.textContent = `EU — Gas injection & withdrawal (TWh/day) — ${range}`;
        setStatus('');
    } catch (err) {
        console.error('Gas storage chart failed:', err);
        setStatus(`Failed: ${err.message || String(err)}`);
    }
}

// ─── Gas storage by country ──────────────────────────────────────────────────

const STORAGE_COUNTRY_COLORS = [
    '#0ea5e9','#f59e0b','#10b981','#8b5cf6','#ef4444',
    '#06b6d4','#f97316','#6366f1','#84cc16','#ec4899',
    '#14b8a6','#a855f7','#eab308','#22c55e','#3b82f6',
    '#d946ef','#78716c','#f43f5e','#0891b2','#65a30d',
];
let storageCountriesSelected = new Set();
let storageLatestFillCache = {};

async function initStorageCountryGrid() {
    if (storageCountryGridInited) return;
    storageCountryGridInited = true;
    const gridEl = document.getElementById('storageCountryGrid');
    if (!gridEl) return;

    const bindRange = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            storageCountryRange = range;
            updateStorageCountryRangeBtnActive();
            if (storageCountriesSelected.size) loadStorageCountryChart([...storageCountriesSelected], range);
        });
    };
    bindRange('storageCountryRange3mBtn', '3m');
    bindRange('storageCountryRange6mBtn', '6m');
    bindRange('storageCountryRange1yBtn', '1y');
    bindRange('storageCountryRange2yBtn', '2y');
    bindRange('storageCountryRange5yBtn', '5y');
    updateStorageCountryRangeBtnActive();

    // Fetch latest values per country
    let latestFill = {};
    let latestRows = [];
    try {
        const latestDate = await supabase
            .from('gas_storage_country_daily')
            .select('gas_day')
            .order('gas_day', { ascending: false })
            .limit(1);
        if (latestDate.data?.length) {
            const ld = latestDate.data[0].gas_day;
            const { data } = await supabase
                .from('gas_storage_country_daily')
                .select('country, full_pct, gas_in_storage_twh, injection_twh, withdrawal_twh, gas_day')
                .eq('gas_day', ld)
                .order('full_pct', { ascending: false });
            if (data) {
                latestRows = data;
                data.forEach(r => { latestFill[r.country] = Number(r.full_pct); });
                storageLatestFillCache = latestFill;
            }
        }
    } catch (_) {}

    renderStorageCountryTable(latestRows);

    await renderStorageGeoMap(gridEl, latestFill).catch(e => {
        console.warn('Storage geo map failed, using tiles:', e);
        renderStorageTileGrid(gridEl, latestFill);
    });
}

async function renderStorageGeoMap(container, latestFill) {
    const countryGeo = await fetchEuropeCountriesGeoJsonOnce();
    const width = 1400, height = 860, padding = 10;
    const bounds = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };

    const selArr = [...storageCountriesSelected];
    const selLabel = selArr.length === 0 ? '—' : selArr.length === 1 ? selArr[0] : `${selArr.length} countries`;

    container.innerHTML = `
        <div class="energy-map-shell">
            <div class="energy-map-top">
                <div class="energy-map-top-left">
                    <div class="energy-map-title">Gas storage fill level</div>
                    <div class="energy-map-subtitle">Latest fill level — click to chart (multi-select)</div>
                </div>
                <div class="energy-map-top-right">
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Selected</div>
                        <div class="energy-map-chip-value">${escapeHtml(selLabel)}</div>
                    </div>
                </div>
            </div>
            <div class="energy-map-legend energy-map-legend--premium">
                <span>Empty</span>
                <div class="energy-map-legend-bar" style="background: linear-gradient(to right, #ef4444, #f59e0b, #10b981);"></div>
                <span>Full</span>
            </div>
            <div class="energy-map-stage">
                <svg class="energy-geo-map" viewBox="0 0 ${width} ${height}" role="img" aria-label="Gas storage map"></svg>
            </div>
        </div>`;

    const svg = container.querySelector('svg.energy-geo-map');
    if (!svg) return;

    let tooltip = document.querySelector('.energy-map-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.className = 'energy-map-tooltip';
        tooltip.style.display = 'none';
        document.body.appendChild(tooltip);
    }

    function toggleCountry(iso2) {
        if (storageCountriesSelected.has(iso2)) {
            storageCountriesSelected.delete(iso2);
        } else {
            storageCountriesSelected.add(iso2);
        }
        const chartCard = document.getElementById('storageCountryChartCard');
        if (storageCountriesSelected.size) {
            chartCard?.style.setProperty('display', '');
            loadStorageCountryChart([...storageCountriesSelected], storageCountryRange);
        } else {
            chartCard?.style.setProperty('display', 'none');
        }
        renderStorageGeoMap(container, latestFill).catch(() => {});
    }

    const countryFeatures = Array.isArray(countryGeo?.features) ? countryGeo.features : [];
    for (const f of countryFeatures) {
        const iso2 = String(f?.properties?.ISO2 || '').toUpperCase();
        if (!iso2 || iso2 === 'RU' || iso2 === 'BY') continue;
        const dataKey = iso2GeoToDataKey(iso2);
        const val = latestFill[dataKey];
        const hasData = Number.isFinite(val);
        const fill = hasData ? mixColorRedToGreen(val) : NO_DATA_FILL;
        const geom = f.geometry;
        if (!geom) continue;
        const paths = [];
        if (geom.type === 'Polygon') {
            paths.push(polygonToPath(geom.coordinates[0], width, height, bounds, padding));
        } else if (geom.type === 'MultiPolygon') {
            for (const poly of geom.coordinates) if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
        } else continue;
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', paths.join(' '));
        path.setAttribute('fill', fill);
        path.setAttribute('data-iso2', dataKey);
        if (hasData) path.style.cursor = 'pointer';
        if (storageCountriesSelected.has(dataKey)) path.classList.add('is-selected');
        path.addEventListener('mouseenter', () => {
            tooltip.style.display = 'block';
            tooltip.textContent = `${dataKey} — ${hasData ? val.toFixed(1) + '%' : 'no data'}`;
        });
        path.addEventListener('mousemove', e => { tooltip.style.left = `${e.clientX}px`; tooltip.style.top = `${e.clientY}px`; });
        path.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
        if (hasData) path.addEventListener('click', () => toggleCountry(dataKey));
        svg.appendChild(path);
    }
}

function renderStorageTileGrid(container, latestFill) {
    container.innerHTML = `<div class="energy-map-grid">` +
    GAS_STORAGE_COUNTRIES.map(c => {
        const pct = latestFill[c];
        const bg = Number.isFinite(pct) ? mixColorRedToGreen(pct) : 'rgba(148,163,184,0.25)';
        const textColor = Number.isFinite(pct) ? textColorForBg(pct) : 'rgba(15,23,42,0.8)';
        return `<div class="energy-map-tile ${storageCountriesSelected.has(c) ? 'active' : ''}" data-storage-country="${escapeHtml(c)}" style="background:${bg}; color:${textColor}">
            <div class="energy-map-tile-code">${escapeHtml(c)}</div>
            <div class="energy-map-tile-value">${Number.isFinite(pct) ? pct.toFixed(0) + '%' : '—'}</div>
        </div>`;
    }).join('') + `</div>`;
    container.querySelectorAll('.energy-map-tile[data-storage-country]').forEach(el => {
        el.addEventListener('click', () => {
            const c = el.getAttribute('data-storage-country');
            if (!c) return;
            if (storageCountriesSelected.has(c)) { storageCountriesSelected.delete(c); el.classList.remove('active'); }
            else { storageCountriesSelected.add(c); el.classList.add('active'); }
            const chartCard = document.getElementById('storageCountryChartCard');
            if (storageCountriesSelected.size) { chartCard?.style.setProperty('display', ''); loadStorageCountryChart([...storageCountriesSelected], storageCountryRange); }
            else { chartCard?.style.setProperty('display', 'none'); }
        });
    });
}

function renderStorageCountryTable(rows) {
    const section = document.getElementById('storageCountryTableSection');
    const tbody = document.getElementById('storageCountryTableBody');
    const dateEl = document.getElementById('storageCountryTableDate');
    if (!tbody || !rows.length) return;
    const fmt = v => v != null && Number.isFinite(Number(v)) ? Number(v).toFixed(1) : '—';
    const fmtPct = v => v != null && Number.isFinite(Number(v)) ? Number(v).toFixed(1) + '%' : '—';
    if (dateEl && rows[0]?.gas_day) dateEl.textContent = `As of ${rows[0].gas_day}`;
    tbody.innerHTML = rows.map(r => {
        const pct = Number(r.full_pct);
        const bg = Number.isFinite(pct) ? mixColorRedToGreen(pct) : NO_DATA_FILL;
        const inj = Number(r.injection_twh);
        const wit = Number(r.withdrawal_twh);
        const flowColor = Number.isFinite(inj) && Number.isFinite(wit)
            ? (inj > wit ? 'var(--green)' : inj < wit ? 'var(--red)' : '')
            : '';
        const countryName = CB_COUNTRY_NAMES[r.country] || r.country;
        return `<tr style="cursor:pointer;" data-country="${escapeHtml(r.country)}">
            <td>
                <span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:${bg}; margin-right:6px; vertical-align:middle;"></span>
                <strong>${escapeHtml(countryName)}</strong>
            </td>
            <td>${fmtPct(r.full_pct)}</td>
            <td>${fmt(r.gas_in_storage_twh)}</td>
            <td style="color:${flowColor}">${fmt(r.injection_twh)}</td>
            <td style="color:${flowColor}">${fmt(r.withdrawal_twh)}</td>
        </tr>`;
    }).join('');
    tbody.querySelectorAll('tr[data-country]').forEach(tr => {
        tr.addEventListener('click', () => {
            const country = tr.getAttribute('data-country');
            storageCountriesSelected.add(country);
            document.getElementById('storageCountryChartCard').style.display = '';
            loadStorageCountryChart([...storageCountriesSelected], storageCountryRange);
            renderStorageGeoMap(document.getElementById('storageCountryGrid'), storageLatestFillCache).catch(() => {});
        });
    });
    section?.style.setProperty('display', '');
}

function updateStorageCountryRangeBtnActive() {
    ['3m', '6m', '1y', '2y', '5y'].forEach(r => {
        const key = r.charAt(0).toUpperCase() + r.slice(1);
        document.getElementById(`storageCountryRange${key}Btn`)?.classList.toggle('active', r === storageCountryRange);
    });
}

async function loadStorageCountryChart(countries, range) {
    if (storageCountryLoadInFlight) return await storageCountryLoadInFlight;
    storageCountryLoadInFlight = (async () => {
        const statusEl = document.getElementById('storageCountryStatus');
        const titleEl = document.getElementById('storageCountryChartTitle');
        const canvas = document.getElementById('storageCountryChart');
        if (!canvas) return;
        const setStatus = msg => { if (statusEl) statusEl.textContent = msg || ''; };

        try {
            if (!supabase) return;
            const since = gasStorageRangeToSince(range);
            setStatus(`Loading storage (${range})…`);

            // Fetch all selected countries in parallel
            const countryDatasets = await Promise.all(countries.map(async (country, i) => {
                const rows = await gasFetchAllPaged(() =>
                    supabase.from('gas_storage_country_daily')
                        .select('gas_day, full_pct')
                        .eq('country', country)
                        .gte('gas_day', since)
                        .order('gas_day', { ascending: true })
                , 1000, 10_000);
                return { country, rows };
            }));

            // Build unified label set from all dates
            const allDates = new Set();
            countryDatasets.forEach(({ rows }) => rows.forEach(r => allDates.add(r.gas_day)));
            const labels = [...allDates].sort();

            const datasets = countryDatasets
                .filter(d => d.rows.length > 0)
                .map(({ country, rows }, i) => {
                    const byDate = Object.fromEntries(rows.map(r => [r.gas_day, r.full_pct]));
                    const color = STORAGE_COUNTRY_COLORS[i % STORAGE_COUNTRY_COLORS.length];
                    return {
                        label: country,
                        data: labels.map(d => byDate[d] != null ? Number(Number(byDate[d]).toFixed(2)) : null),
                        borderColor: color,
                        backgroundColor: 'transparent',
                        fill: false, tension: 0.3, pointRadius: 0, borderWidth: 2,
                        spanGaps: true,
                    };
                });

            if (!datasets.length) { setStatus('No storage data for selected countries.'); return; }

            if (storageCountryChart) { try { storageCountryChart.destroy(); } catch (_) {} storageCountryChart = null; }
            storageCountryChart = new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true, maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { display: datasets.length > 1 } },
                    scales: {
                        x: { type: 'category', ticks: { maxRotation: 0, maxTicksLimit: 10 }, grid: { display: false } },
                        y: { min: 0, ticks: { callback: v => `${v}%` } },
                    },
                },
            });
            const title = countries.length === 1
                ? `${countries[0]} — Gas storage fill level (%) — ${range}`
                : `Gas storage fill level (%) — ${range}`;
            if (titleEl) titleEl.textContent = title;
            setStatus('');
        } catch (err) {
            console.error('Storage country chart failed:', err);
            setStatus(`Failed: ${err.message || String(err)}`);
        }
    })();
    try { return await storageCountryLoadInFlight; } finally { storageCountryLoadInFlight = null; }
}

// ─── Cross-border flows / transmission ───────────────────────────────────────

let flowsTabInited = false;
let flowsRange = 'week';
let flowsSelectedZone = null;
let flowsPartnerChart = null;
let flowsNetChart = null;
let flowsMapData = null; // cached for re-renders after zone click

// Key EU border pairs (both directions for net calculation)
const TRANSMISSION_PAIRS = [
    {from:'FR',to:'DE'},{from:'DE',to:'FR'},{from:'FR',to:'BE'},{from:'BE',to:'FR'},
    {from:'FR',to:'ES'},{from:'ES',to:'FR'},{from:'FR',to:'IT'},{from:'IT',to:'FR'},
    {from:'FR',to:'GB'},{from:'GB',to:'FR'},{from:'DE',to:'AT'},{from:'AT',to:'DE'},
    {from:'DE',to:'NL'},{from:'NL',to:'DE'},{from:'DE',to:'PL'},{from:'PL',to:'DE'},
    {from:'DE',to:'CZ'},{from:'CZ',to:'DE'},{from:'DE',to:'DK1'},{from:'DK1',to:'DE'},
    {from:'DE',to:'DK2'},{from:'DK2',to:'DE'},{from:'BE',to:'NL'},{from:'NL',to:'BE'},
    {from:'BE',to:'GB'},{from:'GB',to:'BE'},{from:'NL',to:'GB'},{from:'GB',to:'NL'},
    {from:'AT',to:'IT'},{from:'IT',to:'AT'},{from:'AT',to:'HU'},{from:'HU',to:'AT'},
    {from:'AT',to:'CZ'},{from:'CZ',to:'AT'},{from:'AT',to:'SI'},{from:'SI',to:'AT'},
    {from:'ES',to:'PT'},{from:'PT',to:'ES'},{from:'CZ',to:'SK'},{from:'SK',to:'CZ'},
    {from:'SK',to:'HU'},{from:'HU',to:'SK'},{from:'HU',to:'RO'},{from:'RO',to:'HU'},
    {from:'HU',to:'HR'},{from:'HR',to:'HU'},{from:'RO',to:'BG'},{from:'BG',to:'RO'},
    {from:'BG',to:'GR'},{from:'GR',to:'BG'},{from:'IT',to:'SI'},{from:'SI',to:'IT'},
    {from:'IT',to:'GR'},{from:'GR',to:'IT'},{from:'SI',to:'HR'},{from:'HR',to:'SI'},
    {from:'PL',to:'CZ'},{from:'CZ',to:'PL'},{from:'PL',to:'SK'},{from:'SK',to:'PL'},
    {from:'NO2',to:'NL'},{from:'NL',to:'NO2'},{from:'NO2',to:'DK1'},{from:'DK1',to:'NO2'},
    {from:'SE3',to:'DK1'},{from:'DK1',to:'SE3'},{from:'SE4',to:'DK2'},{from:'DK2',to:'SE4'},
    {from:'FI',to:'SE1'},{from:'SE1',to:'FI'},{from:'FI',to:'EE'},{from:'EE',to:'FI'},
    {from:'EE',to:'LV'},{from:'LV',to:'EE'},{from:'LV',to:'LT'},{from:'LT',to:'LV'},
    {from:'LT',to:'PL'},{from:'PL',to:'LT'},
    // Switzerland was absent from this list, so its four borders were never
    // ingested and every balance that depends on them was short.
    {from:'CH',to:'FR'},{from:'FR',to:'CH'},{from:'CH',to:'DE'},{from:'DE',to:'CH'},
    {from:'CH',to:'IT'},{from:'IT',to:'CH'},{from:'CH',to:'AT'},{from:'AT',to:'CH'},
];

function netFlowColor(netGwh, maxAbsGwh) {
    if (!Number.isFinite(netGwh) || maxAbsGwh <= 0) return 'rgba(148,163,184,0.25)';
    const t = Math.max(-1, Math.min(1, netGwh / maxAbsGwh));
    if (t >= 0) {
        // 0→grey, 1→blue
        return `rgb(${Math.round(lerp(180,14,t))},${Math.round(lerp(180,165,t))},${Math.round(lerp(180,233,t))})`;
    } else {
        // -1→orange, 0→grey
        const a = -t;
        return `rgb(${Math.round(lerp(180,249,a))},${Math.round(lerp(180,115,a))},${Math.round(lerp(180,22,a))})`;
    }
}

function initFlowsTabControls() {
    const bindRange = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => { flowsRange = range; updateFlowsRangeBtnActive(); loadFlowsMap(range); });
    };
    bindRange('flowsRangeDayBtn', 'day');
    bindRange('flowsRangeWeekBtn', 'week');
    bindRange('flowsRangeMonthBtn', 'month');
    bindRange('flowsRange6mBtn', '6m');
    updateFlowsRangeBtnActive();

    const refreshBtn = document.getElementById('flowsRefreshBtn');
    if (refreshBtn && !refreshBtn.dataset.bound) {
        refreshBtn.dataset.bound = '1';
        refreshBtn.addEventListener('click', async () => {
            refreshBtn.disabled = true;
            refreshBtn.textContent = '↻ Loading…';
            const setStatus = msg => { const el = document.getElementById('flowsStatus'); if (el) el.textContent = msg; };
            setStatus('Fetching latest flow data from ENTSO-E (this takes ~30s)…');
            try {
                const res = await fetch(`${window.__supabaseUrl}/functions/v1/entsoe_ingest_transmission_eu_latest`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${window.__supabaseAnonKey}` },
                    body: JSON.stringify({ pairs: TRANSMISSION_PAIRS, concurrency: 8, delay_ms: 50 }),
                });
                const data = await res.json();
                setStatus(data.ok ? `Loaded ${data.rows_upserted} flow readings. Errors: ${data.errors || 0}` : `Error: ${data.message || JSON.stringify(data)}`);
                if (data.ok) loadFlowsMap(flowsRange);
            } catch (e) {
                setStatus(`Refresh failed: ${e.message}`);
            } finally {
                refreshBtn.disabled = false;
                refreshBtn.textContent = '↻ Refresh data';
            }
        });
    }
}

function updateFlowsRangeBtnActive() {
    ['day','week','month','6m'].forEach(r => {
        const key = r === 'day' ? 'Day' : r === 'week' ? 'Week' : r === 'month' ? 'Month' : '6m';
        document.getElementById(`flowsRange${key}Btn`)?.classList.toggle('active', r === flowsRange);
    });
}

function flowsRangeToSince(range) {
    const d = new Date();
    const days = { day: 1, week: 7, month: 30, '6m': 180 };
    d.setDate(d.getDate() - (days[range] ?? 7));
    return d.toISOString().slice(0, 10);
}

async function loadFlowsMap(range) {
    const container = document.getElementById('flowsMapContainer');
    const statusEl = document.getElementById('flowsStatus');
    if (!container || !supabase) return;
    const setStatus = msg => { if (statusEl) statusEl.textContent = msg || ''; };
    setStatus('Loading flow data…');

    try {
        const since = flowsRangeToSince(range);
        const useIntraday = range === 'day';
        const table = useIntraday ? 'electricity_net_imports_mw' : 'electricity_net_imports_daily_mwh';
        const valueCol = useIntraday ? 'net_mw' : 'net_mwh';

        const rows = await gasFetchAllPaged(() =>
            supabase.from(table)
                .select(`zone_id,${valueCol}`)
                .gte('ts', useIntraday ? new Date(Date.now() - 86400000).toISOString() : since)
                .limit(50000)
        , 1000, 200_000);

        if (!rows.length) {
            setStatus('No flow data yet — click "↻ Refresh data" to load from ENTSO-E.');
            container.innerHTML = `<div class="chart-loading" style="padding:40px; text-align:center;">
                No cross-border flow data.<br>Click <strong>↻ Refresh data</strong> above to load the latest from ENTSO-E.
            </div>`;
            return;
        }

        // Aggregate net position per country (sum over time window, aggregate bidding zones → country)
        const countryNet = new Map();
        for (const r of rows) {
            const iso2 = zoneToCountryIso2(r.zone_id);
            if (!iso2 || iso2 === 'EU') continue;
            const v = Number(r[valueCol]);
            if (!Number.isFinite(v)) continue;
            countryNet.set(iso2, (countryNet.get(iso2) || 0) + v);
        }

        // Convert to average (divide by number of data points per zone)
        const countryCount = new Map();
        for (const r of rows) {
            const iso2 = zoneToCountryIso2(r.zone_id);
            if (!iso2 || iso2 === 'EU') continue;
            countryCount.set(iso2, (countryCount.get(iso2) || 0) + 1);
        }
        const netByZone = {};
        countryNet.forEach((sum, iso2) => {
            const n = countryCount.get(iso2) || 1;
            netByZone[iso2] = sum / n;
        });

        flowsMapData = netByZone;
        setStatus('');
        await renderFlowsGeoMap(container, netByZone);
    } catch (err) {
        console.error('Flows map failed:', err);
        setStatus(`Failed: ${err.message}`);
    }
}

async function renderFlowsGeoMap(container, netByZone) {
    const countryGeo = await fetchEuropeCountriesGeoJsonOnce();
    const width = 1400, height = 860, padding = 10;
    const bounds = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };

    const maxAbs = Math.max(...Object.values(netByZone).map(Math.abs).filter(Number.isFinite), 1);
    const selected = String(flowsSelectedZone || '').toUpperCase();
    const selNet = netByZone[selected];
    const selLabel = selected || '—';

    container.innerHTML = `
        <div class="energy-map-shell">
            <div class="energy-map-top">
                <div class="energy-map-top-left">
                    <div class="energy-map-title">Net import/export position</div>
                    <div class="energy-map-subtitle">Click a country to see its trading partner breakdown</div>
                </div>
                <div class="energy-map-top-right">
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Selected</div>
                        <div class="energy-map-chip-value">${escapeHtml(selLabel)}</div>
                    </div>
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Net position</div>
                        <div class="energy-map-chip-value">${Number.isFinite(selNet) ? (selNet > 0 ? '+' : '') + Math.round(selNet / 1000) + ' TWh' : '—'}</div>
                    </div>
                </div>
            </div>
            <div class="energy-map-legend energy-map-legend--premium">
                <span style="color:#f97316;">Net exporter</span>
                <div class="energy-map-legend-bar" style="background:linear-gradient(to right,#f97316,#94a3b8,#0ea5e9);"></div>
                <span style="color:#0ea5e9;">Net importer</span>
            </div>
            <div class="energy-map-stage">
                <svg class="energy-geo-map" viewBox="0 0 ${width} ${height}" role="img" aria-label="Flows map"></svg>
            </div>
        </div>`;

    const svg = container.querySelector('svg.energy-geo-map');
    if (!svg) return;

    let tooltip = document.querySelector('.energy-map-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.className = 'energy-map-tooltip';
        tooltip.style.display = 'none';
        document.body.appendChild(tooltip);
    }

    const countryFeatures = Array.isArray(countryGeo?.features) ? countryGeo.features : [];
    for (const f of countryFeatures) {
        const iso2 = String(f?.properties?.ISO2 || '').toUpperCase();
        if (!iso2 || iso2 === 'RU' || iso2 === 'BY') continue;
        const dataKey = iso2GeoToDataKey(iso2);
        const val = netByZone[dataKey];
        const fill = netFlowColor(val, maxAbs);
        const geom = f.geometry;
        if (!geom) continue;
        const paths = [];
        if (geom.type === 'Polygon') {
            paths.push(polygonToPath(geom.coordinates[0], width, height, bounds, padding));
        } else if (geom.type === 'MultiPolygon') {
            for (const poly of geom.coordinates) if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
        } else continue;

        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', paths.join(' '));
        path.setAttribute('fill', fill);
        path.style.cursor = Number.isFinite(val) ? 'pointer' : 'default';
        if (dataKey === selected) path.classList.add('is-selected');

        const gwh = Number.isFinite(val) ? Math.round(val / 1000) : null;
        path.addEventListener('mouseenter', () => {
            tooltip.style.display = 'block';
            tooltip.textContent = gwh != null
                ? `${dataKey} — ${gwh > 0 ? 'net import ' : 'net export '} ${Math.abs(gwh)} TWh`
                : `${dataKey} — no data`;
        });
        path.addEventListener('mousemove', e => { tooltip.style.left = `${e.clientX}px`; tooltip.style.top = `${e.clientY}px`; });
        path.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
        if (Number.isFinite(val)) {
            path.addEventListener('click', () => {
                flowsSelectedZone = dataKey;
                renderFlowsGeoMap(container, netByZone).catch(() => {});
                loadFlowsCountryCharts(dataKey, flowsRange);
            });
        }
        svg.appendChild(path);
    }
}

async function loadFlowsCountryCharts(zone, range) {
    document.getElementById('flowsCountrySection')?.style.setProperty('display', '');
    const titleEl = document.getElementById('flowsCountryTitle');
    if (titleEl) titleEl.textContent = `${zone} — Cross-border flows`;

    const since = flowsRangeToSince(range);
    const useIntraday = range === 'day';

    // Partner breakdown: sum net flow per trading partner
    try {
        const { data: rows } = await supabase
            .from('electricity_crossborder_flows')
            .select('ts, from_zone, to_zone, mw')
            .or(`from_zone.eq.${zone},to_zone.eq.${zone}`)
            .gte('ts', useIntraday ? new Date(Date.now() - 86400000).toISOString() : since + 'T00:00:00Z')
            .limit(20000);

        if (rows?.length) {
            // Net per partner (positive = zone imports from partner)
            const partnerNet = new Map();
            for (const r of rows) {
                if (r.to_zone === zone) {
                    // import from r.from_zone
                    partnerNet.set(r.from_zone, (partnerNet.get(r.from_zone) || 0) + Number(r.mw || 0));
                } else {
                    // export to r.to_zone
                    partnerNet.set(r.to_zone, (partnerNet.get(r.to_zone) || 0) - Number(r.mw || 0));
                }
            }
            const partners = [...partnerNet.entries()]
                .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
                .slice(0, 12);
            const labels = partners.map(([p]) => p);
            const values = partners.map(([, v]) => Math.round(v / 1000)); // MWh → GWh
            const colors = values.map(v => v > 0 ? '#0ea5e9' : '#f97316');

            if (flowsPartnerChart) { try { flowsPartnerChart.destroy(); } catch (_) {} flowsPartnerChart = null; }
            const canvas = document.getElementById('flowsPartnerChart');
            if (canvas) {
                flowsPartnerChart = new Chart(canvas.getContext('2d'), {
                    type: 'bar',
                    data: { labels, datasets: [{ label: 'Net flow (GWh)', data: values, backgroundColor: colors }] },
                    options: {
                        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { ticks: { callback: v => `${v} GWh` }, grid: { display: false } },
                            y: { ticks: { font: { size: 11 } } },
                        },
                    },
                });
            }
        }
    } catch (e) { console.error('Partner chart failed:', e); }

    // Net imports over time for this zone
    try {
        const table = useIntraday ? 'electricity_net_imports_mw' : 'electricity_net_imports_daily_mwh';
        const valueCol = useIntraday ? 'net_mw' : 'net_mwh';
        const { data: timeRows } = await supabase
            .from(table)
            .select(`ts,${valueCol}`)
            .eq('zone_id', zone)
            .gte('ts', useIntraday ? new Date(Date.now() - 86400000).toISOString() : since + 'T00:00:00Z')
            .order('ts', { ascending: true })
            .limit(5000);

        if (timeRows?.length) {
            const labels = timeRows.map(r => {
                const d = new Date(r.ts);
                return useIntraday ? d.toLocaleString() : d.toLocaleDateString();
            });
            const values = timeRows.map(r => r[valueCol] != null ? Math.round(Number(r[valueCol]) / 1000) : null);
            const posColor = 'rgba(14,165,233,0.8)', negColor = 'rgba(249,115,22,0.8)';

            if (flowsNetChart) { try { flowsNetChart.destroy(); } catch (_) {} flowsNetChart = null; }
            const canvas = document.getElementById('flowsNetChart');
            if (canvas) {
                flowsNetChart = new Chart(canvas.getContext('2d'), {
                    type: 'bar',
                    data: {
                        labels,
                        datasets: [{
                            label: 'Net imports (GWh/day)',
                            data: values,
                            backgroundColor: values.map(v => v != null && v >= 0 ? posColor : negColor),
                        }],
                    },
                    options: {
                        responsive: true, maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: { ticks: { maxRotation: 0, maxTicksLimit: 8 }, grid: { display: false } },
                            y: { ticks: { callback: v => `${v} GWh` } },
                        },
                    },
                });
            }
        }
    } catch (e) { console.error('Net time chart failed:', e); }
}

function initChartBuilderControls() {
    const search = document.getElementById('cbCountrySearch');

    const genTypeSection = document.getElementById('cbGenTypeSection');
    const syncGenTypeSection = () => {
        if (!genTypeSection) return;
        genTypeSection.hidden = (cbComposerMetric !== 'generation');
    };

    // Metric cards — set composer metric (what the next country click will use)
    const metricCards = document.querySelectorAll('#chartBuilderEmTab [data-elec-metric]');
    metricCards.forEach(card => {
        if (card.dataset.bound) return;
        card.dataset.bound = '1';
        card.addEventListener('click', () => {
            const m = card.getAttribute('data-elec-metric');
            cbComposerMetric = m;
            metricCards.forEach(c => c.classList.toggle('active', c === card));
            syncGenTypeSection();
            track('elec_cb_metric', { metric: m });
            // Re-render grid to show which countries already have this metric selected
            renderCbCountryGrid();
        });
    });
    metricCards.forEach(c => c.classList.toggle('active', c.getAttribute('data-elec-metric') === cbComposerMetric));
    syncGenTypeSection();

    // Generation type pills
    const genPills = document.querySelectorAll('#cbGenTypeSection [data-gen-filter]');
    genPills.forEach(pill => {
        if (pill.dataset.bound) return;
        pill.dataset.bound = '1';
        pill.addEventListener('click', () => {
            const filterVal = pill.getAttribute('data-gen-filter');
            cbComposerGenFilter = filterVal || null;
            genPills.forEach(p => p.classList.toggle('active', p === pill));
            renderCbCountryGrid();
        });
    });

    if (search && !search.dataset.bound) {
        search.dataset.bound = '1';
        search.addEventListener('input', () => renderCbCountryGrid());
    }

    const clearBtn = document.getElementById('cbClearBtn');
    if (clearBtn && !clearBtn.dataset.bound) {
        clearBtn.dataset.bound = '1';
        clearBtn.addEventListener('click', () => {
            cbSelected = [];
            cbRecolorAndRenderSelected();
            renderCbCountryGrid();
            loadChartBuilder();
        });
    }

    const bindRange = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            cbRange = range;
            track('elec_cb_range', { range });
            updateCbRangeButtons();
            loadChartBuilder();
        });
    };
    bindRange('cbRangeDayBtn', 'day');
    bindRange('cbRangeWeekBtn', 'week');
    bindRange('cbRangeMonthBtn', 'month');
    bindRange('cbRange6mBtn', '6m');
    bindRange('cbRange1yBtn', '1y');
    bindRange('cbRange5yBtn', '5y');

    const bindAgg = (id, mode) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            cbAggMode = mode;
            updateCbAggButtons();
            loadChartBuilder();
        });
    };
    bindAgg('cbAggAutoBtn', 'auto');
    bindAgg('cbAggIntradayBtn', 'intraday');
    bindAgg('cbAggDailyBtn', 'daily');
    bindAgg('cbAggWeeklyBtn', 'weekly');

    // Default series on first load
    if (!cbSelected.length) {
        cbSelected = ['DE', 'FR', 'ES'].map((country, idx) => ({
            id: `${country}:${cbComposerMetric}:total:${idx}`,
            country,
            metric: cbComposerMetric,
            psrFilter: null,
            color: cbColor(idx),
            visible: true,
        }));
    }

    cbRecolorAndRenderSelected();
    renderCbCountryGrid();
    updateCbRangeButtons();
    updateCbAggButtons();
}

function updateCbRangeButtons() {
    const map = {
        day: 'cbRangeDayBtn',
        week: 'cbRangeWeekBtn',
        month: 'cbRangeMonthBtn',
        '6m': 'cbRange6mBtn',
        '1y': 'cbRange1yBtn',
        '5y': 'cbRange5yBtn',
    };
    Object.entries(map).forEach(([range, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.toggle('active', cbRange === range);
    });
}

function updateCbAggButtons() {
    const map = { auto: 'cbAggAutoBtn', intraday: 'cbAggIntradayBtn', daily: 'cbAggDailyBtn', weekly: 'cbAggWeeklyBtn' };
    Object.entries(map).forEach(([mode, id]) => {
        document.getElementById(id)?.classList.toggle('active', cbAggMode === mode);
    });
}

function renderCbCountryGrid() {
    const grid = document.getElementById('cbCountriesGrid');
    const search = document.getElementById('cbCountrySearch');
    const pickedEl = document.getElementById('cbPickedCount');
    if (!grid) return;

    const q = String(search?.value || '').trim().toUpperCase();
    const list = CB_COUNTRIES
        .filter(c => !q || c.includes(q) || (CB_COUNTRY_NAMES[c] || '').toUpperCase().includes(q))
        .sort((a, b) => a.localeCompare(b));

    // A country card is "active" when it has a series matching the current composer state.
    // For generation, psrFilter is also part of the key.
    const seriesKey = (country, metric, psrFilter) =>
        `${country}:${metric}:${psrFilter ?? 'total'}`;
    const activeSet = new Set(cbSelected.map(s => seriesKey(s.country, s.metric, s.psrFilter)));
    const composerFilter = cbComposerMetric === 'generation' ? cbComposerGenFilter : null;
    const maxReached = cbSelected.length >= 6;

    if (pickedEl) pickedEl.textContent = `${cbSelected.length} / 6`;

    grid.innerHTML = list.map(c => {
        const isActive = activeSet.has(seriesKey(c, cbComposerMetric, composerFilter));
        const isDisabled = !isActive && maxReached;
        const name = CB_COUNTRY_NAMES[c] || '';
        return `
            <button class="cb-country-card ${isActive ? 'active' : ''}" data-cb-country="${escapeHtml(c)}" ${isDisabled ? 'disabled' : ''} title="${escapeHtml(name || c)}">
                <span class="cb-country-meat">
                    <span class="cb-country-code">${escapeHtml(c)}</span>
                    <span class="cb-country-name">${escapeHtml(name)}</span>
                </span>
            </button>
        `;
    }).join('');

    grid.querySelectorAll('button[data-cb-country]').forEach(btn => {
        btn.addEventListener('click', () => {
            const c = String(btn.getAttribute('data-cb-country') || '');
            if (!c) return;
            const psrFilter = cbComposerMetric === 'generation' ? cbComposerGenFilter : null;
            const key = seriesKey(c, cbComposerMetric, psrFilter);
            const existsIdx = cbSelected.findIndex(s => seriesKey(s.country, s.metric, s.psrFilter) === key);
            if (existsIdx >= 0) {
                cbSelected.splice(existsIdx, 1);
            } else {
                if (cbSelected.length >= 6) return;
                cbSelected.push({
                    id: `${c}:${cbComposerMetric}:${psrFilter ?? 'total'}:${Date.now()}`,
                    country: c,
                    metric: cbComposerMetric,
                    psrFilter: psrFilter,
                    color: cbColor(cbSelected.length),
                    visible: true,
                });
                track('elec_cb_series', { country: c, metric: cbComposerMetric, filter: psrFilter ?? 'total' });
            }
            cbRecolorAndRenderSelected();
            renderCbCountryGrid();
            loadChartBuilder();
        });
    });
}

function cbCandidateZonesForCountry(country) {
    const c = String(country || '').toUpperCase();
    if (c === 'DK') return ['DK1', 'DK2'];
    if (c === 'SE') return ['SE1', 'SE2', 'SE3', 'SE4'];
    if (c === 'NO') return ['NO1', 'NO2', 'NO3', 'NO4', 'NO5'];
    if (c === 'UK') return ['GB'];
    return [c];
}

function cbRollupCountry(points, country, mode = 'sum') {
    const candidates = cbCandidateZonesForCountry(country);
    const byTs = new Map();
    for (const p of points) {
        const z = String(p.zone || '').toUpperCase();
        if (!candidates.includes(z)) continue;
        const prev = byTs.get(p.ts) || { sum: 0, n: 0 };
        prev.sum += Number(p.y) || 0;
        prev.n += 1;
        byTs.set(p.ts, prev);
    }
    return [...byTs.entries()]
        .map(([ts, v]) => ({ ts, y: mode === 'avg' ? (v.n ? v.sum / v.n : null) : v.sum }))
        .filter(p => Number.isFinite(Number(p.y)))
        .sort((a, b) => new Date(a.ts).getTime() - new Date(b.ts).getTime());
}

async function cbFetchRenewableRaw(range, needEU = false) {
    const since = cbRangeToSinceIso(range);
    const useWeekly = cbAggMode === 'weekly' || (cbAggMode === 'auto' && range === '5y');
    const useDaily = !useWeekly && (cbAggMode === 'daily' || (cbAggMode === 'auto' && (range === 'month' || range === '6m' || range === '1y')));
    const table = useWeekly ? 'energy_mix_weekly' : useDaily ? 'energy_mix_daily' : 'energy_mix_snapshots';
    const rows = await gasFetchAllPaged(() =>
        supabase
            .from(table)
            .select('ts, zone_id, renewable_percent')
            .eq('source', 'entsoe')
            .gte('ts', since)
            .order('ts', { ascending: true })
    );
    const points = rows
        .filter(r => r.ts && r.zone_id && Number.isFinite(Number(r.renewable_percent)))
        .map(r => ({ ts: r.ts, zone: String(r.zone_id).toUpperCase(), y: Number(r.renewable_percent) }));

    let euSeries = [];
    if (needEU) {
        const euTable = useWeekly ? 'energy_eu_weekly_mv' : useDaily ? 'energy_eu_daily_mv' : 'energy_eu_15m_mv';
        const euRows = await gasFetchAllPaged(() =>
            supabase
                .from(euTable)
                .select('ts, renewable_percent')
                .gte('ts', since)
                .order('ts', { ascending: true })
        );
        euSeries = euRows
            .filter(r => r.ts && Number.isFinite(Number(r.renewable_percent)))
            .map(r => ({ ts: r.ts, y: Number(r.renewable_percent) }));
    }
    return { points, euSeries, mode: 'avg', unit: '%', title: 'Renewable share', fmt: (v) => `${Number(v).toFixed(1)}%` };
}

async function cbFetchDemandRaw(range, needEU = false) {
    const since = cbRangeToSinceIso(range);
    const useWeekly = cbAggMode === 'weekly' || (cbAggMode === 'auto' && range === '5y');
    const useDaily = !useWeekly && (cbAggMode === 'daily' || (cbAggMode === 'auto' && (range === 'month' || range === '6m' || range === '1y')));
    const raw = !(useWeekly || useDaily); // intraday or auto-day/week
    const table = raw ? 'electricity_load_snapshots' : (useWeekly ? 'electricity_load_weekly_mwh' : 'electricity_load_daily_mwh');
    const valueCol = raw ? 'load_mw' : 'consumption_mwh';
    const rows = await gasFetchAllPaged(() =>
        supabase
            .from(table)
            .select(`ts, zone_id, ${valueCol}`)
            .eq('source', 'entsoe')
            .gte('ts', since)
            .order('ts', { ascending: true })
    );
    const points = rows
        .filter(r => r.ts && r.zone_id && Number.isFinite(Number(r[valueCol])))
        .map(r => ({ ts: r.ts, zone: String(r.zone_id).toUpperCase(), y: Number(r[valueCol]) }));
    let euSeries = [];
    if (needEU) {
        if (raw) {
            // Use the hourly-aggregated MV instead of summing raw zone snapshots.
            const euMvRows = await gasFetchAllPaged(() =>
                supabase
                    .from('electricity_eu_load_15m_mv')
                    .select('ts, load_mw')
                    .gte('ts', since)
                    .order('ts', { ascending: true })
            , 1000, 100_000);
            euSeries = euMvRows
                .filter(r => r.ts && Number.isFinite(Number(r.load_mw)) && Number(r.load_mw) > 0)
                .map(r => ({ ts: r.ts, y: Number(r.load_mw) }));
        } else {
            const euTable = useWeekly ? 'electricity_eu_load_weekly_mwh' : 'electricity_eu_load_daily_mwh';
            const euCol = 'consumption_mwh';
            const euRows = await gasFetchAllPaged(() =>
                supabase
                    .from(euTable)
                    .select(`ts, ${euCol}`)
                    .gte('ts', since)
                    .order('ts', { ascending: true })
            );
            euSeries = euRows
                .filter(r => r.ts && Number.isFinite(Number(r[euCol])))
                .map(r => ({ ts: r.ts, y: Number(r[euCol]) }));
        }
    }
    const fmt = raw ? fmtMwShort : fmtGWh;
    return { points, euSeries, mode: 'sum', unit: raw ? 'MW' : 'GWh', title: 'Demand total', fmt };
}

async function cbFetchPriceRaw(range, needEU = false) {
    const since = cbRangeToSinceIso(range);
    const useWeekly = cbAggMode === 'weekly' || (cbAggMode === 'auto' && range === '5y');
    const useDaily = !useWeekly && (cbAggMode === 'daily' || (cbAggMode === 'auto' && (range === 'month' || range === '6m' || range === '1y')));
    const raw = !(useWeekly || useDaily);
    const table = useWeekly ? 'electricity_price_weekly' : useDaily ? 'electricity_price_daily' : 'electricity_day_ahead_prices';
    // Only the raw snapshots table has a `source` column; the aggregated tables do not.
    const query = () => {
        const q = supabase
            .from(table)
            .select('ts, zone_id, price_eur_per_mwh')
            .gte('ts', since)
            .order('ts', { ascending: true });
        return raw ? q.eq('source', 'entsoe') : q;
    };
    const rows = await gasFetchAllPaged(query);
    const points = rows
        .filter(r => r.ts && r.zone_id && Number.isFinite(Number(r.price_eur_per_mwh)))
        .map(r => ({ ts: r.ts, zone: String(r.zone_id).toUpperCase(), y: Number(r.price_eur_per_mwh) }));

    let euSeries = [];
    if (needEU) {
        const euTable = useWeekly ? 'electricity_eu_price_weekly_mv' : useDaily ? 'electricity_eu_price_daily_mv' : 'electricity_eu_price_hourly_mv';
        const euRows = await gasFetchAllPaged(() =>
            supabase
                .from(euTable)
                .select('ts, price_eur_per_mwh')
                .gte('ts', since)
                .order('ts', { ascending: true })
        );
        euSeries = euRows
            .filter(r => r.ts && Number.isFinite(Number(r.price_eur_per_mwh)))
            .map(r => ({ ts: r.ts, y: Number(r.price_eur_per_mwh) }));
    }
    return { points, euSeries, mode: 'avg', unit: '€/MWh', title: 'Day-ahead price', fmt: fmtEurPerMwh };
}

async function cbFetchGenerationRaw(range, needEU = false, psrFilter = null, zoneFilter = null) {
    const since = cbRangeToSinceIso(range);
    const useWeekly = cbAggMode === 'weekly' || (cbAggMode === 'auto' && range === '5y');
    const raw = cbAggMode === 'intraday' || (cbAggMode === 'auto' && (range === 'day' || range === 'week'));
    const table = useWeekly ? 'electricity_generation_weekly_mwh' : raw ? 'electricity_generation_snapshots' : 'electricity_generation_daily_mwh';
    const valueCol = raw ? 'mw' : 'production_mwh';

    // Resolve psr_type codes for the filter (null = total = no filter needed).
    const psrTypes = psrFilter
        ? (ELEC_TYPE_GROUPS.find(g => g.key === psrFilter)?.types ?? null)
        : null;

    // When EU is selected we need all zones to sum EU total accurately.
    // Otherwise, restrict to only the zones that appear in selected series — big speedup.
    const useZoneFilter = !needEU && zoneFilter && zoneFilter.length > 0;

    const rows = await gasFetchAllPagedParallel(() => {
        let q = supabase
            .from(table)
            .select(`ts, zone_id, psr_type, ${valueCol}`)
            .eq('source', 'entsoe')
            .gte('ts', since)
            .order('ts', { ascending: true });
        if (psrTypes) q = q.in('psr_type', psrTypes);
        if (useZoneFilter) q = q.in('zone_id', zoneFilter);
        return q;
    }, 1000, 600_000, 8);

    // Sum across psr_type rows into a zone total at each timestamp.
    const byTsZone = new Map();
    for (const r of rows) {
        const ts = r.ts;
        const zone = String(r.zone_id || '').toUpperCase();
        const y = Number(r[valueCol]);
        if (!ts || !zone || !Number.isFinite(y)) continue;
        const key = `${ts}|${zone}`;
        byTsZone.set(key, (byTsZone.get(key) || 0) + y);
    }
    const points = [...byTsZone.entries()].map(([k, y]) => {
        const [ts, zone] = k.split('|');
        return { ts, zone, y };
    }).sort((a, b) => new Date(a.ts) - new Date(b.ts));

    let euSeries = [];
    if (needEU) {
        if (raw) {
            // Query the EU hourly MV (avg per zone → sum across zones) to avoid spikes.
            // Direct per-timestamp sum of raw snapshots causes spikes because ENTSO-E
            // zones report at mixed resolutions — hourly zones are absent from 15-min bins.
            const euMvRows = await gasFetchAllPaged(() => {
                let q = supabase
                    .from('electricity_eu_generation_15m_mv')
                    .select('ts, psr_type, mw')
                    .gte('ts', since)
                    .order('ts', { ascending: true });
                if (psrTypes) q = q.in('psr_type', psrTypes);
                return q;
            }, 1000, 100_000);
            const byTs = new Map();
            for (const r of euMvRows) {
                if (!r.ts || !Number.isFinite(Number(r.mw))) continue;
                byTs.set(r.ts, (byTs.get(r.ts) || 0) + Number(r.mw));
            }
            euSeries = [...byTs.entries()].map(([ts, y]) => ({ ts, y })).sort((a, b) => new Date(a.ts) - new Date(b.ts));
        } else {
            const euTable = useWeekly ? 'electricity_eu_generation_weekly_mwh' : 'electricity_eu_generation_daily_mwh';
            const euRows = await gasFetchAllPaged(() => {
                let q = supabase
                    .from(euTable)
                    .select('ts, production_mwh, psr_type')
                    .gte('ts', since)
                    .order('ts', { ascending: true });
                if (psrTypes) q = q.in('psr_type', psrTypes);
                return q;
            }, 1000, 600_000);
            const byTs = new Map();
            for (const r of euRows) {
                if (!r.ts || !Number.isFinite(Number(r.production_mwh))) continue;
                byTs.set(r.ts, (byTs.get(r.ts) || 0) + Number(r.production_mwh));
            }
            euSeries = [...byTs.entries()].map(([ts, y]) => ({ ts, y })).sort((a, b) => new Date(a.ts) - new Date(b.ts));
        }
    }
    const typeLabel = psrFilter ? (ELEC_TYPE_GROUPS.find(g => g.key === psrFilter)?.label ?? psrFilter) : 'total';
    // EU raw uses the hourly MV so the unit is still MW (instantaneous avg over each hour).
    return { points, euSeries, mode: 'sum', unit: raw ? 'MW' : 'GWh', title: `Generation ${typeLabel}`, fmt: raw ? fmtMwShort : fmtGWh };
}

function cbColor(i) {
    const palette = [
        '#2563eb', '#dc2626', '#059669', '#7c3aed', '#ea580c', '#0891b2', '#be185d', '#65a30d',
    ];
    return palette[i % palette.length];
}

function cbFormatLabel(ts, range) {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return String(ts);
    if (range === 'day' || range === 'week') return d.toLocaleString(); // intraday: show date + time
    return d.toLocaleDateString();  // daily/weekly aggregates: date only, no 2 AM artifact
}

function cbRecolorAndRenderSelected() {
    cbSelected = cbSelected.slice(0, 6).map((s, idx) => ({ ...s, color: cbColor(idx) }));
    const listEl = document.getElementById('cbSeriesList');
    const countEl = document.getElementById('cbSeriesCount');
    const pickedEl = document.getElementById('cbPickedCount');
    if (countEl) countEl.textContent = String(cbSelected.length);
    if (pickedEl) pickedEl.textContent = `${cbSelected.length} / 6`;
    if (!listEl) return;

    if (!cbSelected.length) {
        listEl.innerHTML = '<div class="cb-empty-series">Pick a metric + zone below to add series.</div>';
        return;
    }

    listEl.innerHTML = cbSelected.map((s) => {
        const hiddenClass = s.visible ? '' : 'is-hidden';
        const fullLabel = cbSeriesLabel(s);
        const [countryPart, metricPart] = fullLabel.split(' · ');
        const eyeIcon = s.visible
            ? '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><ellipse cx="6" cy="6" rx="5" ry="3.5" stroke="currentColor" stroke-width="1.2"/><circle cx="6" cy="6" r="1.5" fill="currentColor"/></svg>'
            : '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M1 1L11 11" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><path d="M2.5 4.5C1.8 5 1.3 5.5 1 6c1.2 2 3.2 3.5 5 3.5 1 0 2-.4 2.8-1M4 2.7C4.6 2.3 5.3 2 6 2c1.8 0 3.8 1.5 5 3.5-.3.5-.7 1-1.2 1.4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>';
        return `
            <div class="cb-series-row ${hiddenClass}" data-cb-series-id="${escapeHtml(s.id)}">
                <div class="cb-swatch" style="background:${s.color};"><div class="cb-swatch-inner"></div></div>
                <div class="cb-series-label">
                    <div class="cb-series-code">${escapeHtml(countryPart ?? s.country)}</div>
                    <div class="cb-series-meta">${escapeHtml(metricPart ?? '')}</div>
                </div>
                <div class="cb-series-actions">
                    <button type="button" class="cb-icon-btn cb-series-eye" title="${s.visible ? 'Hide' : 'Show'}">${eyeIcon}</button>
                    <button type="button" class="cb-icon-btn cb-icon-btn-danger cb-series-remove" title="Remove">
                        <svg width="10" height="10" viewBox="0 0 10 10" fill="none"><path d="M1 1L9 9M9 1L1 9" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>
                    </button>
                </div>
            </div>
        `;
    }).join('');

    listEl.querySelectorAll('.cb-series-row').forEach((row) => {
        const sid = row.getAttribute('data-cb-series-id');
        const series = cbSelected.find(s => s.id === sid);
        if (!series) return;

        const eyeBtn = row.querySelector('.cb-series-eye');
        if (eyeBtn) {
            eyeBtn.addEventListener('click', () => {
                series.visible = !series.visible;
                cbSyncChartVisibility();
                cbRecolorAndRenderSelected();
            });
        }

        const removeBtn = row.querySelector('.cb-series-remove');
        if (removeBtn) {
            removeBtn.addEventListener('click', () => {
                cbSelected = cbSelected.filter(s => s.id !== sid);
                cbRecolorAndRenderSelected();
                renderCbCountryGrid();
                loadChartBuilder();
            });
        }
    });
}

function cbSyncChartVisibility() {
    if (!chartBuilderChart) return;
    for (const ds of chartBuilderChart.data.datasets || []) {
        const sid = ds._cbSeriesId;
        if (!sid) continue;
        const series = cbSelected.find(s => s.id === sid);
        ds.hidden = series ? !series.visible : true;
    }
    chartBuilderChart.update('none');
}

async function loadChartBuilder() {
    const statusEl = document.getElementById('cbStatus');
    const titleEl = document.getElementById('cbChartTitle');
    const canvas = document.getElementById('chartBuilderCanvas');
    if (!canvas || !supabase) return;
    const setStatus = (m) => { if (statusEl) statusEl.textContent = m || ''; };
    try {
        const selected = cbSelected.slice(0, 6);
        if (!selected.length) {
            setStatus('Pick at least one zone to start.');
            if (titleEl) titleEl.textContent = 'Select zones to build a chart';
            return;
        }
        setStatus(`Loading ${selected.length} series…`);

        // Fetch each unique metric+psrFilter combination once — avoids duplicate API calls.
        // Pass needEU=true only when 'EU' is among the selected series.
        const needEU = selected.some(s => s.country === 'EU');

        // For generation, pre-compute only the zones that are actually needed.
        // This filters server-side instead of fetching all ~50 zones — critical for intraday speed.
        const genZones = [...new Set(
            selected
                .filter(s => s.metric === 'generation' && s.country !== 'EU')
                .flatMap(s => cbCandidateZonesForCountry(s.country))
        )];

        // Build unique fetch keys: "metric:psrFilter|total"
        const fetchKeys = [...new Set(selected.map(s => `${s.metric}:${s.psrFilter ?? 'total'}`))];

        const fetchedByKey = new Map();
        await Promise.all(fetchKeys.map(async (fkey) => {
            const [metric, filterStr] = fkey.split(':');
            const psrFilter = filterStr === 'total' ? null : filterStr;
            let fetched;
            if (metric === 'demand') fetched = await cbFetchDemandRaw(cbRange, needEU);
            else if (metric === 'generation') fetched = await cbFetchGenerationRaw(cbRange, needEU, psrFilter, genZones);
            else if (metric === 'prices') fetched = await cbFetchPriceRaw(cbRange, needEU);
            else fetched = await cbFetchRenewableRaw(cbRange, needEU);
            fetchedByKey.set(fkey, fetched);
        }));

        // Determine Y-axis assignment: first unit → left 'y', second unit → right 'y1'
        const allUnits = [...new Set([...fetchedByKey.values()].map(f => f?.unit).filter(Boolean))];
        const unitToAxis = new Map([[allUnits[0], 'y']]);
        if (allUnits[1]) unitToAxis.set(allUnits[1], 'y1');

        const datasets = [];

        // Build one dataset per series. EU uses euSeries from the fetch; others use cbRollupCountry.
        selected.forEach((s) => {
            const fkey = `${s.metric}:${s.psrFilter ?? 'total'}`;
            const f = fetchedByKey.get(fkey);
            if (!f) return;
            const yId = unitToAxis.get(f.unit) || 'y';

            if (s.country === 'EU') {
                const euData = f.euSeries || [];
                if (!euData.length) return;
                datasets.push({
                    label: cbSeriesLabel(s),
                    data: euData.map(p => p.y),
                    _ts: euData.map(p => p.ts),
                    _fmt: f.fmt,
                    borderColor: s.color,
                    backgroundColor: s.color + '22',
                    fill: false,
                    pointRadius: 0,
                    tension: 0.2,
                    borderWidth: 2,
                    spanGaps: true,
                    hidden: !s.visible,
                    yAxisID: yId,
                    _cbSeriesId: s.id,
                });
            } else {
                const series = cbRollupCountry(f.points, s.country, f.mode);
                if (!series.length) return;
                datasets.push({
                    label: cbSeriesLabel(s),
                    data: series.map(p => p.y),
                    _ts: series.map(p => p.ts),
                    _fmt: f.fmt,
                    borderColor: s.color,
                    backgroundColor: s.color + '22',
                    fill: false,
                    pointRadius: 0,
                    tension: 0.2,
                    borderWidth: 2,
                    spanGaps: true,
                    hidden: !s.visible,
                    yAxisID: yId,
                    _cbSeriesId: s.id,
                });
            }
        });

        if (!datasets.length) {
            setStatus('No data for selected countries/range.');
            return;
        }

        // Union timestamps and align all series to the same x-axis
        const tsSet = new Set();
        datasets.forEach(ds => ds._ts.forEach(ts => tsSet.add(ts)));
        const tsList = [...tsSet].sort((a, b) => new Date(a) - new Date(b));
        const labels = tsList.map(ts => cbFormatLabel(ts, cbRange));
        datasets.forEach(ds => {
            const map = new Map(ds._ts.map((ts, idx) => [ts, ds.data[idx]]));
            ds.data = tsList.map(ts => map.has(ts) ? map.get(ts) : null);
            delete ds._ts;
        });

        const ctx = canvas.getContext('2d');
        if (!ctx) return;
        const existing = Chart.getChart(canvas);
        if (existing) existing.destroy();
        if (chartBuilderChart) { try { chartBuilderChart.destroy(); } catch (_) {} chartBuilderChart = null; }

        // Build per-axis scale config
        const makeYScale = (unit, position, drawGrid) => {
            const isPercent = unit === '%';
            const fmtFn = (() => {
                const fkey = fetchKeys.find(k => fetchedByKey.get(k)?.unit === unit);
                return fkey ? fetchedByKey.get(fkey).fmt : (v) => String(v);
            })();
            return {
                position,
                grid: { drawOnChartArea: drawGrid },
                beginAtZero: isPercent,
                suggestedMin: isPercent ? 0 : undefined,
                suggestedMax: isPercent ? 100 : undefined,
                ticks: { callback: (v) => fmtFn(Number(v)) },
                title: { display: true, text: unit },
            };
        };

        const scales = {
            x: { type: 'category', ticks: { maxRotation: 0, maxTicksLimit: 12 }, grid: { display: false } },
            y: makeYScale(allUnits[0] || '%', 'left', true),
        };
        if (allUnits[1]) {
            scales.y1 = makeYScale(allUnits[1], 'right', false);
        }

        chartBuilderChart = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (item) => {
                                const v = item.raw;
                                const fmt = item.dataset._fmt || ((x) => String(x));
                                if (v == null || !Number.isFinite(Number(v))) return `${item.dataset.label}: —`;
                                return `${item.dataset.label}: ${fmt(Number(v))}`;
                            },
                        },
                    },
                },
                scales,
            },
        });

        if (titleEl) {
            const seriesLabels = selected.map(s => cbSeriesLabel(s));
            titleEl.textContent = seriesLabels.length <= 4 ? seriesLabels.join(', ') : `${seriesLabels.length} series`;
        }
        setStatus(`Rendered ${datasets.length} series.`);
    } catch (err) {
        console.error('Chart builder failed:', err);
        setStatus(`Failed: ${err.message || String(err)}`);
    }
}

function insertChartBuilderSectionForGasPage() {
    const gasPage = document.getElementById('gasMeterPage');
    // If the gas meter already has its own native "Demand" chart builder tab,
    // don't inject the mirror/CTA block anymore.
    if (!gasPage || document.getElementById('chartBuilderMirrorSection') || document.getElementById('gasDemandEmTab')) return;
    const target = gasPage.querySelector('.section');
    if (!target || !target.parentNode) return;

    const section = document.createElement('div');
    section.className = 'section';
    section.id = 'chartBuilderMirrorSection';
    section.innerHTML = `
        <div class="section-header">
            <h2 class="section-title">Chart builder</h2>
            <div class="section-actions">
                <button class="btn-secondary" id="cbOpenInElectricityBtn">Open in Electricity Meter</button>
            </div>
        </div>
        <p style="color: var(--text-secondary); margin-bottom: 0;">
            Build custom multi-country comparisons (renewables, generation total, demand total, day-ahead prices) in the Electricity Meter “Chart Builder” tab.
        </p>
    `;
    target.parentNode.insertBefore(section, target);
    const openBtn = document.getElementById('cbOpenInElectricityBtn');
    if (openBtn) {
        openBtn.addEventListener('click', () => {
            navigateToPage('energy-meter');
            switchElectricityMeterTab('chart-builder');
        });
    }
}

async function loadPriceTabData(forceRefresh = false) {
    const statusEl = document.getElementById('priceMeterStatus');
    const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg || ''; };
    const container = document.getElementById('priceMapContainer');
    if (!container) return;

    try {
        if (!supabase) throw new Error('Supabase client not initialized.');
        setStatus('Fetching latest prices…');
        container.innerHTML = '<div class="chart-loading">Loading map…</div>';

        // Optional: trigger fresh ingestion (if Edge Function is reachable).
        if (forceRefresh) {
            try {
                await fetch(`${SUPABASE_URL}/functions/v1/entsoe_ingest_price_eu_latest`, {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({ delay_ms: 0, concurrency: 6 }),
                });
            } catch (_) {}
        }

        updatePriceMapWindowButtons();
        const days = priceMapWindow === '30d' ? 30 : 1;
        const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
        // Supabase / PostgREST often caps responses (commonly 1000 rows). Page
        // so late-publishing zones (e.g. IT sometimes lags others) still appear
        // in the map window.
        const byZone = new Map();
        const pageSize = 1000;
        let from = 0;
        let fetched = 0;
        const maxRows = priceMapWindow === '30d' ? 140_000 : 20_000;
        while (true) {
            const to = from + pageSize - 1;
            const { data, error } = await supabase
                .from('electricity_day_ahead_prices')
                .select('zone_id, ts, price_eur_per_mwh')
                .eq('source', 'entsoe')
                .gte('ts', since)
                .order('ts', { ascending: false })
                .range(from, to);
            if (error) throw new Error(error.message);
            const rows = Array.isArray(data) ? data : [];
            fetched += rows.length;
            for (const r of rows) {
                const z = String(r.zone_id || '').toUpperCase();
                const v = Number(r.price_eur_per_mwh);
                if (!z || !Number.isFinite(v)) continue;
                const prev = byZone.get(z) || { sum: 0, n: 0, newest: null };
                prev.sum += v;
                prev.n += 1;
                const t = r.ts ? new Date(r.ts).getTime() : NaN;
                if (Number.isFinite(t) && (!prev.newest || t > prev.newest)) prev.newest = t;
                byZone.set(z, prev);
            }
            if (rows.length < pageSize) break;
            from += pageSize;
            if (fetched >= maxRows) break;
        }
        const latest = [];
        for (const [z, v] of byZone.entries()) {
            latest.push({ zone_id: z, ts: v.newest ? new Date(v.newest).toISOString() : null, price: v.n ? (v.sum / v.n) : null });
        }
        latest.sort((a, b) => String(a.zone_id).localeCompare(String(b.zone_id)));
        priceLatestRows = latest;

        const newestMs = Math.max(0, ...latest.map(r => r.ts ? new Date(r.ts).getTime() : 0));
        document.getElementById('priceLastUpdated').textContent = newestMs ? new Date(newestMs).toLocaleString() : '-';
        document.getElementById('priceZones').textContent = String(latest.length);
        const vals = latest.map(r => Number(r.price)).filter(Number.isFinite);
        const euAvg = vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
        document.getElementById('priceEuAvg').textContent = fmtEurPerMwh(euAvg);
        document.getElementById('priceSelected').textContent = priceSelectedZone ? String(priceSelectedZone) : '-';

        // Use the same clickable EU geo map style as generation/demand tabs.
        renderPriceGeoMap(container, latest).catch((e) => {
            console.warn('Price geo map render failed, falling back to tile grid:', e);
            renderPriceTileGrid(container, latest);
            mapFallbackNote(container, e);
        });
        updatePriceEuRangeButtonActive();
        updatePriceZoneRangeButtonActive();
        await loadPriceEuChart(priceEuRange);
        if (priceSelectedZone) await loadPriceZoneChart(priceSelectedZone, priceZoneRange);
        setStatus('');
    } catch (err) {
        console.error('Price tab failed:', err);
        setStatus(`Failed: ${err.message || String(err)}`);
        container.innerHTML = '<div class="chart-loading">Failed to load prices.</div>';
    }
}

function renderPriceTileGrid(container, rows) {
    const values = rows.map(r => Number(r.price)).filter(Number.isFinite);
    const max = values.length ? Math.max(...values) : 1;
    const min = values.length ? Math.min(...values) : 0;
    const legend = `
        <div class="energy-map-legend">
            <span>Low price</span>
            <div class="energy-map-legend-bar" style="background: linear-gradient(90deg, rgba(34,197,94,0.2), rgba(239,68,68,0.9));"></div>
            <span>High price</span>
        </div>
    `;
    const tiles = rows.map(r => {
        const zone = String(r.zone_id || '');
        const v = Number(r.price);
        const t = Number.isFinite(v) && max > min ? Math.max(0, Math.min(1, (v - min) / (max - min))) : 0;
        const bg = `rgba(${Math.round(34 + t * (239-34))},${Math.round(197 + t * (68-197))},${Math.round(94 + t * (68-94))},${0.18 + t * 0.55})`;
        const isActive = String(priceSelectedZone || '').toUpperCase() === zone.toUpperCase();
        return `
            <div class="energy-map-tile ${isActive ? 'active' : ''}" data-zone="${escapeHtml(zone)}" style="background:${bg}">
                <div class="energy-map-tile-code">${escapeHtml(zone)}</div>
                <div class="energy-map-tile-value">${escapeHtml(Number.isFinite(v) ? fmtEurPerMwh(v) : '—')}</div>
            </div>
        `;
    }).join('');

    container.innerHTML = `${legend}<div class="energy-map-grid">${tiles}</div>`;
    container.querySelectorAll('.energy-map-tile').forEach(el => {
        el.addEventListener('click', () => {
            const z = el.getAttribute('data-zone');
            if (!z) return;
            priceSelectedZone = z;
            document.getElementById('priceSelected').textContent = z;
            updatePriceZoneRangeButtonActive();
            loadPriceZoneChart(z, priceZoneRange);
            container.querySelectorAll('.energy-map-tile').forEach(t => t.classList.remove('active'));
            el.classList.add('active');
        });
    });
}

async function renderPriceGeoMap(container, latestRows) {
    const rows = (latestRows || []).filter(r => r.zone_id && Number.isFinite(Number(r.price)));
    if (!rows.length) throw new Error('No price rows.');

    const countryGeo = await fetchEuropeCountriesGeoJsonOnce();
    const PRICE_NOT_AVAILABLE = {
        GB: 'No ENTSO‑E day-ahead price data for GB (API returns “No matching data found”).',
        IE: 'No ENTSO‑E day-ahead price data for IE (API returns “No matching data found”).',
    };
    const width = 1400;
    const height = 860;
    const padding = 10;
    const bounds = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };

    const byCountry = {};
    for (const r of rows) {
        const iso2 = zoneToCountryIso2(r.zone_id);
        const v = Number(r.price);
        if (!iso2 || !Number.isFinite(v)) continue;
        // If multiple bidding zones map to same ISO2, average them.
        const prev = byCountry[iso2] || { sum: 0, n: 0 };
        prev.sum += v;
        prev.n += 1;
        byCountry[iso2] = prev;
    }
    const values = Object.values(byCountry).map(v => v.n ? v.sum / v.n : NaN).filter(Number.isFinite);
    const min = values.length ? Math.min(...values) : 0;
    const max = values.length ? Math.max(...values) : 1;

    container.innerHTML = `
        <div class="energy-map-shell">
            <div class="energy-map-top">
                <div class="energy-map-top-left">
                    <div class="energy-map-title">Day-ahead electricity price</div>
                    <div class="energy-map-subtitle">${priceMapWindow === '30d' ? 'Last 30d average' : 'Last 24h average'} (click a country to chart)</div>
                </div>
                <div class="energy-map-top-right">
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Selected</div>
                        <div class="energy-map-chip-value">${escapeHtml(priceSelectedZone || '—')}</div>
                    </div>
                </div>
            </div>
            <div class="energy-map-legend energy-map-legend--premium">
                <span>Low</span>
                <div class="energy-map-legend-bar" style="background: linear-gradient(90deg, rgba(34,197,94,0.2), rgba(239,68,68,0.9));"></div>
                <span>High</span>
            </div>
            <div class="energy-map-stage">
                <svg class="energy-geo-map" viewBox="0 0 ${width} ${height}" role="img" aria-label="Day-ahead price map"></svg>
            </div>
        </div>
    `;

    const svg = container.querySelector('svg.energy-geo-map');
    if (!svg) return;

    let tooltip = document.querySelector('.energy-map-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.className = 'energy-map-tooltip';
        tooltip.style.display = 'none';
        document.body.appendChild(tooltip);
    }

    const features = Array.isArray(countryGeo?.features) ? countryGeo.features : [];
    for (const f of features) {
        const iso2 = String(f?.properties?.ISO2 || '').toUpperCase();
        if (!iso2) continue;
        if (iso2 === 'RU' || iso2 === 'BY') continue;

        const dataKey = iso2GeoToDataKey(iso2);
        const naReason = PRICE_NOT_AVAILABLE[dataKey] || null;
        const agg = byCountry[dataKey];
        const v = agg && agg.n ? (agg.sum / agg.n) : null;
        const t = Number.isFinite(v) && max > min ? Math.max(0, Math.min(1, (v - min) / (max - min))) : null;
        const fill = t == null ? NO_DATA_FILL : `rgba(${Math.round(34 + t * (239-34))},${Math.round(197 + t * (68-197))},${Math.round(94 + t * (68-94))},${0.18 + t * 0.55})`;

        const geom = f.geometry;
        if (!geom) continue;
        const type = geom.type;
        const coords = geom.coordinates;

        const paths = [];
        if (type === 'Polygon') {
            paths.push(polygonToPath(coords[0], width, height, bounds, padding));
        } else if (type === 'MultiPolygon') {
            for (const poly of coords) if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
        } else continue;

        const d = paths.join(' ');
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', d);
        path.setAttribute('fill', fill);
        path.setAttribute('data-iso2', iso2);
        path.style.cursor = naReason ? 'not-allowed' : 'pointer';
        if (naReason) {
            path.setAttribute('stroke', 'rgba(100,116,139,0.65)');
            path.setAttribute('stroke-width', '1');
            path.setAttribute('stroke-dasharray', '4 3');
        }

        path.addEventListener('mouseenter', () => {
            tooltip.style.display = 'block';
            if (naReason) tooltip.textContent = `${iso2} — N/A (${naReason})`;
            else tooltip.textContent = `${iso2} — ${Number.isFinite(v) ? fmtEurPerMwh(v) : '—'}`;
        });
        path.addEventListener('mousemove', (e) => {
            tooltip.style.left = `${e.clientX}px`;
            tooltip.style.top = `${e.clientY}px`;
        });
        path.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
        path.addEventListener('click', () => {
            if (naReason) return;
            const picked = pickZoneForCountry(rows.map(r => ({ zone_id: r.zone_id })), iso2);
            // If we can't pick a zone from the price rows (some countries split), fall back to ISO2.
            priceSelectedZone = picked?.zone_id ? String(picked.zone_id) : iso2;
            document.getElementById('priceSelected').textContent = priceSelectedZone;
            updatePriceZoneRangeButtonActive();
            loadPriceZoneChart(priceSelectedZone, priceZoneRange);
        });

        svg.appendChild(path);
    }
}

async function loadPriceEuChart(range) {
    if (priceEuChartLoadInFlight) return await priceEuChartLoadInFlight;
    priceEuChartLoadInFlight = (async () => {
        const statusEl = document.getElementById('priceEuStatus');
        const titleEl = document.getElementById('priceEuChartTitle');
        const canvas = document.getElementById('priceEuChart');
        if (!canvas) return;
        const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg || ''; };

        try {
            if (!supabase) throw new Error('Supabase client not initialized.');
            const since = priceRangeToSinceIso(range);
            const useWeekly = range === '5y';
            const useDaily = range === '6m' || range === '1y';
            const mvTable = useWeekly
                ? 'electricity_eu_price_weekly_mv'
                : useDaily
                ? 'electricity_eu_price_daily_mv'
                : 'electricity_eu_price_hourly_mv';

            setStatus(`Loading EU prices (${range})…`);
            if (titleEl) titleEl.textContent = 'EU — Day-ahead price (€/MWh)';

            const maxPoints = useWeekly ? 400 : useDaily ? 900 : (range === 'month' ? 1200 : 400);
            const { data, error } = await supabase
                .from(mvTable)
                .select('ts, price_eur_per_mwh')
                .gte('ts', since)
                .order('ts', { ascending: false })
                .limit(maxPoints);
            if (error) throw new Error(error.message);

            const rows = (Array.isArray(data) ? data : []).reverse();
            const labels = rows.map(r => {
                const d = new Date(r.ts);
                if (Number.isNaN(d.getTime())) return String(r.ts);
                return (useWeekly || useDaily) ? d.toLocaleDateString() : d.toLocaleString();
            });
            const series = rows.map(r => Number(r.price_eur_per_mwh));

            const ctx = canvas.getContext('2d');
            if (!ctx) return;
            const existing = Chart.getChart(canvas);
            if (existing) existing.destroy();
            if (priceEuChart) { try { priceEuChart.destroy(); } catch (_) {} priceEuChart = null; }

            priceEuChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels,
                    datasets: [{
                        label: 'EU day-ahead price (€/MWh)',
                        data: series,
                        borderColor: '#ef4444',
                        backgroundColor: 'rgba(239, 68, 68, 0.10)',
                        fill: true,
                        tension: 0.25,
                        pointRadius: series.length <= 2 ? 3 : 0,
                        borderWidth: 2,
                    }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { type: 'category', ticks: { maxRotation: 0 }, grid: { display: false } },
                        y: { beginAtZero: false, ticks: { callback: (v) => fmtEurPerMwh(Number(v)) } },
                    },
                },
            });
            setStatus('');
        } catch (err) {
            console.error('EU price chart failed:', err);
            setStatus(`Failed: ${err.message || String(err)}`);
        }
    })();
    try { return await priceEuChartLoadInFlight; }
    finally { priceEuChartLoadInFlight = null; }
}

async function loadPriceZoneChart(zone, range) {
    if (priceZoneChartLoadInFlight) return await priceZoneChartLoadInFlight;
    priceZoneChartLoadInFlight = (async () => {
        const statusEl = document.getElementById('priceZoneStatus');
        const titleEl = document.getElementById('priceZoneChartTitle');
        const canvas = document.getElementById('priceZoneChart');
        if (!canvas) return;
        const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg || ''; };

        try {
            if (!supabase) throw new Error('Supabase client not initialized.');
            const since = priceRangeToSinceIso(range);
            const useWeekly = range === '5y';
            const useDaily = range === '6m' || range === '1y';
            const table = useWeekly ? 'electricity_price_weekly' : useDaily ? 'electricity_price_daily' : 'electricity_day_ahead_prices';
            const valueCol = useWeekly || useDaily ? 'price_eur_per_mwh' : 'price_eur_per_mwh';
            const maxPoints = useWeekly ? 400 : useDaily ? 900 : 800;

            setStatus(`Loading ${zone} prices (${range})…`);
            if (titleEl) titleEl.textContent = `${zone} — Day-ahead price (€/MWh)`;

            const { data, error } = await supabase
                .from(table)
                .select(`ts, ${valueCol}`)
                .eq('zone_id', zone)
                .gte('ts', since)
                .order('ts', { ascending: false })
                .limit(maxPoints);
            if (error) throw new Error(error.message);

            const rows = (Array.isArray(data) ? data : []).reverse();
            const labels = rows.map(r => {
                const d = new Date(r.ts);
                if (Number.isNaN(d.getTime())) return String(r.ts);
                return (useWeekly || useDaily) ? d.toLocaleDateString() : d.toLocaleString();
            });
            const series = rows.map(r => Number(r[valueCol]));

            const ctx = canvas.getContext('2d');
            if (!ctx) return;
            const existing = Chart.getChart(canvas);
            if (existing) existing.destroy();
            if (priceZoneChart) { try { priceZoneChart.destroy(); } catch (_) {} priceZoneChart = null; }

            priceZoneChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels,
                    datasets: [{
                        label: 'Price (€/MWh)',
                        data: series,
                        borderColor: '#dc2626',
                        backgroundColor: 'rgba(220, 38, 38, 0.10)',
                        fill: true,
                        tension: 0.25,
                        pointRadius: series.length <= 2 ? 3 : 0,
                        borderWidth: 2,
                    }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { type: 'category', ticks: { maxRotation: 0 }, grid: { display: false } },
                        y: { beginAtZero: false, ticks: { callback: (v) => fmtEurPerMwh(Number(v)) } },
                    },
                },
            });
            setStatus('');
        } catch (err) {
            console.error('Zone price chart failed:', err);
            setStatus(`Failed: ${err.message || String(err)}`);
        }
    })();
    try { return await priceZoneChartLoadInFlight; }
    finally { priceZoneChartLoadInFlight = null; }
}

async function loadElectricityTabData(forceRefresh = false) {
    const statusEl = document.getElementById('elecMeterStatus');
    const tbody = document.getElementById('elecMeterTableBody');
    const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg || ''; };
    if (!tbody) return;

    try {
        if (!supabase) throw new Error('Supabase client not initialized.');

        setStatus('Fetching latest generation…');
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color: var(--text-secondary); padding: 24px;">Loading...</td></tr>';

        // Pull the last 24 hours of snapshots and extract totalMw / renewableMw via JSONB projection.
        const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
        const { data, error } = await supabase
            .from('energy_mix_snapshots')
            .select('id, zone_id, country_code, ts, source, totalMw:raw->>totalMw, totalMwAlt:raw->>total_mw, renewableMw:raw->>renewableMw, renewableMwAlt:raw->>renewable_mw, euTotalMw:raw->>euTotalMw, euTotalMwAlt:raw->>euTotal_mw')
            .eq('source', 'entsoe')
            .gte('ts', since)
            .order('ts', { ascending: false })
            .limit(2000);
        if (error) throw new Error(error.message);

        const rowsRaw = Array.isArray(data) ? data : [];
        // Normalize raw JSON field naming (some historical loads use snake_case).
        const rows = rowsRaw.map(r => ({
            ...r,
            totalMw: r.totalMw ?? r.totalMwAlt ?? null,
            renewableMw: r.renewableMw ?? r.renewableMwAlt ?? null,
            euTotalMw: r.euTotalMw ?? r.euTotalMwAlt ?? null,
        }));
        // Separate EU aggregate row from per-zone rows.
        const zoneRows = rows.filter(r => (r.zone_id || r.country_code) !== 'EU');
        const euRows = rows.filter(r => (r.zone_id || r.country_code) === 'EU');
        const latestByZone = dedupeLatestByZone(zoneRows);
        latestByZone.sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime());

        elecLatestRows = latestByZone;

        if (!latestByZone.length) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color: var(--text-secondary); padding: 24px;">No data yet. Run ENTSO‑E ingestion.</td></tr>';
            setStatus('No snapshots found.');
            document.getElementById('elecLastUpdated').textContent = '-';
            document.getElementById('elecZones').textContent = '0';
            document.getElementById('elecEuTotal').textContent = '-';
            document.getElementById('elecAvgZone').textContent = '-';
            return;
        }

        // Stats
        const newest = latestByZone.reduce((acc, r) => {
            const t = new Date(r.ts).getTime();
            return Number.isFinite(t) ? Math.max(acc, t) : acc;
        }, 0);
        const totals = latestByZone
            .map(r => Number(r.totalMw))
            .filter(v => Number.isFinite(v));
        const avgZoneMw = totals.length ? totals.reduce((a, b) => a + b, 0) / totals.length : null;
        const latestEu = euRows.length
            ? Number(euRows.sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime())[0].euTotalMw)
            : null;

        document.getElementById('elecLastUpdated').textContent = newest ? new Date(newest).toLocaleString() : '-';
        document.getElementById('elecZones').textContent = String(latestByZone.length);
        document.getElementById('elecEuTotal').textContent = fmtMwShort(latestEu);
        document.getElementById('elecAvgZone').textContent = fmtMwShort(avgZoneMw);

        // Table
        tbody.innerHTML = latestByZone.map(r => {
            const zone = r.zone_id || r.country_code || '-';
            const ts = r.ts;
            const total = Number(r.totalMw);
            const ren = Number(r.renewableMw);
            const src = r.source || '-';
            const tsStr = ts ? new Date(ts).toLocaleString() : '-';
            return `
                <tr class="elec-row" data-zone="${escapeHtml(String(zone))}" data-source="${escapeHtml(String(src))}">
                    <td>${escapeHtml(String(zone))}</td>
                    <td>${escapeHtml(tsStr)}</td>
                    <td>${escapeHtml(Number.isFinite(total) ? Math.round(total).toLocaleString() : '-')}</td>
                    <td>${escapeHtml(Number.isFinite(ren) ? Math.round(ren).toLocaleString() : '-')}</td>
                    <td>${escapeHtml(String(src))}</td>
                </tr>
            `;
        }).join('');

        tbody.querySelectorAll('tr.elec-row').forEach(tr => {
            tr.addEventListener('click', () => {
                const z = tr.getAttribute('data-zone');
                const s = tr.getAttribute('data-source');
                if (z) {
                    elecSelectedZone = z;
                    elecSelectedSource = s || null;
                    loadElecZoneTotalChart(z, elecZoneRange, elecSelectedSource);
                }
            });
        });

        // Default selection: mirror renewable tab's selected zone if set, else FR.
        if (!elecSelectedZone) {
            const fr = latestByZone.find(r => (r.zone_id || r.country_code) === 'FR');
            const pick = (energySelectedZone && latestByZone.find(r => (r.zone_id || r.country_code) === energySelectedZone))
                || fr
                || latestByZone[0];
            if (pick) {
                elecSelectedZone = pick.zone_id || pick.country_code;
                elecSelectedSource = pick.source || 'entsoe';
            }
        }

        // Map
        renderElectricityMap(latestByZone);

        // Charts
        updateElecEuRangeButtonActive();
        updateElecZoneRangeButtonActive();
        await loadElecEuTotalChart(elecEuRange);
        if (elecSelectedZone) await loadElecZoneTotalChart(elecSelectedZone, elecZoneRange, elecSelectedSource);

        setStatus(`Loaded ${latestByZone.length} zones.`);
    } catch (err) {
        console.error('Electricity tab load failed:', err);
        tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color: var(--error-color); padding: 24px;">Failed to load: ${escapeHtml(err.message || String(err))}</td></tr>`;
        if (statusEl) statusEl.textContent = 'Failed to load.';
    }
}

function renderElectricityMap(latestRows) {
    const container = document.getElementById('elecMapContainer');
    if (!container) return;
    const rows = (latestRows || []).filter(r => (r.zone_id || r.country_code) && Number.isFinite(Number(r.totalMw)));
    if (!rows.length) {
        container.innerHTML = '<div class="chart-loading">No ENTSO‑E generation data yet.</div>';
        return;
    }
    renderElectricityGeoMap(container, rows).catch((e) => {
        console.warn('Electricity geo map render failed, falling back to tile grid:', e);
        renderElectricityTileGrid(container, rows);
        mapFallbackNote(container, e);
    });
}

function aggregateZoneMw(rows) {
    // Build map of zone -> totalMw (latest value per zone) and country -> average/sum MW.
    const byZone = {};
    for (const r of rows) {
        const z = String(r.zone_id || r.country_code || '').toUpperCase();
        const mw = Number(r.totalMw);
        if (!z || !Number.isFinite(mw)) continue;
        byZone[z] = mw;
    }
    const byCountry = {};
    for (const r of rows) {
        const iso2 = zoneToCountryIso2(r.zone_id || r.country_code);
        const mw = Number(r.totalMw);
        if (!Number.isFinite(mw)) continue;
        const prev = byCountry[iso2] || { sum: 0, n: 0, latestTs: null };
        prev.sum += mw;
        prev.n += 1;
        const t = r.ts ? new Date(r.ts).getTime() : NaN;
        if (Number.isFinite(t) && (!prev.latestTs || t > prev.latestTs)) prev.latestTs = t;
        byCountry[iso2] = prev;
    }
    const byCountryOut = {};
    Object.entries(byCountry).forEach(([iso2, v]) => {
        // For countries with multiple bidding zones (DK/SE/NO) we sum so the country
        // total reflects actual generation, not an average of zones.
        byCountryOut[iso2] = { mw: v.sum, latestTs: v.latestTs ? new Date(v.latestTs).toISOString() : null };
    });
    return { byZone, byCountry: byCountryOut };
}

function renderElectricityTileGrid(container, rows) {
    const values = rows.map(r => Number(r.totalMw)).filter(Number.isFinite);
    const max = values.length ? Math.max(...values) : 1;
    const legend = `
        <div class="energy-map-legend">
            <span>Low generation</span>
            <div class="energy-map-legend-bar" style="background: linear-gradient(90deg, rgba(219,234,254,1), rgba(29,78,216,1));"></div>
            <span>High generation</span>
        </div>
    `;
    const tiles = rows
        .sort((a, b) => String(a.zone_id || a.country_code).localeCompare(String(b.zone_id || b.country_code)))
        .map(r => {
            const zone = String(r.zone_id || r.country_code);
            const mw = Number(r.totalMw);
            const t = Number.isFinite(mw) && max > 0 ? Math.max(0, Math.min(1, mw / max)) : 0;
            const bg = Number.isFinite(mw) ? gasBlueScale(t) : 'rgba(148,163,184,0.25)';
            const color = Number.isFinite(mw) ? gasBlueTextForBg(t) : 'rgba(15,23,42,0.8)';
            const isActive = elecSelectedZone === zone;
            const val = Number.isFinite(mw) ? fmtMwShort(mw) : '—';
            return `
                <div class="energy-map-tile ${isActive ? 'active' : ''}" data-zone="${escapeHtml(zone)}" style="background:${bg}; color:${color}">
                    <div class="energy-map-tile-code">${escapeHtml(zone)}</div>
                    <div class="energy-map-tile-value">${escapeHtml(val)}</div>
                </div>
            `;
        })
        .join('');
    container.innerHTML = `${legend}<div class="energy-map-grid">${tiles}</div>`;
    container.querySelectorAll('.energy-map-tile').forEach(el => {
        el.addEventListener('click', () => {
            const z = el.getAttribute('data-zone');
            if (!z) return;
            elecSelectedZone = z;
            elecSelectedSource = 'entsoe';
            loadElecZoneTotalChart(z, elecZoneRange, 'entsoe');
            container.querySelectorAll('.energy-map-tile').forEach(t => t.classList.remove('active'));
            el.classList.add('active');
        });
    });
}

async function renderElectricityGeoMap(container, rows) {
    const [countryGeo, zoneGeo] = await Promise.all([
        fetchEuropeCountriesGeoJsonOnce(),
        // .catch here as well as inside: a cosmetic overlay must never be
        // able to reject the Promise.all and take the base map down with it.
        fetchEntsoeZonesGeoJsonOnce().catch(() => null),
    ]);
    const hasZoneOverlay = Array.isArray(zoneGeo?.features) && zoneGeo.features.length > 0;

    const { byZone, byCountry } = aggregateZoneMw(rows);

    // Scale: use max of country-level sums + standalone zones for the base layer;
    // use max of zone values for the DK/SE/NO overlay.
    const countryMax = Math.max(0, ...Object.values(byCountry).map(v => Number(v.mw)).filter(Number.isFinite));
    const zoneMax = Math.max(0, ...Object.values(byZone).map(v => Number(v)).filter(Number.isFinite));

    const width = 1400;
    const height = 860;
    const padding = 10;
    const bounds = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };

    const selectedZone = String(elecSelectedZone || '').toUpperCase();
    const selectedIso2 = zoneToCountryIso2(selectedZone);
    const selectedLabel = selectedZone ? selectedZone : '—';
    const selectedMw =
        selectedZone && Object.prototype.hasOwnProperty.call(byZone, selectedZone)
            ? byZone[selectedZone]
            : (selectedIso2 && byCountry[selectedIso2]?.mw);

    container.innerHTML = `
        <div class="energy-map-shell">
            <div class="energy-map-top">
                <div class="energy-map-top-left">
                    <div class="energy-map-title">Total generation map</div>
                    <div class="energy-map-subtitle">Countries + bidding zones for DK/SE/NO (click to chart)</div>
                </div>
                <div class="energy-map-top-right">
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Selected</div>
                        <div class="energy-map-chip-value">${escapeHtml(selectedLabel)}</div>
                    </div>
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Generation</div>
                        <div class="energy-map-chip-value">${Number.isFinite(selectedMw) ? escapeHtml(fmtMwShort(selectedMw)) : '—'}</div>
                    </div>
                </div>
            </div>
            <div class="energy-map-legend energy-map-legend--premium">
                <span>Low</span>
                <div class="energy-map-legend-bar" style="background: linear-gradient(90deg, rgba(219,234,254,1), rgba(29,78,216,1));"></div>
                <span>High</span>
            </div>
            <div class="energy-map-stage">
                <svg class="energy-geo-map" viewBox="0 0 ${width} ${height}" role="img" aria-label="Total electricity generation map"></svg>
            </div>
        </div>
    `;

    const svg = container.querySelector('svg.energy-geo-map');
    if (!svg) return;

    let tooltip = document.querySelector('.energy-map-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.className = 'energy-map-tooltip';
        tooltip.style.display = 'none';
        document.body.appendChild(tooltip);
    }

    // Base countries (exclude DK/SE/NO/GB; those get zone overlays)
    const countryFeatures = Array.isArray(countryGeo?.features) ? countryGeo.features : [];
    for (const f of countryFeatures) {
        const iso2 = String(f?.properties?.ISO2 || '').toUpperCase();
        if (!iso2) continue;
        if (iso2 === 'RU' || iso2 === 'BY') continue;
        // Only cede these to the bidding-zone overlay if that overlay actually
        // loaded; otherwise DK/SE/NO would be drawn by nobody and vanish.
        if (hasZoneOverlay && (iso2 === 'DK' || iso2 === 'SE' || iso2 === 'NO')) continue;
        // GB rendered via zone overlay below; skip base layer to avoid double-draw
        // GB/UK were dropped from the country layer here because the overlay was
        // meant to draw them. The load, storage and flows maps never did this,
        // which is why the UK appears on those and vanished from these three.
        // Only cede it when the overlay is actually available.
        if (hasZoneOverlay && (iso2 === 'GB' || iso2 === 'UK')) continue;

        const dataKey = iso2GeoToDataKey(iso2);
        const mw = byCountry[dataKey]?.mw;
        const t = Number.isFinite(mw) && countryMax > 0 ? Math.max(0, Math.min(1, mw / countryMax)) : null;
        const fill = t == null ? NO_DATA_FILL : gasBlueScale(t);

        const geom = f.geometry;
        if (!geom) continue;
        const type = geom.type;
        const coords = geom.coordinates;

        const paths = [];
        if (type === 'Polygon') {
            paths.push(polygonToPath(coords[0], width, height, bounds, padding));
        } else if (type === 'MultiPolygon') {
            for (const poly of coords) {
                if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
            }
        } else {
            continue;
        }

        const d = paths.join(' ');
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', d);
        path.setAttribute('fill', fill);
        path.setAttribute('data-iso2', iso2);
        path.style.cursor = 'pointer';
        if (selectedIso2 && iso2GeoMatchesSelection(iso2, selectedIso2)) path.classList.add('is-selected');

        path.addEventListener('mouseenter', () => {
            const v = byCountry[dataKey]?.mw;
            tooltip.style.display = 'block';
            tooltip.textContent = `${iso2} — ${Number.isFinite(v) ? fmtMwShort(v) : '—'}`;
        });
        path.addEventListener('mousemove', (e) => {
            tooltip.style.left = `${e.clientX}px`;
            tooltip.style.top = `${e.clientY}px`;
        });
        path.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
        path.addEventListener('click', () => {
            const picked = pickZoneForCountry(rows, iso2);
            if (!picked) return;
            elecSelectedZone = String(picked.zone_id || picked.country_code);
            elecSelectedSource = 'entsoe';
            loadElecZoneTotalChart(elecSelectedZone, elecZoneRange, 'entsoe');
            renderElectricityGeoMap(container, rows).catch(() => {});
        });

        svg.appendChild(path);
    }

    // Overlay DK/SE/NO/GB bidding zones
    const europeBbox = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };
    const zoneFeaturesAll = Array.isArray(zoneGeo?.features) ? zoneGeo.features : [];
    const overlayZones = new Set(['DK1', 'DK2', 'SE1', 'SE2', 'SE3', 'SE4', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'GB']);
    for (const f of zoneFeaturesAll) {
        const zoneId = normalizeZoneNameToId(f?.properties?.zoneName);
        if (!zoneId || !overlayZones.has(zoneId)) continue;
        const geom = filterGeometryToBbox(f?.geometry, europeBbox);
        if (!geom) continue;

        const mw = byZone[zoneId];
        const t = Number.isFinite(mw) && zoneMax > 0 ? Math.max(0, Math.min(1, mw / zoneMax)) : null;
        const fill = t == null ? NO_DATA_FILL : gasBlueScale(t);

        const type = geom.type;
        const coords = geom.coordinates;
        const paths = [];
        if (type === 'Polygon') {
            paths.push(polygonToPath(coords[0], width, height, bounds, padding));
        } else if (type === 'MultiPolygon') {
            for (const poly of coords) {
                if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
            }
        } else {
            continue;
        }

        const d = paths.join(' ');
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', d);
        path.setAttribute('fill', fill);
        path.setAttribute('data-zone', zoneId);
        path.style.cursor = 'pointer';
        path.classList.add('bz-overlay');
        if (selectedZone && zoneId === selectedZone) path.classList.add('is-selected');

        path.addEventListener('mouseenter', () => {
            const v = byZone[zoneId];
            tooltip.style.display = 'block';
            tooltip.textContent = `${zoneId} — ${Number.isFinite(v) ? fmtMwShort(v) : '—'}`;
        });
        path.addEventListener('mousemove', (e) => {
            tooltip.style.left = `${e.clientX}px`;
            tooltip.style.top = `${e.clientY}px`;
        });
        path.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
        path.addEventListener('click', () => {
            elecSelectedZone = zoneId;
            elecSelectedSource = 'entsoe';
            loadElecZoneTotalChart(elecSelectedZone, elecZoneRange, 'entsoe');
            renderElectricityGeoMap(container, rows).catch(() => {});
        });

        svg.appendChild(path);
    }
}

// ENTSO-E psrType codes grouped into display categories for the stacked chart.
const ELEC_TYPE_GROUPS = [
    { key: 'wind',         label: 'Wind',            types: ['B18', 'B19'], color: '#38bdf8' },
    { key: 'solar',        label: 'Solar',            types: ['B16'],        color: '#fbbf24' },
    { key: 'hydro',        label: 'Hydro',            types: ['B10', 'B11', 'B12'], color: '#2dd4bf' },
    { key: 'nuclear',      label: 'Nuclear',          types: ['B14'],        color: '#a78bfa' },
    { key: 'gas',          label: 'Gas',              types: ['B04'],        color: '#fb923c' },
    { key: 'coal',         label: 'Coal',             types: ['B02', 'B05'], color: '#78716c' },
    { key: 'biomass',      label: 'Biomass/Waste',    types: ['B01', 'B17'], color: '#4ade80' },
    { key: 'other_ren',    label: 'Other renew.',     types: ['B09', 'B13', 'B15'], color: '#86efac' },
    { key: 'other_fossil', label: 'Oil/Other fossil', types: ['B03', 'B06', 'B07', 'B08'], color: '#d97706' },
    { key: 'other',        label: 'Other',            types: ['B20'],        color: '#94a3b8' },
];

// Transform flat rows [{ts, psr_type, <valueCol>}] into Chart.js stacked datasets.
function buildElecTypeDatasets(rows, valueCol) {
    // Collect unique sorted timestamps
    const tsSet = new Set();
    for (const r of rows) { if (r.ts) tsSet.add(String(r.ts)); }
    const timestamps = Array.from(tsSet).sort();

    // Build lookup ts → { psr_type → value }
    const lookup = Object.create(null);
    for (const r of rows) {
        if (!r.ts) continue;
        const k = String(r.ts);
        if (!lookup[k]) lookup[k] = Object.create(null);
        const v = Number(r[valueCol]);
        if (Number.isFinite(v)) lookup[k][r.psr_type] = (lookup[k][r.psr_type] || 0) + v;
    }

    const datasets = [];
    for (const g of ELEC_TYPE_GROUPS) {
        const data = timestamps.map(ts => {
            const byType = lookup[ts] || {};
            const sum = g.types.reduce((acc, t) => acc + (byType[t] || 0), 0);
            return sum > 0 ? sum : null;
        });
        if (data.some(v => v != null)) {
            datasets.push({
                label: g.label,
                data,
                backgroundColor: g.color + 'cc',
                borderColor: g.color,
                fill: true,
                pointRadius: 0,
                tension: 0.2,
                borderWidth: 1,
                stack: 'gen',
                spanGaps: true,
            });
        }
    }
    return { timestamps, datasets };
}

/**
 * EU generation by type — fetches from generation MWh MVs (daily/weekly)
 * or the 15-min EU aggregate MV for the "day" range.
 */
async function elecFetchEuTotalSeries(range) {
    const since = euRangeToSinceIso(range);
    const useWeekly = range === '5y';
    const use15m = range === 'day' || range === 'week';
    const table = useWeekly ? 'electricity_eu_generation_weekly_mwh'
                : use15m    ? 'electricity_eu_generation_15m_mv'
                :              'electricity_eu_generation_daily_mwh';
    const valueCol = use15m ? 'mw' : 'production_mwh';

    const rows = await gasFetchAllPaged(() =>
        supabase
            .from(table)
            .select(`ts, psr_type, ${valueCol}`)
            .gte('ts', since)
            .order('ts', { ascending: true })
    );
    return { rows, valueCol, labelDaily: !use15m };
}

async function elecFetchZoneTotalSeries(zone, range, source) {
    const since = rangeToSinceIso(range);
    const useWeekly = range === '5y';
    const use15m = range === 'day' || range === 'week';
    const table = useWeekly ? 'electricity_generation_weekly_mwh'
                : use15m    ? 'electricity_generation_snapshots'
                :              'electricity_generation_daily_mwh';
    const valueCol = use15m ? 'mw' : 'production_mwh';

    const rows = await gasFetchAllPaged(() => {
        let q = supabase
            .from(table)
            .select(`ts, psr_type, ${valueCol}`)
            .eq('zone_id', zone)
            .gte('ts', since)
            .order('ts', { ascending: true });
        if (source && use15m) q = q.eq('source', source);
        return q;
    });
    return { rows, valueCol, labelDaily: !use15m };
}

function groupGenerationByPeriod(rows, valueCol, period) {
    const grouped = new Map();
    for (const r of rows) {
        if (!r.ts || !r.psr_type) continue;
        const key = tsPeriodKey(r.ts, period);
        if (!grouped.has(key)) grouped.set(key, new Map());
        const byType = grouped.get(key);
        const v = Number(r[valueCol]);
        if (Number.isFinite(v)) byType.set(r.psr_type, (byType.get(r.psr_type) || 0) + v);
    }
    return grouped;
}

async function loadElecEuTotalChart(range) {
    if (elecEuChartLoadInFlight) return await elecEuChartLoadInFlight;

    elecEuChartLoadInFlight = (async () => {
        const statusEl = document.getElementById('elecEuStatus');
        const titleEl = document.getElementById('elecEuChartTitle');
        const canvas = document.getElementById('elecEuChart');
        if (!canvas) return;
        const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg || ''; };

        try {
            if (!supabase) throw new Error('Supabase client not initialized.');

            if (elecEuMode === 'aggregate') {
                await renderGenAggChart('elecEu', null, range, canvas, titleEl, setStatus);
                return;
            }

            const isMwh = range !== 'day';
            const unit = isMwh ? 'GWh' : 'MW';
            const fmt = isMwh ? fmtGWh : fmtMwShort;

            setStatus(`Loading EU generation by type (${range})…`);
            if (titleEl) titleEl.textContent = `EU — Electricity generation by type (${unit})`;

            const { rows, valueCol, labelDaily } = await elecFetchEuTotalSeries(range);
            const { timestamps, datasets } = buildElecTypeDatasets(rows, valueCol);

            const labels = timestamps.map(ts => {
                const d = new Date(ts);
                return Number.isNaN(d.getTime()) ? ts : (labelDaily ? d.toLocaleDateString() : d.toLocaleString());
            });

            if (!timestamps.length) {
                setStatus('No generation data yet. Run electricity_generation_mwh.sql then refresh MVs.');
            } else {
                const lastTotal = datasets.reduce((s, ds) => s + (Number(ds.data[ds.data.length - 1]) || 0), 0);
                setStatus(`Latest total: ${fmt(lastTotal)}`);
            }

            const ctx = canvas.getContext('2d');
            if (!ctx) return;
            const existing = Chart.getChart(canvas);
            if (existing) existing.destroy();
            if (elecEuChart) { try { elecEuChart.destroy(); } catch (_) {} elecEuChart = null; }

            elecEuChart = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: {
                        legend: { position: 'top', labels: { boxWidth: 12, font: { size: 11 } } },
                        tooltip: {
                            backgroundColor: 'rgba(15, 23, 42, 0.92)',
                            titleColor: '#fff',
                            bodyColor: '#fff',
                            padding: 10,
                            filter: (item) => item.raw != null && item.raw > 0,
                            callbacks: {
                                label: (item) => {
                                    const total = item.chart.data.datasets.reduce((s, ds) => s + (Number(ds.data[item.dataIndex]) || 0), 0);
                                    const pct = total > 0 ? ` (${((Number(item.raw) / total) * 100).toFixed(1)}%)` : '';
                                    return `${item.dataset.label}: ${fmt(Number(item.raw))}${pct}`;
                                },
                                footer: (items) => {
                                    const total = items.reduce((s, i) => s + (Number(i.raw) || 0), 0);
                                    return total > 0 ? `Total: ${fmt(total)}` : '';
                                },
                            },
                        },
                    },
                    scales: {
                        x: { type: 'category', ticks: { maxRotation: 0, maxTicksLimit: 10 }, grid: { display: false } },
                        y: {
                            stacked: true,
                            beginAtZero: true,
                            ticks: { callback: (v) => fmt(Number(v)) },
                            grid: { color: 'rgba(148, 163, 184, 0.25)' },
                        },
                    },
                },
            });
        } catch (err) {
            console.error('Electricity EU chart load failed:', err);
            if (statusEl) statusEl.textContent = `Failed: ${err.message || String(err)}`;
        }
    })();

    try { return await elecEuChartLoadInFlight; }
    finally { elecEuChartLoadInFlight = null; }
}

async function loadElecZoneTotalChart(zone, range, source = null) {
    if (elecZoneChartLoadInFlight) return await elecZoneChartLoadInFlight;

    elecZoneChartLoadInFlight = (async () => {
        const statusEl = document.getElementById('elecZoneStatus');
        const titleEl = document.getElementById('elecZoneChartTitle');
        const canvas = document.getElementById('elecZoneChart');
        if (!canvas) return;
        const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg || ''; };

        try {
            if (!supabase) throw new Error('Supabase client not initialized.');

            if (elecZoneMode === 'aggregate') {
                await renderGenAggChart('elecZone', zone, range, canvas, titleEl, setStatus, source);
                return;
            }

            const isMwh = range !== 'day';
            const unit = isMwh ? 'GWh' : 'MW';
            const fmt = isMwh ? fmtGWh : fmtMwShort;

            setStatus(`Loading ${zone} generation by type (${range})…`);
            if (titleEl) titleEl.textContent = `${zone} — Generation by type (${unit})`;

            const { rows, valueCol, labelDaily } = await elecFetchZoneTotalSeries(zone, range, source);
            const { timestamps, datasets } = buildElecTypeDatasets(rows, valueCol);

            const labels = timestamps.map(ts => {
                const d = new Date(ts);
                return Number.isNaN(d.getTime()) ? ts : (labelDaily ? d.toLocaleDateString() : d.toLocaleString());
            });

            if (!timestamps.length) {
                setStatus(`No generation data for ${zone} in selected range.`);
            } else {
                const lastTotal = datasets.reduce((s, ds) => s + (Number(ds.data[ds.data.length - 1]) || 0), 0);
                setStatus(`Latest total: ${fmt(lastTotal)}`);
            }

            const ctx = canvas.getContext('2d');
            if (!ctx) return;
            const existing = Chart.getChart(canvas);
            if (existing) existing.destroy();
            if (elecZoneChart) { try { elecZoneChart.destroy(); } catch (_) {} elecZoneChart = null; }

            elecZoneChart = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: {
                        legend: { position: 'top', labels: { boxWidth: 12, font: { size: 11 } } },
                        tooltip: {
                            backgroundColor: 'rgba(15, 23, 42, 0.92)',
                            titleColor: '#fff',
                            bodyColor: '#fff',
                            padding: 10,
                            filter: (item) => item.raw != null && item.raw > 0,
                            callbacks: {
                                label: (item) => {
                                    const total = item.chart.data.datasets.reduce((s, ds) => s + (Number(ds.data[item.dataIndex]) || 0), 0);
                                    const pct = total > 0 ? ` (${((Number(item.raw) / total) * 100).toFixed(1)}%)` : '';
                                    return `${item.dataset.label}: ${fmt(Number(item.raw))}${pct}`;
                                },
                                footer: (items) => {
                                    const total = items.reduce((s, i) => s + (Number(i.raw) || 0), 0);
                                    return total > 0 ? `Total: ${fmt(total)}` : '';
                                },
                            },
                        },
                    },
                    scales: {
                        x: { type: 'category', ticks: { maxRotation: 0, maxTicksLimit: 10 }, grid: { display: false } },
                        y: {
                            stacked: true,
                            beginAtZero: true,
                            ticks: { callback: (v) => fmt(Number(v)) },
                            grid: { color: 'rgba(148, 163, 184, 0.25)' },
                        },
                    },
                },
            });
        } catch (err) {
            console.error('Electricity zone chart load failed:', err);
            if (statusEl) statusEl.textContent = `Failed: ${err.message || String(err)}`;
        }
    })();

    try { return await elecZoneChartLoadInFlight; }
    finally { elecZoneChartLoadInFlight = null; }
}

async function renderGenAggChart(chartVar, zone, period, canvas, titleEl, setStatus, source) {
    setStatus(`Loading generation aggregate (${period})…`);
    const since = aggSincePeriod(period);
    const isEu = !zone;

    let rows;
    if (isEu) {
        const { data, error } = await supabase.from('electricity_eu_generation_daily_mwh')
            .select('ts, psr_type, production_mwh').gte('ts', since).order('ts', { ascending: true }).limit(10000);
        if (error) throw new Error(error.message);
        rows = Array.isArray(data) ? data : [];
    } else {
        rows = await gasFetchAllPaged(() => {
            let q = supabase.from('electricity_generation_daily_mwh')
                .select('ts, psr_type, production_mwh').eq('zone_id', zone).gte('ts', since).order('ts', { ascending: true });
            return q;
        }, 1000, 50_000);
    }

    const grouped = groupGenerationByPeriod(rows, 'production_mwh', period);
    const keys = [...grouped.keys()].sort();
    const labels = keys.map(k => tsPeriodLabel(k, period));
    const title = isEu ? `EU — Generation by type per ${period} (GWh)` : `${zone} — Generation by type per ${period} (GWh)`;
    if (titleEl) titleEl.textContent = title;
    setStatus('');

    const ctx = canvas.getContext('2d');
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();
    if (chartVar === 'elecEu') { if (elecEuChart) { try { elecEuChart.destroy(); } catch(_) {} elecEuChart = null; } }
    else { if (elecZoneChart) { try { elecZoneChart.destroy(); } catch(_) {} elecZoneChart = null; } }

    const datasets = [];
    for (const g of ELEC_TYPE_GROUPS) {
        const data = keys.map(k => {
            const byType = grouped.get(k) || new Map();
            const sum = g.types.reduce((acc, t) => acc + (byType.get(t) || 0), 0);
            return sum > 0 ? sum : null; // MWh, fmtGWh handles display
        });
        if (data.some(v => v != null)) {
            datasets.push({ label: g.label, data, backgroundColor: g.color + 'cc', borderColor: g.color, borderWidth: 1, stack: 'gen' });
        }
    }

    const chart = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, font: { size: 11 } } },
                tooltip: {
                    backgroundColor: 'rgba(15,23,42,0.92)', titleColor: '#fff', bodyColor: '#fff', padding: 10,
                    filter: (item) => item.raw != null && item.raw > 0,
                    callbacks: {
                        label: (item) => {
                            const total = item.chart.data.datasets.reduce((s, ds) => s + (Number(ds.data[item.dataIndex]) || 0), 0);
                            const pct = total > 0 ? ` (${((Number(item.raw) / total) * 100).toFixed(1)}%)` : '';
                            return `${item.dataset.label}: ${fmtGWh(Number(item.raw))}${pct}`;
                        },
                        footer: (items) => {
                            const total = items.reduce((s, i) => s + (Number(i.raw) || 0), 0);
                            return total > 0 ? `Total: ${fmtGWh(total)}` : '';
                        },
                    },
                },
            },
            scales: {
                x: { type: 'category', stacked: true, ticks: { maxRotation: 45, maxTicksLimit: 20 }, grid: { display: false } },
                y: { stacked: true, beginAtZero: true, ticks: { callback: v => fmtGWh(Number(v)) }, grid: { color: 'rgba(148,163,184,0.25)' } },
            },
        },
    });
    if (chartVar === 'elecEu') elecEuChart = chart;
    else elecZoneChart = chart;
}

// Bucket raw {ts, y} points into day/week averages. When bucket is null, returns input unchanged.
function bucketPoints(points, bucket) {
    if (!bucket || !points.length) return points;
    const keyer = (ts) => {
        const d = new Date(ts);
        if (Number.isNaN(d.getTime())) return String(ts);
        if (bucket === 'day') {
            return new Date(d.getFullYear(), d.getMonth(), d.getDate()).toISOString();
        }
        if (bucket === 'week') {
            // ISO week: snap to Monday 00:00 UTC
            const day = (d.getUTCDay() + 6) % 7; // Mon=0..Sun=6
            const monday = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - day));
            return monday.toISOString();
        }
        return String(ts);
    };
    const agg = new Map();
    for (const p of points) {
        const k = keyer(p.ts);
        const prev = agg.get(k) || { sum: 0, n: 0 };
        prev.sum += Number(p.y);
        prev.n += 1;
        agg.set(k, prev);
    }
    const out = Array.from(agg.entries())
        .map(([ts, v]) => ({ ts, y: v.n ? v.sum / v.n : 0 }))
        .sort((a, b) => new Date(a.ts).getTime() - new Date(b.ts).getTime());
    return out;
}

// Drop a single-point, extreme outlier (e.g. the recurring "15th spike") so charts remain readable.
// This is display-only and does not modify stored data.
function elecDropExtremeSpike(points) {
    const pts = Array.isArray(points) ? points.slice() : [];
    if (pts.length < 8) return pts;
    const ys = pts.map(p => Number(p.y)).filter(Number.isFinite).sort((a, b) => a - b);
    if (ys.length < 8) return pts;
    const median = ys[Math.floor(ys.length / 2)];
    if (!Number.isFinite(median) || median <= 0) return pts;

    const out = [];
    for (let i = 0; i < pts.length; i++) {
        const y = Number(pts[i].y);
        if (!Number.isFinite(y)) continue;
        const prev = i > 0 ? Number(pts[i - 1].y) : NaN;
        const next = i + 1 < pts.length ? Number(pts[i + 1].y) : NaN;
        const neighborMax = Math.max(
            Number.isFinite(prev) ? prev : -Infinity,
            Number.isFinite(next) ? next : -Infinity
        );
        const neighborOk = Number.isFinite(neighborMax) && neighborMax > 0;
        const isSpike = y > median * 2.5 && (!neighborOk || y > neighborMax * 1.8);
        if (isSpike) continue;
        out.push(pts[i]);
    }
    return out.length ? out : pts;
}

// =========================
// Chart info modal — methodology & data source descriptions
// =========================

const CHART_INFO = {
    energyEuChart: {
        title: 'EU Renewable Share — Methodology & Source',
        html: `
            <p><strong>Data source:</strong> <a href="https://transparency.entsoe.eu" target="_blank" rel="noopener">ENTSO-E Transparency Platform</a> — Document type A75 (Actual Generation Per Production Type), process type A16 (Realised).</p>
            <p><strong>Coverage:</strong> All EU member states plus Norway, Switzerland, and the United Kingdom (GB bidding zone). Each country or bidding zone is queried individually using its official EIC code.</p>
            <p><strong>Renewable share (%):</strong> Computed as the ratio of renewable generation to total generation at the time of the latest available interval. Renewable PSR types included: B01 (Biomass), B09 (Geothermal), B11 (Hydro Run-of-River), B12 (Hydro Reservoir), B13 (Marine), B15 (Other Renewable), B16 (Solar), B17 (Wind Offshore), B18 (Wind Onshore), B19 (Waste).</p>
            <p><strong>Update frequency:</strong> Hourly (data ingested at :12 past each hour).</p>
            <p><strong>Aggregation:</strong> EU aggregate is the sum of renewable MW / sum of total MW across all reporting zones at the latest available timestamp.</p>
        `,
    },
    energyFranceChart: {
        title: 'Zone Renewable Share — Methodology & Source',
        html: `
            <p><strong>Data source:</strong> <a href="https://transparency.entsoe.eu" target="_blank" rel="noopener">ENTSO-E Transparency Platform</a> — Document type A75 (Actual Generation Per Production Type).</p>
            <p><strong>Coverage:</strong> Individual bidding zone selected on the map or table. For France (FR), the data is sourced from RTE France via ENTSO-E.</p>
            <p><strong>Renewable share (%):</strong> Same renewable PSR type definition as the EU aggregate chart. The percentage is the ratio of renewable MW to total MW at each reported interval.</p>
            <p><strong>Historical data:</strong> Day/week views use raw 15-minute or hourly snapshots. Month/6m/1y views aggregate to daily averages. 5-year view uses weekly averages.</p>
        `,
    },
    elecEuChart: {
        title: 'EU Electricity Generation by Type — Methodology & Source',
        html: `
            <p><strong>Data source:</strong> <a href="https://transparency.entsoe.eu" target="_blank" rel="noopener">ENTSO-E Transparency Platform</a> — Document type A75 (Actual Generation Per Production Type).</p>
            <p><strong>Coverage:</strong> EU member states plus Norway, Switzerland, and the United Kingdom.</p>
            <p><strong>PSR types shown:</strong> Nuclear (B14), Hard Coal (B05), Natural Gas (B04 + B07), Hydro (B11 + B12), Wind Onshore (B18), Wind Offshore (B17), Solar (B16), Oil (B06), Biomass (B01 + B15), Other (remaining types).</p>
            <p><strong>Unit:</strong> Megawatts (MW) for Day/Week ranges; GWh per day or per week for longer ranges (computed as avg MW × 24h).</p>
            <p><strong>Update frequency:</strong> Hourly ingestion. Day/week ranges show raw 15-minute or hourly resolution; longer ranges use pre-aggregated daily/weekly materialized views.</p>
        `,
    },
    elecZoneChart: {
        title: 'Zone Electricity Generation by Type — Methodology & Source',
        html: `
            <p><strong>Data source:</strong> <a href="https://transparency.entsoe.eu" target="_blank" rel="noopener">ENTSO-E Transparency Platform</a> — Document type A75, per bidding zone.</p>
            <p><strong>Coverage:</strong> Individual zone selected on the map or table. For multi-zone countries (Denmark, Sweden, Norway) the zone breakdown is shown separately.</p>
            <p><strong>PSR types:</strong> Same generation type breakdown as the EU chart. The stacked area chart shows the contribution of each technology to the total at each point in time.</p>
            <p><strong>Historical data:</strong> Day/week views use 15-minute granularity to reveal daily patterns (night vs. day, baseload vs. peak). Month and longer ranges aggregate to daily GWh.</p>
        `,
    },
    loadEuChart: {
        title: 'EU Electricity Demand — Methodology & Source',
        html: `
            <p><strong>Data source:</strong> <a href="https://transparency.entsoe.eu" target="_blank" rel="noopener">ENTSO-E Transparency Platform</a> — Document type A65 (System Total Load, Actual), process type A16.</p>
            <p><strong>Coverage:</strong> All reporting ENTSO-E zones (EU + EEA + UK). The EU aggregate is the sum of actual load across all zones at each timestamp.</p>
            <p><strong>Unit:</strong> MW for day/week ranges; GWh per day or per week for longer ranges (computed as avg load MW × 24h using uniform ENTSO-E reporting intervals).</p>
            <p><strong>Update frequency:</strong> Hourly ingestion. The daily/weekly MWh materialized views are refreshed automatically after each ingestion run.</p>
            <p><strong>Note:</strong> Load data represents actual consumption including transmission losses, excluding pumped-storage consumption.</p>
        `,
    },
    loadZoneChart: {
        title: 'Zone Electricity Demand — Methodology & Source',
        html: `
            <p><strong>Data source:</strong> <a href="https://transparency.entsoe.eu" target="_blank" rel="noopener">ENTSO-E Transparency Platform</a> — Document type A65 (System Total Load, Actual), per bidding zone.</p>
            <p><strong>Coverage:</strong> Individual zone selected on the map or table.</p>
            <p><strong>Unit:</strong> MW for day/week ranges (raw resolution); GWh per day/week for longer ranges.</p>
            <p><strong>Historical data:</strong> Day/week ranges use raw 15-minute or half-hourly resolution snapshots for intraday patterns. Month and longer ranges use the <code>electricity_load_daily_mwh</code> or <code>electricity_load_weekly_mwh</code> materialized views.</p>
        `,
    },
    gasEuChart: {
        title: 'EU Gas Demand — Methodology & Source',
        html: `
            <p><strong>Data source:</strong> National transmission system operators (TSOs) — data accessed via native TSO APIs and ENTSOG transparency platforms. Countries and sources:</p>
            <ul>
                <li><strong>FR</strong>: GRTgaz (native API)</li>
                <li><strong>AT</strong>: AGGM (native API)</li>
                <li><strong>DE</strong>: THE marketplace (native API)</li>
                <li><strong>DK</strong>: Energinet (native API)</li>
                <li><strong>IE</strong>: Gas Networks Ireland / CSO (native API)</li>
                <li><strong>UK</strong>: National Gas (NTS offtake data)</li>
                <li><strong>PT</strong>: REN DataHub (native API)</li>
                <li><strong>BE, BG, EE, HR, HU, IT, LU, LV, NL, PL, RO, SI</strong>: ENTSOG off-take points</li>
            </ul>
            <p><strong>Methodology:</strong> Bruegel-parity methodology. Total demand = power sector + household/LDZ + industry offtake. Power gas consumption is derived from gas-fired generation (ENTSO-E A75) and an assumed thermal efficiency factor.</p>
            <p><strong>Unit:</strong> GWh per gas day (06:00–06:00 UTC).</p>
            <p><strong>Calibration:</strong> Monthly values are calibrated to Eurostat monthly statistics where available to correct for double-counting or measurement biases in TSO signals.</p>
        `,
    },
    gasCountryChart: {
        title: 'Country Gas Demand — Methodology & Source',
        html: `
            <p><strong>Data source:</strong> National TSO — see EU aggregate chart for source by country.</p>
            <p><strong>Sector breakdown:</strong></p>
            <ul>
                <li><strong>Power:</strong> Gas consumed by gas-fired power plants (derived from ENTSO-E generation data or TSO power offtake signals)</li>
                <li><strong>Household/LDZ:</strong> Low-pressure distribution zone offtake — proxy for residential and small commercial demand</li>
                <li><strong>Industry:</strong> High-pressure industrial offtake</li>
            </ul>
            <p><strong>Data availability:</strong> Varies by country. Not all TSOs publish sector-level breakdowns. Where sector data is unavailable, total demand only is shown.</p>
            <p><strong>Update frequency:</strong> Daily (05:00 UTC, after the previous gas day closes). Data may lag 1–3 days depending on TSO publication schedules.</p>
        `,
    },
    priceEuChart: {
        title: 'EU Day-Ahead Electricity Prices — Methodology & Source',
        html: `
            <p><strong>Data source:</strong> ENTSO‑E Transparency Platform — Day-ahead prices (<code>A44</code>, process <code>A01</code>).</p>
            <p><strong>Coverage:</strong> EU + EEA bidding zones where ENTSO‑E publishes day-ahead prices. Some zones (e.g. GB/IE) may return “No matching data found” for some windows.</p>
            <p><strong>Aggregation:</strong> Simple average across zones per timestamp (unweighted).</p>
            <p><strong>Update frequency:</strong> Hourly ingestion (prices are day-ahead, published daily; intraday availability depends on zone).</p>
            <p><strong>Units:</strong> €/MWh.</p>
        `,
    },
    priceZoneChart: {
        title: 'Zone Day-Ahead Electricity Prices — Methodology & Source',
        html: `
            <p><strong>Data source:</strong> ENTSO‑E Transparency Platform — Day-ahead prices (<code>A44</code>, process <code>A01</code>).</p>
            <p><strong>Zone definition:</strong> Bidding zones (e.g. <code>DE</code>, <code>ES</code>, <code>NO1</code>, <code>SE3</code>).</p>
            <p><strong>Special cases:</strong> Countries split into multiple price areas (e.g. Italy) are averaged across selected areas by timestamp to provide a single country proxy.</p>
            <p><strong>Units:</strong> €/MWh.</p>
        `,
    },
};

function showChartInfo(chartId) {
    const info = CHART_INFO[chartId];
    if (!info) return;
    const modal = document.getElementById('chartInfoModal');
    const title = document.getElementById('chartInfoTitle');
    const body = document.getElementById('chartInfoBody');
    if (!modal || !title || !body) return;
    title.textContent = info.title;
    body.innerHTML = info.html;
    modal.classList.add('active');
}

// ── Chart PNG export ──────────────────────────────────────────────
// Grabs the canvas (which already has the E3G watermark drawn by the
// plugin) and triggers a PNG download.
const CHART_EXPORT_NAMES = {
    energyEuChart:               'e3g-eu-renewable-share.png',
    energyFranceChart:           'e3g-zone-renewable-share.png',
    elecEuChart:                 'e3g-eu-electricity-generation.png',
    elecZoneChart:               'e3g-zone-electricity-generation.png',
    loadEuChart:                 'e3g-eu-electricity-demand.png',
    loadZoneChart:               'e3g-zone-electricity-demand.png',
    gasEuChart:                  'e3g-eu-gas-demand.png',
    gasCountryChart:             'e3g-country-gas-demand.png',
    chartBuilderCanvas:          'e3g-elec-chart-builder.png',
    gasDemandChartBuilderCanvas: 'e3g-gas-chart-builder.png',
};
// Returns [{ color, label }] for chart-builder canvases, [] for others.
function getExportLegendItems(canvasId) {
    if (canvasId === 'chartBuilderCanvas') {
        return cbSelected
            .filter(s => s.visible !== false)
            .map(s => ({ color: s.color, label: cbSeriesLabel(s) }));
    }
    if (canvasId === 'gasDemandChartBuilderCanvas') {
        return gasDemandCbSelected
            .filter(s => s.visible !== false)
            .map(s => ({ color: s.color, label: `${s.displayCode} · ${gasCbMetricTitle(s.metric || 'total')}` }));
    }
    return [];
}

function exportChart(canvasId) {
    const srcCanvas = document.getElementById(canvasId);
    if (!srcCanvas) return;
    const filename = CHART_EXPORT_NAMES[canvasId] || `${canvasId}.png`;
    track('chart_export', { chart: canvasId });

    const legendItems = getExportLegendItems(canvasId);

    // Legend footer metrics
    const LEGEND_PAD_TOP = 14;
    const LEGEND_PAD_BOTTOM = 14;
    const LEGEND_PAD_X = 24;
    const SWATCH_SIZE = 12;
    const SWATCH_R = 3;
    const FONT_SIZE = 13;
    const ITEM_GAP = 28;  // horizontal gap between items
    const SWATCH_TEXT_GAP = 8;

    // Measure legend items to figure out row layout
    const tempCtx = document.createElement('canvas').getContext('2d');
    tempCtx.font = `500 ${FONT_SIZE}px "Inter", system-ui, sans-serif`;
    const itemWidths = legendItems.map(item =>
        SWATCH_SIZE + SWATCH_TEXT_GAP + tempCtx.measureText(item.label).width
    );

    // Pack items into rows that fit srcCanvas.width
    const maxRowW = srcCanvas.width - LEGEND_PAD_X * 2;
    const rows = [];
    let currentRow = [];
    let currentRowW = 0;
    for (let i = 0; i < legendItems.length; i++) {
        const w = itemWidths[i];
        const needed = currentRow.length === 0 ? w : ITEM_GAP + w;
        if (currentRow.length > 0 && currentRowW + needed > maxRowW) {
            rows.push(currentRow);
            currentRow = [i];
            currentRowW = w;
        } else {
            currentRow.push(i);
            currentRowW += needed;
        }
    }
    if (currentRow.length) rows.push(currentRow);

    const ROW_H = FONT_SIZE + 10;
    const legendH = legendItems.length > 0
        ? LEGEND_PAD_TOP + rows.length * ROW_H + LEGEND_PAD_BOTTOM
        : 0;

    const out = document.createElement('canvas');
    out.width  = srcCanvas.width;
    out.height = srcCanvas.height + legendH;
    const ctx = out.getContext('2d');

    // White background
    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, out.width, out.height);
    ctx.drawImage(srcCanvas, 0, 0);

    // Draw legend footer
    if (legendH > 0) {
        // Separator line
        ctx.strokeStyle = '#e5e7eb';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(0, srcCanvas.height);
        ctx.lineTo(out.width, srcCanvas.height);
        ctx.stroke();

        ctx.font = `500 ${FONT_SIZE}px "Inter", system-ui, sans-serif`;
        ctx.textBaseline = 'middle';

        rows.forEach((row, rowIdx) => {
            const rowY = srcCanvas.height + LEGEND_PAD_TOP + rowIdx * ROW_H + ROW_H / 2;
            // Centre the row
            const rowW = row.reduce((sum, idx, pos) => {
                return sum + (pos === 0 ? 0 : ITEM_GAP) + itemWidths[idx];
            }, 0);
            let x = (out.width - rowW) / 2;
            row.forEach((itemIdx, pos) => {
                if (pos > 0) x += ITEM_GAP;
                const item = legendItems[itemIdx];
                // Swatch rounded rect
                ctx.fillStyle = item.color;
                ctx.beginPath();
                if (ctx.roundRect) {
                    ctx.roundRect(x, rowY - SWATCH_SIZE / 2, SWATCH_SIZE, SWATCH_SIZE, SWATCH_R);
                } else {
                    ctx.rect(x, rowY - SWATCH_SIZE / 2, SWATCH_SIZE, SWATCH_SIZE);
                }
                ctx.fill();
                // Label
                ctx.fillStyle = '#374151';
                ctx.fillText(item.label, x + SWATCH_SIZE + SWATCH_TEXT_GAP, rowY);
                x += itemWidths[itemIdx];
            });
        });
    }

    const doDownload = () => {
        const link = document.createElement('a');
        link.download = filename;
        link.href = out.toDataURL('image/png');
        link.click();
    };

    // Use the pre-fetched base64 logo (loaded at startup) to avoid
    // CORS timing issues and canvas taint.
    const dataUrl = window._e3gLogoDataUrl;
    if (!dataUrl) { doDownload(); return; }

    const logo = new Image();
    logo.onload = () => {
        const h = 28;
        const w = (logo.naturalWidth / logo.naturalHeight) * h;
        ctx.save();
        ctx.globalAlpha = 0.65;
        ctx.drawImage(logo, out.width - w - 14, 12, w, h);
        ctx.restore();
        doDownload();
    };
    logo.onerror = doDownload;
    logo.src = dataUrl;
}

// =========================
// EU Gas Meter (v2_bruegel_power_entsoe)
// =========================

const GAS_METHOD_VERSION = 'v2_bruegel_power_entsoe';
const GAS_EU27 = ['AT','BE','BG','HR','CY','CZ','DK','EE','FI','FR','DE','GR','HU','IE','IT','LV','LT','LU','MT','NL','PL','PT','RO','SK','SI','ES','SE'];

const GAS_SECTOR_COLORS = {
    power: '#f59e0b',      // amber
    household: '#3b82f6',  // blue
    industry: '#8b5cf6',   // violet
};

let gasEuRange = '1y';
let gasEuMode = 'intraday';
let gasEuAggRange = 'month';
let gasCountryRange = '1y';
let gasSelectedCountry = null;
let gasEuChart = null;
let gasCountryChart = null;
let gasDemandCbChart = null;
let gasDemandCbChartInFlight = null;
let gasDemandCbRange = 'month';
let gasDemandCbChartType = 'line'; // 'line' | 'area'
let gasDemandCbSelected = []; // [{ id, dbCode, displayCode, metric, color, visible }]
let gasDemandCbComposerMetric = 'total'; // metric active in the composer
let gasDemandCbHasBuilt = false;
let gasDemandCbCandidates = []; // populated in initGasDemandChartBuilderUI

function gasCbDisplayCode(dbCode) {
    // Geo map uses canonical ISO codes; our gas DB uses "UK" and sometimes "GR".
    if (dbCode === 'UK') return 'GB';
    if (dbCode === 'GR') return 'EL';
    return dbCode;
}

// In-memory cache so range buttons (1y / 2y / 5y) flip instantly instead of re-hitting the API.
// We always fetch a 5-year slice once and then filter it client-side per selected range.
// TTL: 10 minutes. `refresh` button invalidates.
const GAS_CACHE_TTL_MS = 10 * 60 * 1000;
const GAS_CACHE_RANGE = '5y';
let gasEuAllRows = null;        // { ts, rows }
let gasEuAllInflight = null;    // Promise to dedupe parallel requests
const gasCountryAllRows = new Map();   // country -> { ts, rows }
const gasCountryAllInflight = new Map(); // country -> Promise

function gasCacheInvalidate() {
    gasEuAllRows = null;
    gasEuAllInflight = null;
    gasCountryAllRows.clear();
    gasCountryAllInflight.clear();
}

function gasCacheFresh(entry) {
    return entry && (Date.now() - entry.ts) < GAS_CACHE_TTL_MS;
}

async function gasFetchEuAll() {
    if (gasCacheFresh(gasEuAllRows)) return gasEuAllRows.rows;
    if (gasEuAllInflight) return gasEuAllInflight;
    const fromDate = gasRangeStartISO(GAS_CACHE_RANGE);
    gasEuAllInflight = (async () => {
        try {
            const rows = await gasFetchAllPaged(
                () => supabase
                    .from('gas_demand_daily')
                    .select('gas_day, country_code, total_mwh, power_mwh, household_mwh, industry_mwh')
                    .eq('method_version', GAS_METHOD_VERSION)
                    .gte('gas_day', fromDate)
                    .order('gas_day', { ascending: true })
            );
            gasEuAllRows = { ts: Date.now(), rows };
            return rows;
        } finally {
            gasEuAllInflight = null;
        }
    })();
    return gasEuAllInflight;
}

async function gasFetchCountryAll(country) {
    const cached = gasCountryAllRows.get(country);
    if (gasCacheFresh(cached)) return cached.rows;
    const existing = gasCountryAllInflight.get(country);
    if (existing) return existing;
    const fromDate = gasRangeStartISO(GAS_CACHE_RANGE);
    const p = (async () => {
        try {
            const rows = await gasFetchAllPaged(
                () => supabase
                    .from('gas_demand_daily')
                    .select('gas_day, total_mwh, power_mwh, household_mwh, industry_mwh, source_total')
                    .eq('method_version', GAS_METHOD_VERSION)
                    .eq('country_code', country)
                    .gte('gas_day', fromDate)
                    .order('gas_day', { ascending: true })
            );
            gasCountryAllRows.set(country, { ts: Date.now(), rows });
            return rows;
        } finally {
            gasCountryAllInflight.delete(country);
        }
    })();
    gasCountryAllInflight.set(country, p);
    return p;
}

function gasRangeStartISO(range) {
    const now = new Date();
    const d = new Date(now);
    if (range === 'month') d.setDate(d.getDate() - 31);
    else if (range === '3m') d.setMonth(d.getMonth() - 3);
    else if (range === '6m') d.setMonth(d.getMonth() - 6);
    else if (range === '1y') d.setFullYear(d.getFullYear() - 1);
    else if (range === '2y') d.setFullYear(d.getFullYear() - 2);
    else if (range === '5y') d.setFullYear(d.getFullYear() - 5);
    else d.setMonth(d.getMonth() - 3);
    return d.toISOString().slice(0, 10);
}

function fmtGWh(mwh) {
    if (mwh == null || !Number.isFinite(Number(mwh))) return '-';
    const gwh = Number(mwh) / 1000;
    if (Math.abs(gwh) >= 1000) return `${(gwh / 1000).toFixed(2)} TWh`;
    if (Math.abs(gwh) >= 10) return `${gwh.toFixed(0)} GWh`;
    return `${gwh.toFixed(1)} GWh`;
}

function gasBlueScale(t) {
    // t in [0,1] → light blue to dark blue
    const tt = Math.max(0, Math.min(1, Number(t) || 0));
    const r = Math.round(lerp(219, 29, tt));
    const g = Math.round(lerp(234, 78, tt));
    const b = Math.round(lerp(254, 216, tt));
    return `rgb(${r}, ${g}, ${b})`;
}

function gasBlueTextForBg(t) {
    return t > 0.45 ? 'rgba(255,255,255,0.95)' : 'rgba(15,23,42,0.92)';
}

function updateGasRangeButtonActive() {
    const euMap = {
        month: 'gasEuRangeMonthBtn',
        '3m': 'gasEuRange3mBtn',
        '6m': 'gasEuRange6mBtn',
        '1y': 'gasEuRange1yBtn',
        '2y': 'gasEuRange2yBtn',
        '5y': 'gasEuRange5yBtn',
    };
    Object.entries(euMap).forEach(([range, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.toggle('active', range === gasEuRange);
    });
    const cMap = {
        month: 'gasRangeMonthBtn',
        '3m': 'gasRange3mBtn',
        '6m': 'gasRange6mBtn',
        '1y': 'gasRange1yBtn',
        '2y': 'gasRange2yBtn',
        '5y': 'gasRange5yBtn',
    };
    Object.entries(cMap).forEach(([range, id]) => {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.toggle('active', range === gasCountryRange);
    });
}

async function gasFetchAllPaged(builder, pageSize = 1000, maxRows = 200_000) {
    // Supabase caps row returns per request; page via .range().
    const out = [];
    let from = 0;
    while (true) {
        const to = from + pageSize - 1;
        const { data, error } = await builder().range(from, to);
        if (error) throw new Error(error.message);
        const rows = Array.isArray(data) ? data : [];
        out.push(...rows);
        if (rows.length < pageSize) break;
        from += pageSize;
        if (from > maxRows) break;
    }
    return out;
}

// Parallel variant: fires `concurrency` pages at once instead of one at a time.
// Dramatically faster for large datasets (e.g. generation intraday) where many
// pages are needed. Falls back cleanly when the dataset ends mid-batch.
async function gasFetchAllPagedParallel(builder, pageSize = 1000, maxRows = 600_000, concurrency = 8) {
    const out = [];
    let from = 0;
    while (from <= maxRows) {
        const offsets = [];
        for (let i = 0; i < concurrency; i++) {
            const off = from + i * pageSize;
            if (off > maxRows) break;
            offsets.push(off);
        }
        const results = await Promise.all(offsets.map(off =>
            builder().range(off, off + pageSize - 1).then(({ data, error }) => {
                if (error) throw new Error(error.message);
                return Array.isArray(data) ? data : [];
            })
        ));
        let done = false;
        for (const rows of results) {
            out.push(...rows);
            if (rows.length < pageSize) { done = true; break; }
        }
        if (done) break;
        from += concurrency * pageSize;
    }
    return out;
}

async function loadGasMeterPage() {
    insertChartBuilderSectionForGasPage();
    const statusEl = document.getElementById('gasMeterStatus');
    const tbody = document.getElementById('gasMeterTableBody');
    const refreshBtn = document.getElementById('gasRefreshBtn');
    const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg || ''; };

    if (!tbody) return;

    const renderLoading = () => {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; color: var(--text-secondary); padding: 24px;">Loading...</td></tr>';
    };

    const bindEu = (btn, range) => {
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            gasEuRange = range;
            updateGasRangeButtonActive();
            loadGasEuAggregateChart(gasEuRange);
        });
    };
    bindEu(document.getElementById('gasEuRangeMonthBtn'), 'month');
    bindEu(document.getElementById('gasEuRange3mBtn'), '3m');
    bindEu(document.getElementById('gasEuRange6mBtn'), '6m');
    bindEu(document.getElementById('gasEuRange1yBtn'), '1y');
    bindEu(document.getElementById('gasEuRange2yBtn'), '2y');
    bindEu(document.getElementById('gasEuRange5yBtn'), '5y');

    const bindEuAgg = (id, range) => {
        const btn = document.getElementById(id);
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => { gasEuAggRange = range; updateGasEuAggRangeButtonActive(); loadGasEuAggregateChart(range); });
    };
    bindEuAgg('gasEuAggWeekBtn', 'week');
    bindEuAgg('gasEuAggMonthBtn', 'month');
    bindEuAgg('gasEuAggYearBtn', 'year');
    const gasEuModeI = document.getElementById('gasEuModeIntradayBtn');
    if (gasEuModeI && !gasEuModeI.dataset.bound) { gasEuModeI.dataset.bound='1'; gasEuModeI.addEventListener('click', () => setGasEuMode('intraday')); }
    const gasEuModeA = document.getElementById('gasEuModeAggBtn');
    if (gasEuModeA && !gasEuModeA.dataset.bound) { gasEuModeA.dataset.bound='1'; gasEuModeA.addEventListener('click', () => setGasEuMode('aggregate')); }
    setChartModePanels('gasEu', gasEuMode);

    const bindCountry = (btn, range) => {
        if (!btn || btn.dataset.bound) return;
        btn.dataset.bound = '1';
        btn.addEventListener('click', () => {
            gasCountryRange = range;
            updateGasRangeButtonActive();
            if (gasSelectedCountry) loadGasCountryChart(gasSelectedCountry, gasCountryRange);
        });
    };
    bindCountry(document.getElementById('gasRangeMonthBtn'), 'month');
    bindCountry(document.getElementById('gasRange3mBtn'), '3m');
    bindCountry(document.getElementById('gasRange6mBtn'), '6m');
    bindCountry(document.getElementById('gasRange1yBtn'), '1y');
    bindCountry(document.getElementById('gasRange2yBtn'), '2y');
    bindCountry(document.getElementById('gasRange5yBtn'), '5y');

    if (refreshBtn && !refreshBtn.dataset.bound) {
        refreshBtn.dataset.bound = '1';
        refreshBtn.addEventListener('click', () => {
            gasCacheInvalidate();
            loadGasMeterPage();
        });
    }

    // Pre-warm the 5-year cache so range buttons (1y/2y/5y) switch instantly.
    // Fires in parallel with the main snapshot fetch.
    if (supabase && !gasCacheFresh(gasEuAllRows)) {
        gasFetchEuAll().catch(err => console.warn('EU cache prewarm failed:', err));
    }

    initGasDemandChartBuilderUI();

    try {
        setStatus('Fetching latest snapshot…');
        renderLoading();

        if (!supabase) throw new Error('Supabase client not initialized.');

        // Each country publishes data at its own cadence (e.g. DE native extractor
        // hits T+1 while ENTSOG-derived values for most of EU27 run on T+2..T+3).
        // Picking a single "latest gas_day" globally would collapse the snapshot
        // to whichever country is freshest today.
        // We therefore fetch ~32 days of data and use it two ways:
        //  - Snapshot table: newest row per country (each at its own best day).
        //  - Map: trailing 30-day sum per country, so one-day publication lag
        //    does not change the colour and cross-country values are comparable.
        const lookbackFrom = (() => {
            const d = new Date();
            d.setUTCDate(d.getUTCDate() - 32);
            return d.toISOString().slice(0, 10);
        })();
        const { data: recentRows, error: recentErr } = await supabase
            .from('gas_demand_daily')
            .select('country_code, gas_day, total_mwh, power_mwh, household_mwh, industry_mwh, source_total, source_split, quality_flag')
            .eq('method_version', GAS_METHOD_VERSION)
            .gte('gas_day', lookbackFrom)
            .order('gas_day', { ascending: false });
        if (recentErr) throw new Error(recentErr.message);

        // Latest per country: prefer the most recent row *with* a total value.
        // Countries for which a new day exists but without a total (null total,
        // e.g. calibration couldn't resolve for a very recent month) would
        // otherwise shadow an older row that does carry a total.
        const latestPerCountry = new Map();
        const latestFallback = new Map();
        for (const row of (recentRows || [])) {
            const cc = row.country_code;
            if (!cc) continue;
            const hasTotal = row.total_mwh != null;
            if (hasTotal) {
                const prev = latestPerCountry.get(cc);
                if (!prev || String(row.gas_day) > String(prev.gas_day)) {
                    latestPerCountry.set(cc, row);
                }
            } else {
                const prev = latestFallback.get(cc);
                if (!prev || String(row.gas_day) > String(prev.gas_day)) {
                    latestFallback.set(cc, row);
                }
            }
        }
        for (const [cc, row] of latestFallback.entries()) {
            if (!latestPerCountry.has(cc)) latestPerCountry.set(cc, row);
        }
        const latestRows = Array.from(latestPerCountry.values())
            .sort((a, b) => Number(b.total_mwh ?? 0) - Number(a.total_mwh ?? 0));
        const latestDay = latestRows.length
            ? latestRows.map(r => String(r.gas_day)).sort().slice(-1)[0]
            : null;
        if (!latestRows.length) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; color: var(--text-secondary); padding: 24px;">No data yet.</td></tr>';
            setStatus('No data found.');
            return;
        }

        const rows = Array.isArray(latestRows) ? latestRows : [];
        const gwh = (v) => (v == null ? '—' : (Number(v) / 1000).toFixed(1));

        const countriesWithData = rows.filter(r => r.total_mwh != null);
        const euTotalMwh = countriesWithData.reduce((s, r) => s + Number(r.total_mwh), 0);
        const euPowerMwh = countriesWithData.reduce((s, r) => s + (r.power_mwh == null ? 0 : Number(r.power_mwh)), 0);
        document.getElementById('gasLastUpdated').textContent = latestDay;
        document.getElementById('gasCountries').textContent = String(countriesWithData.length);
        document.getElementById('gasEuTotal').textContent = `${(euTotalMwh / 1000).toFixed(0)}`;
        document.getElementById('gasPowerShare').textContent = euTotalMwh > 0 ? `${(100 * euPowerMwh / euTotalMwh).toFixed(1)}%` : '-';

        // TTF price stat card — fetch latest 2 rows to show price + day-over-day change.
        fetchTtfPrices((() => { const d = new Date(); d.setUTCDate(d.getUTCDate() - 7); return d.toISOString().slice(0, 10); })())
            .then(ttf => {
                const el = document.getElementById('gasTtfPrice');
                if (!el) return;
                if (!ttf.length) { el.textContent = 'N/A'; return; }
                const latest = ttf.at(-1);
                const prev = ttf.at(-2);
                const price = Number(latest.close_eur_per_mwh).toFixed(2);
                if (prev) {
                    const chg = Number(latest.close_eur_per_mwh) - Number(prev.close_eur_per_mwh);
                    const arrow = chg > 0 ? '▲' : chg < 0 ? '▼' : '—';
                    const color = chg > 0 ? '#dc2626' : chg < 0 ? '#059669' : 'inherit';
                    el.innerHTML = `€${price} <span style="font-size:0.75em;color:${color}">${arrow} ${Math.abs(chg).toFixed(2)}</span>`;
                } else {
                    el.textContent = `€${price}`;
                }
            }).catch(() => {
                const el = document.getElementById('gasTtfPrice');
                if (el) el.textContent = 'N/A';
            });

        tbody.innerHTML = rows.map(r => {
            const c = r.country_code || '-';
            return `
                <tr class="gas-row" data-country="${escapeHtml(String(c))}">
                    <td>${escapeHtml(String(c))}</td>
                    <td>${escapeHtml(String(r.gas_day || '-'))}</td>
                    <td>${escapeHtml(gwh(r.total_mwh))}</td>
                    <td>${escapeHtml(gwh(r.power_mwh))}</td>
                    <td>${escapeHtml(gwh(r.household_mwh))}</td>
                    <td>${escapeHtml(gwh(r.industry_mwh))}</td>
                    <td>${escapeHtml(String(r.source_total || '-'))}</td>
                </tr>
            `;
        }).join('');

        tbody.querySelectorAll('tr.gas-row').forEach(tr => {
            tr.addEventListener('click', () => {
                const c = tr.getAttribute('data-country');
                if (!c) return;
                gasSelectedCountry = c;
                loadGasCountryChart(c, gasCountryRange);
            });
        });

        // Map: trailing 30-day sum per country. This smooths daily publication
        // lag (a country missing the latest one or two days no longer shrinks
        // its colour) and the map values are directly comparable across
        // countries because every country is integrating over the same window.
        const MAP_WINDOW_DAYS = 30;
        const mapWindowEnd = (() => {
            // End = max gas_day present in the fetched data so we're always
            // anchored on "what we actually have" instead of a future date.
            let maxDay = null;
            for (const r of (recentRows || [])) {
                if (r.total_mwh == null) continue;
                const d = String(r.gas_day).slice(0, 10);
                if (!maxDay || d > maxDay) maxDay = d;
            }
            return maxDay;
        })();
        const mapWindowStart = (() => {
            if (!mapWindowEnd) return null;
            const d = new Date(`${mapWindowEnd}T00:00:00Z`);
            d.setUTCDate(d.getUTCDate() - (MAP_WINDOW_DAYS - 1));
            return d.toISOString().slice(0, 10);
        })();
        const mapAgg = new Map();
        if (mapWindowStart && mapWindowEnd) {
            for (const r of (recentRows || [])) {
                if (r.total_mwh == null) continue;
                const day = String(r.gas_day).slice(0, 10);
                if (day < mapWindowStart || day > mapWindowEnd) continue;
                const cc = r.country_code;
                if (!cc) continue;
                const agg = mapAgg.get(cc) || {
                    country_code: cc,
                    total_mwh: 0,
                    power_mwh: 0,
                    household_mwh: 0,
                    industry_mwh: 0,
                    days: 0,
                    first_day: day,
                    last_day: day,
                    power_days: 0,
                    hh_days: 0,
                    ind_days: 0,
                };
                agg.total_mwh += Number(r.total_mwh) || 0;
                if (r.power_mwh != null) { agg.power_mwh += Number(r.power_mwh); agg.power_days++; }
                if (r.household_mwh != null) { agg.household_mwh += Number(r.household_mwh); agg.hh_days++; }
                if (r.industry_mwh != null) { agg.industry_mwh += Number(r.industry_mwh); agg.ind_days++; }
                agg.days++;
                if (day < agg.first_day) agg.first_day = day;
                if (day > agg.last_day) agg.last_day = day;
                mapAgg.set(cc, agg);
            }
        }
        const mapRows = Array.from(mapAgg.values()).map(a => ({
            country_code: a.country_code,
            gas_day: a.last_day,
            total_mwh: a.total_mwh,
            power_mwh: a.power_days ? a.power_mwh : null,
            household_mwh: a.hh_days ? a.household_mwh : null,
            industry_mwh: a.ind_days ? a.industry_mwh : null,
            source_total: `trailing_${a.days}d_sum`,
            _days_in_window: a.days,
            _first_day: a.first_day,
            _last_day: a.last_day,
        })).sort((x, y) => Number(y.total_mwh) - Number(x.total_mwh));
        const gasMapDayEl = document.getElementById('gasMapDay');
        if (gasMapDayEl) {
            gasMapDayEl.textContent = mapWindowStart && mapWindowEnd
                ? `Trailing 30-day sum · ${mapWindowStart} → ${mapWindowEnd} · ${mapRows.length} countries`
                : '';
        }
        renderGasMap(mapRows.length ? mapRows : rows);

        // Default selected country: DE (biggest) then FR, else first row
        if (!gasSelectedCountry) {
            const pick = rows.find(r => r.country_code === 'DE') || rows.find(r => r.country_code === 'FR') || rows[0];
            if (pick) gasSelectedCountry = pick.country_code;
        }

        updateGasRangeButtonActive();

        setStatus(`Loaded ${rows.length} countries.`);

        await Promise.all([
            loadGasEuAggregateChart(gasEuRange),
            gasSelectedCountry ? loadGasCountryChart(gasSelectedCountry, gasCountryRange) : Promise.resolve(),
        ]);
    } catch (err) {
        console.error('Gas meter load failed:', err);
        tbody.innerHTML = `<tr><td colspan="7" style="text-align:center; color: var(--error-color); padding: 24px;">Failed to load: ${escapeHtml(err.message || String(err))}</td></tr>`;
        setStatus('Failed to load.');
    }
}

function gasCbRangeStartISO(range) {
    const d = new Date();
    if (range === 'day') d.setUTCDate(d.getUTCDate() - 1);
    else if (range === 'week') d.setUTCDate(d.getUTCDate() - 7);
    else if (range === 'month') d.setUTCDate(d.getUTCDate() - 30);
    else if (range === '6m') d.setUTCMonth(d.getUTCMonth() - 6);
    else if (range === '1y') d.setUTCFullYear(d.getUTCFullYear() - 1);
    else if (range === '5y') d.setUTCFullYear(d.getUTCFullYear() - 5);
    else d.setUTCDate(d.getUTCDate() - 30);
    return d.toISOString().slice(0, 10);
}

function gasCbMetricTitle(metricId) {
    if (metricId === 'power') return 'Power';
    if (metricId === 'household') return 'Household';
    if (metricId === 'industry') return 'Industry';
    return 'Total demand';
}

function gasCbMetricGwhUnit() {
    return 'GWh/day';
}

function gasCbMetricValueMwh(row, metricId) {
    if (!row) return null;
    if (metricId === 'total') {
        if (row.total_mwh != null) return Number(row.total_mwh);
        const pw = row.power_mwh == null ? 0 : Number(row.power_mwh);
        const hh = row.household_mwh == null ? 0 : Number(row.household_mwh);
        const ind = row.industry_mwh == null ? 0 : Number(row.industry_mwh);
        return pw + hh + ind;
    }
    const col = metricId + '_mwh';
    if (!(col in row)) return null;
    return row[col] == null ? null : Number(row[col]);
}

function gasDemandCbRecolorAndRenderSelected() {
    gasDemandCbSelected = gasDemandCbSelected
        .slice(0, 6)
        .map((s, idx) => ({ ...s, color: cbColor(idx) }));
    const listEl = document.getElementById('gasCbSeriesList');
    const countEl = document.getElementById('gasCbSeriesCount');
    const pickedEl = document.getElementById('gasCbPickedCount');
    if (countEl) countEl.textContent = String(gasDemandCbSelected.length);
    if (pickedEl) pickedEl.textContent = `${gasDemandCbSelected.length} / 6`;
    if (!listEl) return;

    if (!gasDemandCbSelected.length) {
        listEl.innerHTML = '<div class="cb-empty-series">Pick countries below to add series.</div>';
        return;
    }

    listEl.innerHTML = gasDemandCbSelected.map((s) => {
        const hiddenClass = s.visible ? '' : 'is-hidden';
        const metricLabel = gasCbMetricTitle(s.metric || 'total');
        const eyeIcon = s.visible
            ? '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><ellipse cx="6" cy="6" rx="5" ry="3.5" stroke="currentColor" stroke-width="1.2"/><circle cx="6" cy="6" r="1.5" fill="currentColor"/></svg>'
            : '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M1 1L11 11" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><path d="M2.5 4.5C1.8 5 1.3 5.5 1 6c1.2 2 3.2 3.5 5 3.5 1 0 2-.4 2.8-1M4 2.7C4.6 2.3 5.3 2 6 2c1.8 0 3.8 1.5 5 3.5-.3.5-.7 1-1.2 1.4" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>';
        return `
            <div class="cb-series-row ${hiddenClass}" data-gas-series-id="${escapeHtml(s.id)}">
                <div class="cb-swatch" style="background:${s.color};"><div class="cb-swatch-inner"></div></div>
                <div class="cb-series-label">
                    <div class="cb-series-code">${escapeHtml(s.displayCode)}</div>
                    <div class="cb-series-meta">${escapeHtml(metricLabel)}</div>
                </div>
                <div class="cb-series-actions">
                    <button type="button" class="cb-icon-btn cb-series-eye" title="${s.visible ? 'Hide' : 'Show'}">${eyeIcon}</button>
                    <button type="button" class="cb-icon-btn cb-icon-btn-danger cb-series-remove" title="Remove">
                        <svg width="10" height="10" viewBox="0 0 10 10" fill="none"><path d="M1 1L9 9M9 1L1 9" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>
                    </button>
                </div>
            </div>
        `;
    }).join('');

    listEl.querySelectorAll('.cb-series-row').forEach((row) => {
        const sid = row.getAttribute('data-gas-series-id');
        const series = gasDemandCbSelected.find(s => s.id === sid);
        if (!series) return;

        const eyeBtn = row.querySelector('.cb-series-eye');
        if (eyeBtn) {
            eyeBtn.addEventListener('click', () => {
                series.visible = !series.visible;
                gasDemandCbSyncChartVisibility();
                gasDemandCbRecolorAndRenderSelected();
            });
        }

        const removeBtn = row.querySelector('.cb-series-remove');
        if (removeBtn) {
            removeBtn.addEventListener('click', () => {
                gasDemandCbSelected = gasDemandCbSelected.filter(s => s.id !== sid);
                gasDemandCbHasBuilt = false;
                gasDemandCbRecolorAndRenderSelected();
                gasDemandCbRenderCountriesGrid();
                loadGasDemandChartBuilderChart();
            });
        }
    });
}

function gasDemandCbSyncChartVisibility() {
    if (!gasDemandCbChart) return;
    for (const ds of gasDemandCbChart.data.datasets || []) {
        const sid = ds._gasSeriesId;
        if (!sid) continue;
        const series = gasDemandCbSelected.find(s => s.id === sid);
        ds.hidden = series ? !series.visible : true;
    }
    gasDemandCbChart.update('none');
}

function gasDemandCbRenderCountriesGrid() {
    const searchEl = document.getElementById('gasCbCountrySearch');
    const gridEl = document.getElementById('gasCbCountriesGrid');
    const pickedEl = document.getElementById('gasCbPickedCount');
    if (!gridEl) return;

    const q = (searchEl?.value || '').trim().toUpperCase();
    const candidates = (gasDemandCbCandidates || []).filter((c) => {
        if (!q) return true;
        const disp = gasCbDisplayCode(c).toUpperCase();
        const name = (CB_COUNTRY_NAMES[disp] || CB_COUNTRY_NAMES[c] || '').toUpperCase();
        return disp.includes(q) || String(c).toUpperCase().includes(q) || name.includes(q);
    }).sort((a, b) => gasCbDisplayCode(a).localeCompare(gasCbDisplayCode(b)));

    // A country card is "active" when it has a series with the current composer metric
    const activeKey = (dbCode) => `${dbCode}:${gasDemandCbComposerMetric}`;
    const activeSet = new Set(gasDemandCbSelected.map(s => `${s.dbCode}:${s.metric}`));
    const maxReached = gasDemandCbSelected.length >= 6;

    if (pickedEl) pickedEl.textContent = `${gasDemandCbSelected.length} / 6`;

    gridEl.innerHTML = candidates.map((dbCode) => {
        const displayCode = gasCbDisplayCode(dbCode);
        const isActive = activeSet.has(activeKey(dbCode));
        const isDisabled = !isActive && maxReached;
        const name = CB_COUNTRY_NAMES[displayCode] || CB_COUNTRY_NAMES[dbCode] || '';
        return `
            <button type="button"
                    class="cb-country-card ${isActive ? 'active' : ''}"
                    data-gas-db-code="${escapeHtml(dbCode)}"
                    ${isDisabled ? 'disabled' : ''}
                    title="${escapeHtml(name || displayCode)}">
                <span class="cb-country-meat">
                    <span class="cb-country-code">${escapeHtml(displayCode)}</span>
                    <span class="cb-country-name">${escapeHtml(name)}</span>
                </span>
            </button>
        `;
    }).join('');

    gridEl.querySelectorAll('.cb-country-card').forEach((btn) => {
        btn.addEventListener('click', () => {
            const dbCode = btn.getAttribute('data-gas-db-code');
            if (!dbCode) return;
            const key = `${dbCode}:${gasDemandCbComposerMetric}`;
            const existsIdx = gasDemandCbSelected.findIndex(s => `${s.dbCode}:${s.metric}` === key);
            if (existsIdx >= 0) {
                gasDemandCbSelected.splice(existsIdx, 1);
                gasDemandCbHasBuilt = false;
            } else {
                if (gasDemandCbSelected.length >= 6) return;
                const displayCode = gasCbDisplayCode(dbCode);
                gasDemandCbSelected.push({
                    id: `${dbCode}:${gasDemandCbComposerMetric}:${Date.now()}`,
                    dbCode,
                    displayCode,
                    metric: gasDemandCbComposerMetric,
                    color: cbColor(gasDemandCbSelected.length),
                    visible: true,
                });
                gasDemandCbHasBuilt = false;
                track('gas_cb_series', { country: displayCode, metric: gasDemandCbComposerMetric });
            }

            gasDemandCbRecolorAndRenderSelected();
            gasDemandCbRenderCountriesGrid();
            loadGasDemandChartBuilderChart();
        });
    });
}

function initGasDemandChartBuilderUI() {
    const demandPane = document.getElementById('gasDemandEmTab');
    if (!demandPane) return;

    // Avoid duplicating event listeners if the gas page is visited multiple times.
    const alreadyBound = demandPane.dataset.gasCbInitDone === '1';
    demandPane.dataset.gasCbInitDone = '1';

    // Reset builder state on each entry to the gas page.
    gasDemandCbHasBuilt = false;
    if (gasDemandCbChart) {
        try { gasDemandCbChart.destroy(); } catch (_) {}
        gasDemandCbChart = null;
    }

    // Tabs
    const overviewBtn = document.getElementById('gasOverviewTabBtn');
    const storageTabBtn = document.getElementById('gasStorageTabBtn');
    const demandBtn = document.getElementById('gasDemandTabBtn');
    const overviewPane = document.getElementById('gasOverviewEmTab');
    const storagePane = document.getElementById('gasStorageEmTab');
    const demandPaneEl = document.getElementById('gasDemandEmTab');
    if (!alreadyBound && overviewBtn && demandBtn && overviewPane && demandPaneEl) {
        const tabs = [
            { btn: overviewBtn, pane: overviewPane, name: 'overview' },
            { btn: storageTabBtn, pane: storagePane, name: 'storage' },
            { btn: demandBtn, pane: demandPaneEl, name: 'demand' },
        ];
        const setActive = (which) => {
            tabs.forEach(t => {
                if (!t.btn || !t.pane) return;
                const on = t.name === which;
                t.btn.classList.toggle('active', on);
                t.btn.setAttribute('aria-selected', String(on));
                t.pane.classList.toggle('active', on);
            });
            if (which === 'demand') {
                if (!gasDemandCbHasBuilt) loadGasDemandChartBuilderChart();
            } else if (which === 'storage') {
                loadGasStorageTab();
            }
        };

        overviewBtn.addEventListener('click', () => { setSidebarCollapsed(false); setActive('overview'); });
        if (storageTabBtn) storageTabBtn.addEventListener('click', () => { setSidebarCollapsed(false); setActive('storage'); });
        demandBtn.addEventListener('click', () => { setSidebarCollapsed(true); setActive('demand'); });
    }

    // Candidate countries (max 6 selected)
    gasDemandCbCandidates = ['EU27', ...new Set([...GAS_EU27, 'UK'])];

    // Default selection: a "useful" starting point (total demand for 4 countries)
    gasDemandCbSelected = ['DE', 'FR', 'ES', 'UK']
        .filter((c) => gasDemandCbCandidates.includes(c))
        .map((dbCode, idx) => ({
            id: `${dbCode}:total:0`,
            dbCode,
            displayCode: gasCbDisplayCode(dbCode),
            metric: 'total',
            color: cbColor(idx),
            visible: true,
        }));

    // Range buttons
    const rangeBtns = [
        ['day', 'gasCbRangeDayBtn'],
        ['week', 'gasCbRangeWeekBtn'],
        ['month', 'gasCbRangeMonthBtn'],
        ['6m', 'gasCbRange6mBtn'],
        ['1y', 'gasCbRange1yBtn'],
        ['5y', 'gasCbRange5yBtn'],
    ];
    if (!alreadyBound) {
        rangeBtns.forEach(([range, id]) => {
            const btn = document.getElementById(id);
            if (!btn) return;
            btn.addEventListener('click', () => {
                gasDemandCbRange = range;
                track('gas_cb_range', { range });
                rangeBtns.forEach(([_, otherId]) => {
                    const other = document.getElementById(otherId);
                    if (!other) return;
                    other.classList.toggle('active', otherId === id);
                });
                loadGasDemandChartBuilderChart();
            });
        });
    }

    // Metric cards — set the composer metric (what gets added on next country click)
    const metricCards = document.querySelectorAll('#gasDemandEmTab [data-gas-metric]');
    if (!alreadyBound) {
        metricCards.forEach(card => {
            card.addEventListener('click', () => {
                const m = card.getAttribute('data-gas-metric');
                gasDemandCbComposerMetric = m;
                const metricEl = document.getElementById('gasCbMetricSelect');
                if (metricEl) metricEl.value = m;
                metricCards.forEach(c => c.classList.toggle('active', c === card));
                // Re-render country grid to reflect which countries have this metric active
                gasDemandCbRenderCountriesGrid();
            });
        });
    }
    // Sync active metric card on init
    metricCards.forEach(c => c.classList.toggle('active', c.getAttribute('data-gas-metric') === gasDemandCbComposerMetric));

    // Chart type buttons
    const lineBtn = document.getElementById('gasCbChartTypeLineBtn');
    const areaBtn = document.getElementById('gasCbChartTypeAreaBtn');
    if (!alreadyBound && lineBtn && areaBtn) {
        lineBtn.addEventListener('click', () => {
            gasDemandCbChartType = 'line';
            lineBtn.classList.add('active');
            areaBtn.classList.remove('active');
            gasDemandCbHasBuilt = false;
            loadGasDemandChartBuilderChart();
        });
        areaBtn.addEventListener('click', () => {
            gasDemandCbChartType = 'area';
            areaBtn.classList.add('active');
            lineBtn.classList.remove('active');
            gasDemandCbHasBuilt = false;
            loadGasDemandChartBuilderChart();
        });
    }

    // Build button
    const buildBtn = document.getElementById('gasCbBuildBtn');
    if (!alreadyBound && buildBtn) {
        buildBtn.addEventListener('click', () => {
            gasDemandCbHasBuilt = false;
            loadGasDemandChartBuilderChart();
        });
    }

    // Clear button
    const clearBtn = document.getElementById('gasCbClearBtn');
    if (!alreadyBound && clearBtn) {
        clearBtn.addEventListener('click', () => {
            gasDemandCbSelected = [];
            gasDemandCbHasBuilt = false;
            gasDemandCbRecolorAndRenderSelected();
            gasDemandCbRenderCountriesGrid();
            loadGasDemandChartBuilderChart();
        });
    }

    // Country search
    const searchEl = document.getElementById('gasCbCountrySearch');
    if (!alreadyBound && searchEl) {
        searchEl.addEventListener('input', () => gasDemandCbRenderCountriesGrid());
    }

    gasDemandCbRecolorAndRenderSelected();
    gasDemandCbRenderCountriesGrid();
    loadGasDemandChartBuilderChart();
}

async function loadGasDemandChartBuilderChart() {
    if (gasDemandCbChartInFlight) return await gasDemandCbChartInFlight;
    gasDemandCbChartInFlight = (async () => {
    const statusEl = document.getElementById('gasCbStatus');
    const canvas = document.getElementById('gasDemandChartBuilderCanvas');
    const titleEl = document.getElementById('gasCbChartTitle');
    if (!canvas || !supabase) return;

    const setStatus = (m) => { if (statusEl) statusEl.textContent = m || ''; };

    try {
        const selected = gasDemandCbSelected.slice(0, 6);
        const range = gasDemandCbRange;
        const chartType = gasDemandCbChartType;

        if (!selected.length) {
            setStatus('Pick at least one country to start.');
            if (titleEl) titleEl.textContent = 'Demand chart builder';
            return;
        }

        setStatus(`Loading ${selected.length} series…`);
        const since = gasCbRangeStartISO(range);
        const eu27Set = new Set(GAS_EU27);

        // Fetch per-series rows. EU27 is handled specially: sum all EU27 country rows.
        const seriesMaps = new Map(); // `${dbCode}:${metric}` -> Map(day -> gwh)
        const dateSet = new Set();
        await Promise.all(selected.map(async (s) => {
            const key = `${s.dbCode}:${s.metric}`;
            const map = new Map();

            if (s.dbCode === 'EU27') {
                const allRows = await gasFetchEuAll();
                for (const r of allRows || []) {
                    if (!eu27Set.has(r.country_code)) continue;
                    const day = String(r.gas_day || '').slice(0, 10);
                    if (!day || day < since) continue;
                    const mwh = gasCbMetricValueMwh(r, s.metric);
                    if (mwh == null || !Number.isFinite(Number(mwh))) continue;
                    map.set(day, (map.get(day) || 0) + Number(mwh) / 1000);
                    dateSet.add(day);
                }
            } else {
                const rows = await gasFetchCountryAll(s.dbCode);
                for (const r of rows || []) {
                    const day = String(r.gas_day || '').slice(0, 10);
                    if (!day || day < since) continue;
                    const mwh = gasCbMetricValueMwh(r, s.metric);
                    if (mwh == null || !Number.isFinite(Number(mwh))) continue;
                    const gwh = Number(mwh) / 1000;
                    if (!Number.isFinite(gwh)) continue;
                    map.set(day, gwh);
                    dateSet.add(day);
                }
            }

            seriesMaps.set(key, map);
        }));

        const dates = [...dateSet].sort();
        if (!dates.length) {
            setStatus('No data for this range / metric.');
            if (titleEl) titleEl.textContent = `Gas demand — ${range}`;
            return;
        }

        const fmtDay = (day) => `${day.slice(5, 7)}/${day.slice(8, 10)}`;
        const labels = dates.map(fmtDay);
        const datasets = [];

        selected.forEach((s, i) => {
            const key = `${s.dbCode}:${s.metric}`;
            const map = seriesMaps.get(key) || new Map();
            const data = dates.map(d => map.has(d) ? map.get(d) : null);
            const color = s.color || cbColor(i);
            datasets.push({
                label: `${gasCbDisplayCode(s.dbCode)} · ${gasCbMetricTitle(s.metric)}`,
                data,
                borderColor: color,
                backgroundColor: chartType === 'area' ? (color + '22') : 'transparent',
                fill: chartType === 'area',
                pointRadius: 0,
                tension: 0.25,
                borderWidth: 2.2,
                spanGaps: true,
                hidden: !s.visible,
                _gasSeriesId: s.id,
            });
        });

        if (gasDemandCbChart) {
            try { gasDemandCbChart.destroy(); } catch (_) {}
            gasDemandCbChart = null;
        }

        const ctx = canvas.getContext('2d');
        if (!ctx) return;

        gasDemandCbChart = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (item) => {
                                const v = item.raw;
                                if (v == null || !Number.isFinite(Number(v))) return `${item.dataset.label}: —`;
                                return `${item.dataset.label}: ${Number(v).toFixed(1)} GWh`;
                            },
                        },
                    },
                },
                scales: {
                    x: { ticks: { maxTicksLimit: 10 }, grid: { display: false } },
                    y: {
                        beginAtZero: true,
                        title: { display: true, text: 'GWh' },
                        ticks: { callback: (v) => `${Number(v).toFixed(0)}` },
                    },
                },
            },
        });

        if (titleEl) {
            const uniqueMetrics = [...new Set(selected.map(s => s.metric))];
            const metricLabel = uniqueMetrics.length === 1 ? gasCbMetricTitle(uniqueMetrics[0]) : 'Mixed metrics';
            const codes = [...new Set(selected.map(s => gasCbDisplayCode(s.dbCode)))];
            const selTxt = codes.length > 5 ? `${codes.length} countries` : codes.join(', ');
            titleEl.textContent = `${metricLabel} — ${selTxt} · ${range}`;
        }

        setStatus(`Rendered ${datasets.length} series.`);
        gasDemandCbHasBuilt = true;
    } catch (err) {
        console.error('Gas demand chart builder failed:', err);
        setStatus(`Failed: ${err.message || String(err)}`);
    }
    })();
    try { return await gasDemandCbChartInFlight; } finally { gasDemandCbChartInFlight = null; }
}

async function fetchTtfPrices(fromDate) {
    if (!supabase) return [];
    const rows = await gasFetchAllPaged(() =>
        supabase
            .from('gas_price_ttf_daily')
            .select('ts, close_eur_per_mwh')
            .gte('ts', fromDate)
            .order('ts', { ascending: true })
    ).catch(err => { console.warn('TTF price fetch failed:', err.message); return []; });
    return rows.filter(r => r.ts && r.close_eur_per_mwh != null);
}

async function loadGasEuAggregateChart(range) {
    const statusEl = document.getElementById('gasEuStatus');
    const canvas = document.getElementById('gasEuChart');
    const titleEl = document.getElementById('gasEuChartTitle');
    if (!canvas || !supabase) return;
    const setStatus = (m) => { if (statusEl) statusEl.textContent = m || ''; };

    try {
        if (gasEuMode === 'aggregate') {
            await loadGasEuChartAggregate(range, canvas, titleEl, setStatus);
            return;
        }

        const cachedReady = gasCacheFresh(gasEuAllRows);
        setStatus(cachedReady ? `Rendering EU27 (${range})…` : `Loading EU27 aggregate (${range})…`);
        const fromDate = gasRangeStartISO(range);

        const [all, ttfRows] = await Promise.all([
            gasFetchEuAll(),
            fetchTtfPrices(fromDate),
        ]);
        const rows = all.filter(r => String(r.gas_day).slice(0, 10) >= fromDate);

        const by = new Map();
        for (const r of rows) {
            const d = String(r.gas_day).slice(0, 10);
            const agg = by.get(d) || { power: 0, household: 0, industry: 0, anyData: false };
            if (r.power_mwh != null) { agg.power += Number(r.power_mwh); agg.anyData = true; }
            if (r.household_mwh != null) { agg.household += Number(r.household_mwh); agg.anyData = true; }
            if (r.industry_mwh != null) { agg.industry += Number(r.industry_mwh); agg.anyData = true; }
            by.set(d, agg);
        }
        const days = Array.from(by.keys()).sort();
        const power = days.map(d => by.get(d).anyData ? by.get(d).power / 1000 : null);
        const household = days.map(d => by.get(d).anyData ? by.get(d).household / 1000 : null);
        const industry = days.map(d => by.get(d).anyData ? by.get(d).industry / 1000 : null);

        // Align TTF prices to the demand day labels (gas markets skip weekends).
        const ttfByDay = new Map(ttfRows.map(r => [String(r.ts).slice(0, 10), Number(r.close_eur_per_mwh)]));
        const ttfPrices = days.map(d => ttfByDay.get(d) ?? null);
        const hasTtf = ttfRows.length > 0;

        if (titleEl) titleEl.textContent = `EU27 — Gas demand by sector (GWh/day) · ${days[0] || ''} → ${days.at(-1) || ''}`;

        if (gasEuChart) { try { gasEuChart.destroy(); } catch (_) {} }
        gasEuChart = new Chart(canvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: days,
                datasets: [
                    { label: 'Power',     data: power,     backgroundColor: GAS_SECTOR_COLORS.power     + 'cc', borderColor: GAS_SECTOR_COLORS.power,     fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false, yAxisID: 'y' },
                    { label: 'Household', data: household, backgroundColor: GAS_SECTOR_COLORS.household + 'cc', borderColor: GAS_SECTOR_COLORS.household, fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false, yAxisID: 'y' },
                    { label: 'Industry',  data: industry,  backgroundColor: GAS_SECTOR_COLORS.industry  + 'cc', borderColor: GAS_SECTOR_COLORS.industry,  fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false, yAxisID: 'y' },
                    ...(hasTtf ? [{ label: 'TTF price', data: ttfPrices, borderColor: '#dc2626', backgroundColor: 'transparent', fill: false, pointRadius: 0, tension: 0.25, borderWidth: 2, spanGaps: true, yAxisID: 'y1', order: -1 }] : []),
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { position: 'top' },
                    tooltip: {
                        filter: (ctx) => ctx.raw != null,
                        callbacks: {
                            label: (ctx) => {
                                if (ctx.raw == null) return null;
                                if (ctx.dataset.yAxisID === 'y1') return `TTF: ${Number(ctx.raw).toFixed(2)} €/MWh`;
                                const idx = ctx.dataIndex;
                                const demandDatasets = ctx.chart?.data?.datasets?.filter(ds => ds.yAxisID !== 'y1') || [];
                                const total = demandDatasets.reduce((s, ds) => s + (Number(ds.data?.[idx]) || 0), 0);
                                const val = Number(ctx.raw);
                                const pct = total > 0 ? (val / total * 100) : null;
                                return pct != null
                                    ? `${ctx.dataset.label}: ${val.toFixed(0)} GWh (${pct.toFixed(1)}%)`
                                    : `${ctx.dataset.label}: ${val.toFixed(0)} GWh`;
                            },
                            footer: (items) => {
                                const demandVals = items.filter(i => i.raw != null && i.dataset.yAxisID !== 'y1').map(i => Number(i.raw));
                                if (!demandVals.length) return 'No data for this day';
                                return `Total: ${demandVals.reduce((s, v) => s + v, 0).toFixed(0)} GWh`;
                            },
                        },
                    },
                },
                scales: {
                    x: { ticks: { maxTicksLimit: 10 } },
                    y: { stacked: true, title: { display: true, text: 'GWh / day' }, beginAtZero: true },
                    ...(hasTtf ? { y1: { position: 'right', title: { display: true, text: '€/MWh' }, grid: { drawOnChartArea: false }, ticks: { callback: v => `€${v}` } } } : {}),
                },
            },
        });

        setStatus(`EU27: ${days.length} days`);
    } catch (err) {
        console.error('EU gas aggregate failed:', err);
        setStatus(`Failed: ${err.message || err}`);
    }
}

async function loadGasEuChartAggregate(period, canvas, titleEl, setStatus) {
    setStatus(`Loading EU27 gas aggregate by ${period}…`);
    const all = await gasFetchEuAll();
    const eu27Set = new Set(GAS_EU27);

    // Group by (period, sector)
    const byPeriod = new Map();
    for (const r of all || []) {
        if (!eu27Set.has(r.country_code)) continue;
        const key = tsPeriodKey(r.gas_day, period);
        if (!key) continue;
        const agg = byPeriod.get(key) || { power: 0, household: 0, industry: 0, count: 0 };
        if (r.power_mwh != null) agg.power += Number(r.power_mwh);
        if (r.household_mwh != null) agg.household += Number(r.household_mwh);
        if (r.industry_mwh != null) agg.industry += Number(r.industry_mwh);
        agg.count += 1;
        byPeriod.set(key, agg);
    }
    const keys = [...byPeriod.keys()].sort();
    const labels = keys.map(k => tsPeriodLabel(k, period));
    const power = keys.map(k => byPeriod.get(k).power / 1000);
    const household = keys.map(k => byPeriod.get(k).household / 1000);
    const industry = keys.map(k => byPeriod.get(k).industry / 1000);

    if (titleEl) titleEl.textContent = `EU27 — Gas demand by sector per ${period} (GWh)`;
    setStatus('');
    if (gasEuChart) { try { gasEuChart.destroy(); } catch (_) {} }
    gasEuChart = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: 'Power',     data: power,     backgroundColor: GAS_SECTOR_COLORS.power     + 'cc', borderColor: GAS_SECTOR_COLORS.power,     borderWidth: 1, stack: 'sec' },
                { label: 'Household', data: household, backgroundColor: GAS_SECTOR_COLORS.household + 'cc', borderColor: GAS_SECTOR_COLORS.household, borderWidth: 1, stack: 'sec' },
                { label: 'Industry',  data: industry,  backgroundColor: GAS_SECTOR_COLORS.industry  + 'cc', borderColor: GAS_SECTOR_COLORS.industry,  borderWidth: 1, stack: 'sec' },
            ],
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { position: 'top' },
                tooltip: {
                    callbacks: {
                        label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.raw).toFixed(0)} GWh`,
                        footer: (items) => `Total: ${items.reduce((s, i) => s + (Number(i.raw) || 0), 0).toFixed(0)} GWh`,
                    },
                },
            },
            scales: {
                x: { type: 'category', stacked: true, ticks: { maxRotation: 45 }, grid: { display: false } },
                y: { stacked: true, beginAtZero: true, ticks: { callback: v => `${Math.round(Number(v))} GWh` } },
            },
        },
    });
}

async function loadGasCountryChart(country, range) {
    const statusEl = document.getElementById('gasCountryStatus');
    const canvas = document.getElementById('gasCountryChart');
    const titleEl = document.getElementById('gasCountryChartTitle');
    if (!canvas || !supabase) return;
    const setStatus = (m) => { if (statusEl) statusEl.textContent = m || ''; };

    try {
        const cachedReady = gasCacheFresh(gasCountryAllRows.get(country));
        setStatus(cachedReady ? `Rendering ${country} (${range})…` : `Loading ${country} (${range})…`);
        const fromDate = gasRangeStartISO(range);

        const [all, ttfRows] = await Promise.all([
            gasFetchCountryAll(country),
            fetchTtfPrices(fromDate),
        ]);
        const rows = all.filter(r => String(r.gas_day).slice(0, 10) >= fromDate);
        const toGwh = (v) => (v == null ? null : Number(v) / 1000);
        const days = rows.map(r => String(r.gas_day).slice(0, 10));
        const power = rows.map(r => toGwh(r.power_mwh));
        const household = rows.map(r => toGwh(r.household_mwh));
        const industry = rows.map(r => toGwh(r.industry_mwh));

        const ttfByDay = new Map(ttfRows.map(r => [String(r.ts).slice(0, 10), Number(r.close_eur_per_mwh)]));
        const ttfPrices = days.map(d => ttfByDay.get(d) ?? null);
        const hasTtf = ttfRows.length > 0;

        if (titleEl) titleEl.textContent = `${country} — Gas demand by sector (GWh/day) · ${days[0] || ''} → ${days.at(-1) || ''}`;

        if (gasCountryChart) { try { gasCountryChart.destroy(); } catch (_) {} }
        gasCountryChart = new Chart(canvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: days,
                datasets: [
                    { label: 'Power',     data: power,     backgroundColor: GAS_SECTOR_COLORS.power     + 'cc', borderColor: GAS_SECTOR_COLORS.power,     fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false, yAxisID: 'y' },
                    { label: 'Household', data: household, backgroundColor: GAS_SECTOR_COLORS.household + 'cc', borderColor: GAS_SECTOR_COLORS.household, fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false, yAxisID: 'y' },
                    { label: 'Industry',  data: industry,  backgroundColor: GAS_SECTOR_COLORS.industry  + 'cc', borderColor: GAS_SECTOR_COLORS.industry,  fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false, yAxisID: 'y' },
                    ...(hasTtf ? [{ label: 'TTF price', data: ttfPrices, borderColor: '#dc2626', backgroundColor: 'transparent', fill: false, pointRadius: 0, tension: 0.25, borderWidth: 2, spanGaps: true, yAxisID: 'y1', order: -1 }] : []),
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { position: 'top' },
                    tooltip: {
                        filter: (ctx) => ctx.raw != null,
                        callbacks: {
                            label: (ctx) => {
                                if (ctx.raw == null) return null;
                                if (ctx.dataset.yAxisID === 'y1') return `TTF: ${Number(ctx.raw).toFixed(2)} €/MWh`;
                                const idx = ctx.dataIndex;
                                const demandDatasets = ctx.chart?.data?.datasets?.filter(ds => ds.yAxisID !== 'y1') || [];
                                const total = demandDatasets.reduce((s, ds) => s + (Number(ds.data?.[idx]) || 0), 0);
                                const val = Number(ctx.raw);
                                const pct = total > 0 ? (val / total * 100) : null;
                                return pct != null
                                    ? `${ctx.dataset.label}: ${val.toFixed(1)} GWh (${pct.toFixed(1)}%)`
                                    : `${ctx.dataset.label}: ${val.toFixed(1)} GWh`;
                            },
                            footer: (items) => {
                                const demandVals = items.filter(i => i.raw != null && i.dataset.yAxisID !== 'y1').map(i => Number(i.raw));
                                if (!demandVals.length) return 'No data for this day';
                                return `Total: ${demandVals.reduce((s, v) => s + v, 0).toFixed(1)} GWh`;
                            },
                        },
                    },
                },
                scales: {
                    x: { ticks: { maxTicksLimit: 10 } },
                    y: { stacked: true, title: { display: true, text: 'GWh / day' }, beginAtZero: true },
                    ...(hasTtf ? { y1: { position: 'right', title: { display: true, text: '€/MWh' }, grid: { drawOnChartArea: false }, ticks: { callback: v => `€${v}` } } } : {}),
                },
            },
        });

        setStatus(`${country}: ${days.length} days`);
    } catch (err) {
        console.error('Country gas chart failed:', err);
        setStatus(`Failed: ${err.message || err}`);
    }
}

function renderGasMap(latestRows) {
    const container = document.getElementById('gasMapContainer');
    if (!container) return;

    const rows = (latestRows || []).filter(r => r.country_code && Number.isFinite(Number(r.total_mwh)));
    if (!rows.length) {
        container.innerHTML = '<div class="chart-loading">No gas data yet.</div>';
        return;
    }

    const maxTotal = rows.reduce((m, r) => Math.max(m, Number(r.total_mwh) || 0), 0) || 1;
    const byIso = new Map();
    for (const r of rows) {
        const cc = String(r.country_code || '').toUpperCase();
        if (!cc) continue;
        byIso.set(cc, r);
        // Our DB stores the UK as "UK" but the GeoJSON uses the canonical
        // ISO-3166-1 alpha-2 code "GB", and the Greece feature sometimes
        // uses "EL" (EU code) instead of "GR". Alias both directions so
        // lookups by either key succeed.
        if (cc === 'UK') byIso.set('GB', r);
        else if (cc === 'GB') byIso.set('UK', r);
        else if (cc === 'GR') byIso.set('EL', r);
        else if (cc === 'EL') byIso.set('GR', r);
    }

    renderGasGeoMap(container, rows, byIso, maxTotal).catch((e) => {
        console.warn('Gas geo map failed, fallback to tiles:', e);
        renderGasTileGrid(container, rows, maxTotal);
            mapFallbackNote(container, e);
    });
}

function renderGasTileGrid(container, rows, maxTotal) {
    const legend = `
        <div class="energy-map-legend">
            <span>Low demand</span>
            <div class="energy-map-legend-bar" style="background: linear-gradient(90deg, rgb(219,234,254), rgb(29,78,216));"></div>
            <span>High demand</span>
        </div>
    `;

    const tiles = rows
        .sort((a, b) => String(a.country_code).localeCompare(String(b.country_code)))
        .map(r => {
            const c = String(r.country_code);
            const v = Number(r.total_mwh) || 0;
            const t = v / maxTotal;
            const bg = gasBlueScale(t);
            const color = gasBlueTextForBg(t);
            const isActive = gasSelectedCountry === c;
            return `
                <div class="energy-map-tile ${isActive ? 'active' : ''}" data-country="${escapeHtml(c)}" style="background:${bg}; color:${color}">
                    <div class="energy-map-tile-code">${escapeHtml(c)}</div>
                    <div class="energy-map-tile-value">${(v / 1000).toFixed(1)} GWh</div>
                </div>
            `;
        })
        .join('');

    container.innerHTML = `${legend}<div class="energy-map-grid">${tiles}</div>`;

    container.querySelectorAll('.energy-map-tile').forEach(el => {
        el.addEventListener('click', () => {
            const c = el.getAttribute('data-country');
            if (!c) return;
            gasSelectedCountry = c;
            loadGasCountryChart(c, gasCountryRange);
            container.querySelectorAll('.energy-map-tile').forEach(t => t.classList.remove('active'));
            el.classList.add('active');
        });
    });
}

async function renderGasGeoMap(container, rows, byIso, maxTotal) {
    const countryGeo = await fetchEuropeCountriesGeoJsonOnce();

    const width = 1400;
    const height = 860;
    const padding = 10;
    const bounds = { minLon: -25, maxLon: 45, minLat: 34, maxLat: 72 };

    const selected = String(gasSelectedCountry || '').toUpperCase();
    const selectedRow = byIso.get(selected);
    const selectedTotal = selectedRow ? (Number(selectedRow.total_mwh) || 0) : null;
    // Rows from the trailing-window aggregator carry a `_days_in_window` field.
    const isTrailing = rows.some(r => Number.isFinite(Number(r._days_in_window)));
    const chipLabel = isTrailing ? '30d total' : 'Total';
    const mapTitle = isTrailing ? 'Gas demand map · trailing 30 days' : 'Gas demand map (latest)';
    const mapSubtitle = isTrailing
        ? 'EU27 — total demand summed over the last 30 gas days · click a country to chart'
        : 'EU27 — total daily demand · click a country to chart';

    container.innerHTML = `
        <div class="energy-map-shell">
            <div class="energy-map-top">
                <div class="energy-map-top-left">
                    <div class="energy-map-title">${mapTitle}</div>
                    <div class="energy-map-subtitle">${mapSubtitle}</div>
                </div>
                <div class="energy-map-top-right">
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">Selected</div>
                        <div class="energy-map-chip-value">${escapeHtml(selected || '—')}</div>
                    </div>
                    <div class="energy-map-chip">
                        <div class="energy-map-chip-label">${chipLabel}</div>
                        <div class="energy-map-chip-value">${selectedTotal != null ? (selectedTotal / 1000).toFixed(0) + ' GWh' : '—'}</div>
                    </div>
                </div>
            </div>
            <div class="energy-map-legend energy-map-legend--premium">
                <span>Low</span>
                <div class="energy-map-legend-bar" style="background: linear-gradient(90deg, rgb(219,234,254), rgb(29,78,216));"></div>
                <span>High</span>
            </div>
            <div class="energy-map-stage">
                <svg class="energy-geo-map" viewBox="0 0 ${width} ${height}" role="img" aria-label="Gas demand map"></svg>
            </div>
        </div>
    `;

    const svg = container.querySelector('svg.energy-geo-map');
    if (!svg) return;

    let tooltip = document.querySelector('.energy-map-tooltip');
    if (!tooltip) {
        tooltip = document.createElement('div');
        tooltip.className = 'energy-map-tooltip';
        tooltip.style.display = 'none';
        document.body.appendChild(tooltip);
    }

    const features = Array.isArray(countryGeo?.features) ? countryGeo.features : [];
    const eu27Set = new Set(GAS_EU27);

    // Some GeoJSON features use the formal ISO-3166 alpha-2 code (e.g. "GB",
    // "EL") while our DB / rest of the app use the everyday code ("UK",
    // "GR"). Normalise the feature code to the DB convention so lookups,
    // selection, and colouring all work on a single canonical key.
    const featureCodeToDbCode = (code) => {
        switch (code) {
            case 'GB': return 'UK';
            case 'EL': return 'GR';
            default: return code;
        }
    };

    for (const f of features) {
        const rawIso = String(f?.properties?.ISO2 || '').toUpperCase();
        if (!rawIso) continue;
        if (rawIso === 'RU' || rawIso === 'BY') continue;
        const iso2 = featureCodeToDbCode(rawIso);

        const row = byIso.get(iso2);
        const val = row ? Number(row.total_mwh) : null;
        const t = val != null && maxTotal > 0 ? val / maxTotal : null;
        // UK is not in GAS_EU27 but is a valid, covered country in our data,
        // so we should treat it as clickable/highlightable on the map.
        const isCovered = eu27Set.has(iso2) || iso2 === 'UK';
        const fill = t != null ? gasBlueScale(t) : (isCovered ? 'rgba(148,163,184,0.28)' : 'rgba(148,163,184,0.12)');

        const geom = f.geometry;
        if (!geom) continue;

        const paths = [];
        if (geom.type === 'Polygon') {
            paths.push(polygonToPath(geom.coordinates[0], width, height, bounds, padding));
        } else if (geom.type === 'MultiPolygon') {
            for (const poly of geom.coordinates) {
                if (poly?.[0]) paths.push(polygonToPath(poly[0], width, height, bounds, padding));
            }
        } else {
            continue;
        }

        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', paths.join(' '));
        path.setAttribute('fill', fill);
        path.setAttribute('data-iso2', iso2);
        path.style.cursor = isCovered ? 'pointer' : 'default';
        if (selected && iso2 === selected) path.classList.add('is-selected');

        path.addEventListener('mouseenter', () => {
            const r = byIso.get(iso2);
            tooltip.style.display = 'block';
            if (r) {
                const tot = Number(r.total_mwh) || 0;
                const pw = r.power_mwh == null ? null : Number(r.power_mwh);
                const hh = r.household_mwh == null ? null : Number(r.household_mwh);
                const ind = r.industry_mwh == null ? null : Number(r.industry_mwh);
                const days = Number(r._days_in_window) || 0;
                const first = r._first_day || '';
                const last = r._last_day || r.gas_day || '';
                const header = isTrailing
                    ? `<div style="font-weight:600;margin-bottom:4px;">${iso2} · trailing ${days}d (${first} → ${last})</div>`
                    : `<div style="font-weight:600;margin-bottom:4px;">${iso2}</div>`;
                const totalLabel = isTrailing ? `Total (30d sum)` : `Total`;
                // Sector share: prefer the reported total, but fall back to the
                // sum of known sectors so partial-coverage rows still render
                // meaningful percentages.
                const shareBase = tot > 0
                    ? tot
                    : [pw, hh, ind].filter(v => v != null).reduce((s, v) => s + v, 0);
                const fmtV = (v) => v == null ? '—' : `${(v/1000).toFixed(1)} GWh`;
                const fmtVP = (v) => {
                    if (v == null) return '—';
                    const gwh = (v / 1000).toFixed(1);
                    if (!(shareBase > 0)) return `${gwh} GWh`;
                    return `${gwh} GWh (${(v / shareBase * 100).toFixed(1)}%)`;
                };
                tooltip.innerHTML = `
                    ${header}
                    <div>${totalLabel}: ${fmtV(tot)}</div>
                    <div>Power: ${fmtVP(pw)}</div>
                    <div>Household: ${fmtVP(hh)}</div>
                    <div>Industry: ${fmtVP(ind)}</div>
                `;
            } else {
                tooltip.textContent = `${iso2} — no data`;
            }
        });
        path.addEventListener('mousemove', (e) => {
            tooltip.style.left = `${e.clientX}px`;
            tooltip.style.top = `${e.clientY}px`;
        });
        path.addEventListener('mouseleave', () => {
            tooltip.style.display = 'none';
        });
        if (isCovered) {
            path.addEventListener('click', () => {
                gasSelectedCountry = iso2;
                loadGasCountryChart(iso2, gasCountryRange);
                renderGasGeoMap(container, rows, byIso, maxTotal).catch(() => {});
            });
        }

        svg.appendChild(path);
    }
}

function escapeHtml(str) {
    return String(str)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
}

// Load country overview charts
async function loadCountryOverviewCharts(countryId) {
    try {
        const container = document.getElementById('countryOverviewCharts');
        if (!container) return;
        
        container.innerHTML = '<div class="chart-loading">Loading charts...</div>';

        // Detect if this country is Lithuania or Spain (by DB data, not just in-memory state)
        let isLithuania = false;
        let isSpain = false;
        try {
            const { data: countryInfo, error: countryError } = await supabase
                .from('countries')
                .select('id, name, code')
                .eq('id', countryId)
                .single();
            
            if (!countryError && countryInfo) {
                const countryName = (countryInfo.name || '').toLowerCase();
                const countryCode = (countryInfo.code || '').toUpperCase();
                isLithuania = countryName.includes('lithuania') || countryCode === 'LTU';
                isSpain = countryName.includes('spain') || countryCode === 'ESP';

                // Keep global state in sync if it was not yet populated
                if (!currentCountryData) {
                    currentCountryData = { id: countryId, name: countryInfo.name, code: countryInfo.code };
                } else {
                    if (!currentCountryData.name) currentCountryData.name = countryInfo.name;
                    if (!currentCountryData.code) currentCountryData.code = countryInfo.code;
                }
            }
        } catch (countryLookupError) {
            console.warn('Could not determine country info for overview charts:', countryLookupError);
        }

        // For Lithuania, show ONLY the heating affordability chart from Table 43
        if (isLithuania) {
            const lt43ChartData = await buildLithuaniaTable43ChartFromCsv();
            container.innerHTML = '';
            
            if (!lt43ChartData) {
                container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">Heating affordability chart (Table 43) is not available.</p>';
                return;
            }

            const chartCard = document.createElement('div');
            chartCard.className = 'chart-card';
            chartCard.innerHTML = `
                <h3>${lt43ChartData.title}</h3>
                <div class="chart-container">
                    <canvas id="countryChartLTU_43"></canvas>
                </div>
            `;
            container.appendChild(chartCard);

            setTimeout(() => {
                renderCountryOverviewChart('countryChartLTU_43', lt43ChartData);
            }, 100);

            return;
        }
        
        // Define important table keywords and priorities
        const importantKeywords = [
            { keywords: ['target', 'renovation', 'rate'], priority: 1 },
            { keywords: ['emission', 'ghg', 'co2', 'greenhouse', 'carbon'], priority: 2 },
            { keywords: ['energy', 'efficiency', 'savings', 'consumption'], priority: 3 },
            { keywords: ['investment', 'financing', 'budget', 'cost', 'funding'], priority: 4 },
            { keywords: ['renewable', 'solar', 'res'], priority: 5 },
            { keywords: ['building', 'renovated', 'renovation'], priority: 6 }
        ];
        
        // Get all time-series tables for this country
        let allTables = [];
        const { data: timeSeriesTables, error } = await supabase
            .from('data_tables')
            .select('id, table_number, table_description, table_name, has_time_series, column_names')
            .eq('country_id', countryId)
            .eq('has_time_series', true)
            .order('table_number');
        
        if (error) {
            console.error('Error loading tables:', error);
            container.innerHTML = '<div class="chart-error">Error loading charts</div>';
            return;
        }

        allTables = timeSeriesTables || [];

        // Special case: for Lithuania, always try to include Table 43
        // ( "% Of People Living In Households Unable To Afford Sufficient Heating Due To Lack Of Money" )
        // even if it was not flagged as a time-series table during import.
        if (isLithuania) {
            try {
                const { data: lt43Tables, error: lt43Error } = await supabase
                    .from('data_tables')
                    .select('id, table_number, table_description, table_name, has_time_series, column_names')
                    .eq('country_id', countryId)
                    .or([
                        'table_number.eq.43',
                        'table_description.ilike.%unable%heating%',
                        'table_name.ilike.%Table_43%'
                    ].join(','))
                    .limit(5);

                if (!lt43Error && lt43Tables && lt43Tables.length > 0) {
                    lt43Tables.forEach(tbl => {
                        if (!allTables.some(t => t.id === tbl.id)) {
                            allTables.push(tbl);
                        }
                    });
                } else if (lt43Error) {
                    console.warn('Lithuania Table 43 lookup error for overview charts:', lt43Error);
                }
            } catch (ltError) {
                console.warn('Could not load Lithuania Table 43 for overview charts:', ltError);
            }
        }
        
        if (!allTables || allTables.length === 0) {
            container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No time-series tables available</p>';
            return;
        }
        
        // Score and prioritize tables
        const scoredTables = allTables.map(table => {
            const desc = (table.table_description || table.table_name || '').toLowerCase();
            let score = 0;
            let matchedCategory = '';
            
            for (const category of importantKeywords) {
                for (const keyword of category.keywords) {
                    if (desc.includes(keyword.toLowerCase())) {
                        score += (100 - category.priority * 10); // Higher priority = higher score
                        if (!matchedCategory) {
                            matchedCategory = category.keywords[0];
                        }
                        break;
                    }
                }
            }
            
            return { ...table, score, matchedCategory };
        });
        
        // Sort by score and take top 12
        const topTables = scoredTables
            .filter(t => t.score > 0)
            .sort((a, b) => b.score - a.score)
            .slice(0, 12);
        
        // If no scored tables, take first 8 time-series tables
        let tablesToDisplay = topTables.length > 0 ? topTables : allTables.slice(0, 8);

        // For Spain, always include the energy poverty indicators table (Table 1_7) if available,
        // and prioritize it near the front of the list so it appears prominently.
        if (isSpain) {
            const energyPovertyTable = allTables.find(t => {
                const desc = (t.table_description || '').toLowerCase();
                const name = (t.table_name || '').toLowerCase();
                const num = String(t.table_number || '').toLowerCase();
                return desc.includes('energy poverty indicators') ||
                       name.includes('energy_poverty_indicators') ||
                       num === '1_7';
            });

            if (energyPovertyTable) {
                const existingIdx = tablesToDisplay.findIndex(t => t.id === energyPovertyTable.id);
                if (existingIdx === -1) {
                    tablesToDisplay.unshift(energyPovertyTable);
                } else if (existingIdx > 0) {
                    const [tbl] = tablesToDisplay.splice(existingIdx, 1);
                    tablesToDisplay.unshift(tbl);
                }
            } else {
                console.warn('Spain energy poverty indicators table (Table 1_7) not found in overview tables.');
            }
        }

        // For Lithuania, always include Table 43 in Key Time-Series Data if available
        if (isLithuania) {
            const lt43 = allTables.find(t => String(t.table_number) === '43');
            if (lt43 && !tablesToDisplay.some(t => t.id === lt43.id)) {
                tablesToDisplay.push(lt43);
            }
        }

        console.log('Country overview tables for country', countryId, {
            isLithuania,
            isSpain,
            tableNumbers: tablesToDisplay.map(t => t.table_number || t.table_name)
        });
        
        // Load and render charts
        const charts = [];
        for (const table of tablesToDisplay) {
            const chartData = await buildCountryTableChart(table);
            if (chartData) {
                charts.push({ ...chartData, tableId: table.id });
            }
        }

        // For Lithuania, ensure the heating affordability chart from Table 43
        // is present and appears first in the Key Time-Series Data section.
        if (isLithuania) {
            const hasHeatingChart = charts.some(c => 
                typeof c.title === 'string' && 
                c.title.toLowerCase().includes('unable to afford sufficient heating')
            );
            
            if (!hasHeatingChart) {
                const lt43ChartData = await buildLithuaniaTable43ChartFromCsv();
                if (lt43ChartData) {
                    charts.unshift({ ...lt43ChartData, tableId: 'LTU_43_csv' });
                }
            } else {
                // If it exists but not first, move it to the front
                const idx = charts.findIndex(c => 
                    typeof c.title === 'string' && 
                    c.title.toLowerCase().includes('unable to afford sufficient heating')
                );
                if (idx > 0) {
                    const [heatingChart] = charts.splice(idx, 1);
                    charts.unshift(heatingChart);
                }
            }
        }
        
        // Render charts
        container.innerHTML = '';
        if (charts.length === 0) {
            container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No chartable time-series data available</p>';
            return;
        }
        
        charts.forEach((chartData, index) => {
            const chartCard = document.createElement('div');
            chartCard.className = 'chart-card';
            chartCard.innerHTML = `
                <h3>${chartData.title}</h3>
                <div class="chart-container">
                    <canvas id="countryChart${chartData.tableId}_${index}"></canvas>
                </div>
            `;
            container.appendChild(chartCard);
            
            // Render chart after DOM update
            setTimeout(() => {
                renderCountryOverviewChart(`countryChart${chartData.tableId}_${index}`, chartData);
            }, 100 * (index + 1));
        });
        
    } catch (error) {
        console.error('Error loading country overview charts:', error);
        const container = document.getElementById('countryOverviewCharts');
        if (container) {
            container.innerHTML = '<div class="chart-error">Error loading charts. Please try again.</div>';
        }
    }
}

// Build chart data for a single country table
async function buildCountryTableChart(table) {
    try {
        let points;
        let error;

        // First try Supabase data_points as usual
        try {
            const result = await supabase
                .from('data_points')
                .select('row_data')
                .eq('data_table_id', table.id)
                .limit(200);
            points = result.data;
            error = result.error;
        } catch (e) {
            error = e;
        }
        
        // Special fallback for Lithuania Table 43 if no data_points are found in Supabase
        const isLithuaniaTable43 = String(table.table_number) === '43' &&
            ((currentCountryData && currentCountryData.name && currentCountryData.name.toLowerCase().includes('lithuania')) ||
             (currentCountryData && currentCountryData.code && currentCountryData.code.toUpperCase() === 'LTU'));
        
        if ((!points || points.length === 0) && isLithuaniaTable43 && typeof fetch !== 'undefined') {
            try {
                const csvRelativePath = 'data/Lithuania data/Table_43_%_Of_people_living_in_households_unable_to_afford_sufficient_heating_due_to_lack_of_money.csv';
                const csvUrl = encodeURI(csvRelativePath);
                const response = await fetch(csvUrl);
                
                if (response.ok) {
                    const text = await response.text();
                    const lines = text.split(/\r?\n/).filter(line => line.trim().length > 0);
                    if (lines.length > 1) {
                        const headers = lines[0].split(',');
                        const dataRows = lines.slice(1);
                        points = dataRows.map(rowStr => {
                            const cols = rowStr.split(',');
                            const row_data = {};
                            headers.forEach((h, idx) => {
                                row_data[h] = cols[idx];
                            });
                            return { row_data };
                        });
                        console.warn('Using CSV fallback for Lithuania Table 43 chart (no Supabase data_points found).');
                    }
                } else {
                    console.warn('CSV fallback fetch for Lithuania Table 43 failed with status:', response.status);
                }
            } catch (csvError) {
                console.warn('Error using CSV fallback for Lithuania Table 43 chart:', csvError);
            }
        }
        
        if (error || !points || points.length === 0) {
            console.warn('No data points found for table when building country overview chart:', {
                tableId: table.id,
                tableNumber: table.table_number,
                tableName: table.table_name,
                error
            });
            return null;
        }
        
        // Extract time columns
        const headers = Object.keys(points[0].row_data);
        const timeColumns = headers.filter(h => {
            return /\b(19|20)\d{2}\b/.test(h) || 
                   (h.includes('-') && /\d{4}/.test(h)) ||
                   /^\d{4}$/.test(h.trim());
        });
        
        if (timeColumns.length === 0) return null;
        
        const sortedTimeColumns = timeColumns.sort((a, b) => {
            const yearA = extractYear(a);
            const yearB = extractYear(b);
            return yearA - yearB;
        });
        
        const labels = sortedTimeColumns.map(col => {
            const yearMatch = col.match(/\b(19|20)\d{2}\b/);
            if (yearMatch) return yearMatch[0];
            const rangeMatch = col.match(/(\d{4})-(\d{4})/);
            if (rangeMatch) return rangeMatch[1] + '-' + rangeMatch[2].slice(-2);
            const singleYear = col.match(/^\s*(\d{4})\s*$/);
            if (singleYear) return singleYear[1];
            return col;
        });
        
        // Group data by non-time columns to create multiple series
        const seriesMap = {};
        const nonTimeColumns = headers.filter(h => !timeColumns.includes(h));
        
        points.forEach(row => {
            // Create a key from non-time columns
            const key = nonTimeColumns.length > 0 
                ? nonTimeColumns.map(col => row.row_data[col] || '').join(' | ')
                : 'Total';
            
            if (!seriesMap[key]) {
                seriesMap[key] = new Array(sortedTimeColumns.length).fill(0);
            }
            
            sortedTimeColumns.forEach((col, idx) => {
                const val = parseFloat((row.row_data[col] || '0').toString().replace(/[%,]/g, ''));
                if (!isNaN(val)) {
                    seriesMap[key][idx] += Math.abs(val);
                }
            });
        });
        
        // Limit to top 6 series to avoid clutter
        const seriesEntries = Object.entries(seriesMap)
            .sort((a, b) => {
                const sumA = a[1].reduce((s, v) => s + v, 0);
                const sumB = b[1].reduce((s, v) => s + v, 0);
                return sumB - sumA;
            })
            .slice(0, 6);
        
        if (seriesEntries.length === 0) return null;
        
        const colors = [
            'rgba(197, 183, 114, 1)', // E3G gold
            'rgba(168, 196, 216, 1)', // E3G blue
            'rgba(123, 140, 80, 1)',  // E3G olive
            'rgba(50, 48, 103, 1)',   // E3G dark blue
            'rgba(139, 172, 196, 1)', // Light blue
            'rgba(168, 183, 114, 1)', // Light gold
        ];
        
        const datasets = seriesEntries.map(([label, values], idx) => ({
            label: label.length > 50 ? label.substring(0, 47) + '...' : label,
            data: values,
            borderColor: colors[idx % colors.length],
            backgroundColor: colors[idx % colors.length].replace('1)', '0.2)'),
            tension: 0.1,
            fill: false,
            borderWidth: 2
        }));
        
        return {
            title: table.table_description || table.table_name.replace(/_/g, ' ') || `Table ${table.table_number}`,
            labels: labels,
            datasets: datasets
        };
    } catch (error) {
        console.error('Error building table chart:', error);
        return null;
    }
}

// Build Lithuania Table 43 chart directly from the CSV file as a fallback / override
async function buildLithuaniaTable43ChartFromCsv() {
    try {
        if (typeof fetch === 'undefined') return null;
        
        const csvRelativePath = 'data/Lithuania data/Table_43_%_Of_people_living_in_households_unable_to_afford_sufficient_heating_due_to_lack_of_money.csv';
        const csvUrl = encodeURI(csvRelativePath);
        const response = await fetch(csvUrl);
        
        if (!response.ok) {
            console.warn('Failed to fetch Lithuania Table 43 CSV for overview chart, status:', response.status);
            return null;
        }
        
        const text = await response.text();
        const lines = text.split(/\r?\n/).filter(line => line.trim().length > 0);
        if (lines.length < 2) return null;
        
        const headers = lines[0].split(',');
        const yearHeaders = headers.slice(1); // e.g. 2018, 2019, ..., 2024
        
        let lithuaniaValues = [];
        let euValues = [];
        
        lines.slice(1).forEach(rowStr => {
            const cols = rowStr.split(',');
            const label = (cols[0] || '').toLowerCase();
            const values = cols.slice(1).map(v => parseFloat(v));
            
            if (label.startsWith('lithuania')) {
                lithuaniaValues = values;
            } else if (label.includes('eu')) {
                euValues = values;
            }
        });
        
        if (lithuaniaValues.length === 0 && euValues.length === 0) return null;
        
        const colors = [
            'rgba(197, 183, 114, 1)', // Lithuania
            'rgba(168, 196, 216, 1)'  // EU average
        ];
        
        const datasets = [];
        if (lithuaniaValues.length > 0) {
            datasets.push({
                label: 'Lithuania',
                data: lithuaniaValues,
                borderColor: colors[0],
                backgroundColor: colors[0].replace('1)', '0.2)'),
                tension: 0.1,
                fill: false,
                borderWidth: 2
            });
        }
        if (euValues.length > 0) {
            datasets.push({
                label: 'Average of EU countries',
                data: euValues,
                borderColor: colors[1],
                backgroundColor: colors[1].replace('1)', '0.2)'),
                tension: 0.1,
                fill: false,
                borderWidth: 2
            });
        }
        
        return {
            title: '% Of People Living In Households Unable To Afford Sufficient Heating Due To Lack Of Money',
            labels: yearHeaders,
            datasets
        };
    } catch (error) {
        console.error('Error building Lithuania Table 43 chart from CSV:', error);
        return null;
    }
}

// Extract year from column name
function extractYear(colName) {
    const yearMatch = colName.match(/\b(19|20)(\d{2})\b/);
    if (yearMatch) {
        return parseInt(yearMatch[1] + yearMatch[2]);
    }
    const rangeMatch = colName.match(/(\d{4})-(\d{4})/);
    if (rangeMatch) {
        return parseInt(rangeMatch[1]);
    }
    const singleYear = colName.match(/^\s*(\d{4})\s*$/);
    if (singleYear) {
        return parseInt(singleYear[1]);
    }
    return 0;
}

// Render country overview chart
function renderCountryOverviewChart(canvasId, chartData) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    
    // Destroy previous chart if exists
    const existingChart = Chart.getChart(canvas);
    if (existingChart) {
        existingChart.destroy();
    }
    
    const ctx = canvas.getContext('2d');
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: chartData.labels,
            datasets: chartData.datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: chartData.datasets.length <= 4,
                    position: 'right',
                    labels: {
                        boxWidth: 12,
                        padding: 6,
                        font: { size: 11 }
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    title: {
                        display: true,
                        text: chartData.unitLabel || 'Value',
                        font: { size: 11 }
                    },
                    ticks: {
                        font: { size: 10 }
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Time Period',
                        font: { size: 11 }
                    },
                    ticks: {
                        font: { size: 10 },
                        maxRotation: 45,
                        minRotation: 0
                    }
                }
            }
        }
    });
}

// Switch tab
function switchTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.getAttribute('data-tab') === tabName) {
            btn.classList.add('active');
        }
    });
    
    // Update tab panes
    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.classList.remove('active');
        if (pane.id === `${tabName}Tab`) {
            pane.classList.add('active');
        }
    });
    
    // Load tab content if needed
    if (tabName === 'data-tables' && currentCountryId) {
        loadDataTables(currentCountryId);
    } else if (tabName === 'measures' && currentCountryId) {
        loadMeasures(currentCountryId);
    } else if (tabName === 'stakeholders' && currentCountryId) {
        loadStakeholderMapping(currentCountryId);
    } else if (tabName === 'analysis' && currentCountryId) {
        loadComparisonTables();
    } else if (tabName === 'overview' && currentCountryId) {
        loadCountryOverviewCharts(currentCountryId);
    }
}

// Load dashboard
async function loadDashboard() {
    try {
        showLoading(true);
        console.log('Loading dashboard data...');
        
        if (!supabase) {
            throw new Error('Supabase client not initialized');
        }
        
        // Clear any existing charts first
        const chartsContainer = document.getElementById('dashboardCharts');
        if (chartsContainer) {
            chartsContainer.innerHTML = '<div class="chart-loading">Loading target charts...</div>';
        }
        
        // Load statistics
        const [countriesData, tablesData, measuresData] = await Promise.all([
            supabase.from('countries').select('id'),
            supabase.from('data_tables').select('id, has_time_series'),
            supabase.from('measures').select('id')
        ]);
        
        console.log('Dashboard data loaded:', {
            countries: countriesData,
            tables: tablesData,
            measures: measuresData
        });
        
        if (countriesData.error) {
            throw new Error('Failed to load countries: ' + countriesData.error.message);
        }
        if (tablesData.error) {
            throw new Error('Failed to load tables: ' + tablesData.error.message);
        }
        if (measuresData.error) {
            throw new Error('Failed to load measures: ' + measuresData.error.message);
        }
        
        const countries = countriesData.data || [];
        const tables = tablesData.data || [];
        const measures = measuresData.data || [];
        const timeSeriesTables = tables.filter(t => t.has_time_series).length;
        
        // Update stats
        document.getElementById('totalCountries').textContent = countries.length;
        document.getElementById('totalTables').textContent = tables.length;
        document.getElementById('timeSeriesTables').textContent = timeSeriesTables;
        document.getElementById('totalMeasures').textContent = measures.length;
        
        // Show the dashboard quickly, then load heavy sections after first paint.
        showLoading(false);

        setTimeout(() => {
            loadCountriesGrid().catch(e => console.warn('Countries grid load failed:', e));
        }, 0);

        setTimeout(() => {
            loadDashboardCharts().catch(e => console.warn('Dashboard charts load failed:', e));
        }, 50);
    } catch (error) {
        console.error('Error loading dashboard:', error);
        showError('Failed to load dashboard data');
        showLoading(false);
    }
}

// Load dashboard charts for key targets
async function loadDashboardCharts() {
    try {
        const container = document.getElementById('dashboardCharts');
        container.innerHTML = '<div class="chart-loading">Loading target charts...</div>';
        
        // Define target categories and keywords to search for
        const targetCategories = [
            {
                title: 'GHG Emission Reductions (Per Area)',
                keywords: ['emission', 'ghg', 'co2', 'greenhouse', 'carbon'],
                tableNumbers: ['15', '2_7', '2_7_1', '2_3', '3_12'], // Belgium 15, Romania 2_7 & 2_7_1 (per-area), Spain 2_3 (per-area), Croatia 3_12
                unitType: 'per_area', // kgCO2eq/m2
                unitLabel: 'kgCO2eq/m²'
            },
            {
                title: 'GHG Emission Reductions (Total)',
                keywords: ['emission', 'ghg', 'co2', 'greenhouse', 'carbon'],
                tableNumbers: ['14', '63', '64', '2_6', '2_7', '2_3'], // Slovenia 14, Lithuania 63+64, Finland 2_6+2_7, Romania 2_7 (total), Spain 2_3 (total)
                unitType: 'total', // kt/Mt CO2 eq
                unitLabel: 'kt CO2 eq'
            },
            {
                title: 'Energy Efficiency Targets',
                keywords: ['energy', 'efficiency', 'savings', 'consumption'],
                tableNumbers: ['10', '11', '12', '2_12', '2_13', '3_2', '3_6', '3_8']
            },
            {
                title: 'Renovation Rates',
                keywords: ['renovation', 'rate', 'renovated'],
                tableNumbers: ['7', '8', '2_11', '3_1']
            },
            {
                title: 'Investment & Financing Needs',
                keywords: ['investment', 'financing', 'budget', 'cost', 'funding'],
                tableNumbers: ['19', '5_1']
            },
            {
                title: 'Renewable Energy Deployment',
                keywords: ['renewable', 'solar', 'res', 'renewable energy'],
                tableNumbers: ['13', '2_19', '3_9', '3_10', '3_11']
            }
        ];
        
        const charts = [];
        
        for (const category of targetCategories) {
            // Find tables matching this category - try multiple queries
            let matchingTables = [];
            
            // For GHG charts, search more comprehensively
            if (category.title.includes('GHG Emission Reductions')) {
                // Search by all table numbers for this category
                for (const tableNum of category.tableNumbers) {
                    const { data: tables, error } = await supabase
                        .from('data_tables')
                        .select('id, table_number, table_description, table_name, has_time_series, country_id, countries(name, code)')
                        .eq('table_number', tableNum)
                        .eq('has_time_series', true)
                        .limit(20);
                    
                    if (!error && tables && tables.length > 0) {
                        matchingTables = matchingTables.concat(tables);
                    }
                }
            } else {
                // For other categories, use original logic
                // Try to find by table numbers first
                for (const tableNum of category.tableNumbers.slice(0, 3)) {
                    const { data: tables, error } = await supabase
                        .from('data_tables')
                        .select('id, table_number, table_description, table_name, has_time_series, country_id, countries(name, code)')
                        .eq('table_number', tableNum)
                        .eq('has_time_series', true)
                        .limit(20);
                    
                    if (!error && tables && tables.length > 0) {
                        matchingTables = matchingTables.concat(tables);
                    }
                }
                
                // Also search by keywords in description
                for (const keyword of category.keywords.slice(0, 2)) {
                    const { data: tables, error } = await supabase
                        .from('data_tables')
                        .select('id, table_number, table_description, table_name, has_time_series, country_id, countries(name, code)')
                        .ilike('table_description', `%${keyword}%`)
                        .eq('has_time_series', true)
                        .limit(10);
                    
                    if (!error && tables && tables.length > 0) {
                        // Avoid duplicates
                        const existingIds = new Set(matchingTables.map(t => t.id));
                        matchingTables = matchingTables.concat(tables.filter(t => !existingIds.has(t.id)));
                    }
                }
            }
            
            if (!matchingTables || matchingTables.length === 0) {
                console.log(`No tables found for category: ${category.title}`);
                continue;
            }
            
            console.log(`Found ${matchingTables.length} tables for ${category.title}`);
            
            // Group by table_number to find comparable data
            const tableGroups = {};
            matchingTables.forEach(table => {
                const key = table.table_number || table.table_name;
                if (!tableGroups[key]) {
                    tableGroups[key] = [];
                }
                tableGroups[key].push(table);
            });
            
            // Find tables that exist in multiple countries
            const comparableGroups = Object.entries(tableGroups)
                .filter(([key, tables]) => tables.length > 1)
                .slice(0, 1); // Take first comparable group
            
            // For GHG emissions, use specialized function (don't require multiple countries)
            if (category.title.includes('GHG Emission Reductions')) {
                console.log(`Building GHG chart for ${category.title} with ${matchingTables.length} tables:`, matchingTables.map(t => `${t.countries?.name || 'Unknown'} - ${t.table_number || 'N/A'}`));
                const chartData = await buildGHGComparisonChart(matchingTables, category);
                console.log(`GHG chart data for ${category.title}:`, chartData ? `${chartData.datasets?.length || 0} datasets` : 'null');
                if (chartData && chartData.datasets && chartData.datasets.length > 0) {
                    charts.push(chartData);
                } else {
                    console.warn(`No chart data generated for ${category.title}. Tables found: ${matchingTables.length}`);
                }
            } else if (comparableGroups.length > 0) {
                const [tableNumber, tables] = comparableGroups[0];
                const chartData = await buildComparisonChartData(tables, category.title);
                if (chartData) {
                    charts.push(chartData);
                }
            }
        }
        
        // Render charts
        container.innerHTML = '';
        if (charts.length === 0) {
            container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No comparable target data found across countries</p>';
            return;
        }
        
        charts.forEach((chartData, index) => {
            const chartCard = document.createElement('div');
            chartCard.className = 'chart-card';
            chartCard.innerHTML = `
                <h3>${chartData.title}</h3>
                <div class="chart-container">
                    <canvas id="dashboardChart${index}"></canvas>
                </div>
            `;
            container.appendChild(chartCard);
            
            // Render chart after DOM update with increasing delay to avoid conflicts
            setTimeout(() => {
                renderDashboardChart(`dashboardChart${index}`, chartData);
            }, 150 * (index + 1));
        });
        
    } catch (error) {
        console.error('Error loading dashboard charts:', error);
        document.getElementById('dashboardCharts').innerHTML = 
            '<div class="chart-error">Error loading charts. Please try again.</div>';
    }
}

// Build GHG comparison chart with proper unit handling
async function buildGHGComparisonChart(allTables, category) {
    try {
        const unitType = category.unitType || 'per_area';
        const unitLabel = category.unitLabel || 'kgCO2eq/m²';
        
        // Map of country codes to their specific table numbers
        const countryTableMap = {
            'BEL': unitType === 'per_area' ? ['15'] : [], // Belgium only has per-area
            'ROU': unitType === 'per_area' ? ['2_7_1'] : ['2_7'], // Romania: 2_7_1 for per-area, 2_7 for total
            'HRV': unitType === 'per_area' ? ['3_12'] : [], // Croatia Table 3_12 (operational GHG emissions per area)
            'ESP': unitType === 'per_area' ? ['2_3'] : ['2_3'], // Spain: Table 2_3_2 for emissions (per-area)
            'SVN': unitType === 'total' ? ['14'] : [], // Slovenia only has total
            'LTU': unitType === 'total' ? ['63', '64'] : [], // Lithuania has total
            'FIN': unitType === 'total' ? ['2_6', '2_7'] : [] // Finland has total (residential + non-residential)
        };
        
        const countryData = {};
        const targetYears = ['2030', '2040', '2050'];
        
        console.log(`buildGHGComparisonChart: Processing ${allTables.length} tables for unitType: ${unitType}`);
        
        // Fetch data for each country
        for (const table of allTables) {
            const country = table.countries;
            if (!country) continue;
            
            const countryCode = country.code || '';
            const countryName = country.name || 'Unknown';
            // Use let instead of const to allow reassignment for Romania Table 2_7_1
            let tableNum = table.table_number || '';
            
            // Check if this table matches our criteria
            const expectedTables = countryTableMap[countryCode] || [];
            // Special handling for Romania Table 2_7_1 - check by description/filename too
            const tableDesc = (table.table_description || '').toLowerCase();
            const fileName = (table.table_name || table.original_filename || '').toLowerCase();
            const isRomaniaTable271 = countryCode === 'ROU' && 
                (tableDesc.includes('expected_annual_ghg_emissions_total') ||
                 (fileName.includes('2_7_1') && fileName.includes('expected_annual_ghg_emissions_total')));
            
            // Special handling for Spain Table 2_3 - need to distinguish between emissions (2_3_2) and reductions (2_3_3)
            // For per-area chart, we want Table 2_3_2 (emissions), not Table 2_3_3 (reductions)
            const isSpainTable232 = countryCode === 'ESP' && 
                (tableDesc.includes('co2eq_emissions_per_use') || fileName.includes('2_3_2'));
            const isSpainTable233 = countryCode === 'ESP' && 
                (tableDesc.includes('emission_savings') || tableDesc.includes('reduction') || fileName.includes('2_3_3'));
            
            // For Romania Table 2_7_1, use the actual table number for matching
            if (isRomaniaTable271) {
                // Treat it as table number '2_7_1' for the rest of the logic
                // This is safe because tableNum is declared with 'let' above
                tableNum = '2_7_1';
            }
            
            // For Spain per-area chart, only use Table 2_3_2 (emissions), skip Table 2_3_3 (reductions)
            if (countryCode === 'ESP' && unitType === 'per_area' && isSpainTable233) {
                continue; // Skip reductions table for per-area chart - we want emissions
            }
            
            // For Spain total chart, use Table 2_3_2 (emissions)
            if (countryCode === 'ESP' && unitType === 'total' && isSpainTable233) {
                continue; // Skip reductions table for total chart
            }
            
            // If we have specific tables for this country, only use those
            // For Romania Table 2_7_1, also allow it if it matches by description/filename
            if (expectedTables.length > 0 && !expectedTables.includes(tableNum) && !isRomaniaTable271) {
                continue;
            }
            
            // Get data points
            const { data: points, error } = await supabase
                .from('data_points')
                .select('row_data')
                .eq('data_table_id', table.id)
                .limit(200);
            
            if (error || !points || points.length === 0) continue;
            
            // Extract time-series columns for target years
            const firstRow = points[0].row_data;
            const allColumns = Object.keys(firstRow);
            
            // Special handling for Romania Table 2_7_1 (has Year column + separate metric columns)
            // Reuse tableDesc and fileName already declared above
            // Check again if this is Romania Table 2_7_1 (now that we have tableNum potentially updated)
            const isRomaniaTable271Final = countryCode === 'ROU' && 
                (tableNum === '2_7_1' || 
                 tableDesc.includes('expected_annual_ghg_emissions_total') ||
                 (fileName.includes('2_7_1') && fileName.includes('expected_annual_ghg_emissions_total')));
            
            let yearColumn = null;
            let metricColumn = null;
            
            if (isRomaniaTable271Final) {
                // Find the Year column
                for (const col of allColumns) {
                    if (col.toLowerCase() === 'year' || col.toLowerCase().includes('year')) {
                        yearColumn = col;
                        break;
                    }
                }
                // Find the per-area metric column - look for "Indicative average emission intensity"
                for (const col of allColumns) {
                    const colLower = col.toLowerCase();
                    if ((colLower.includes('emission intensity') || colLower.includes('intensity')) && 
                        (colLower.includes('/m2') || colLower.includes('/m²') || colLower.includes('m2') || colLower.includes('m²') ||
                         colLower.includes('kgco2eq') || colLower.includes('co2eq'))) {
                        metricColumn = col;
                        break;
                    }
                }
            }
            
            // Find columns matching target years and unit type
            const yearColumns = {};
            
            if (isRomaniaTable271Final && yearColumn && metricColumn) {
                // For Romania Table 2_7_1, match Year column values to target years
                // Data is clean, so just match exact year strings
                for (const year of targetYears) {
                    // Find rows where Year column matches the target year exactly
                    const matchingRow = points.find(row => {
                        const yearVal = (row.row_data[yearColumn] || '').toString().trim();
                        // Exact match or contains the year (e.g., "2030" matches "2030")
                        return yearVal === year || yearVal.startsWith(year);
                    });
                    if (matchingRow) {
                        yearColumns[year] = { column: metricColumn, row: matchingRow };
                    }
                }
            } else {
                // Standard column-based detection
                for (const year of targetYears) {
                    for (const col of allColumns) {
                        const colLower = col.toLowerCase();
                        // Skip percentage columns
                        if (colLower.includes('%') || colLower.includes('percent')) continue;
                        
                        // Check if column contains the year
                        if (col.includes(year)) {
                            // For per-area: look for kgCO2eq/m2, kgCO2eq/(m2, per m2, etc.
                            if (unitType === 'per_area') {
                                // Check for per-area indicators
                                const hasPerArea = colLower.includes('/m2') || colLower.includes('/m²') || 
                                                 colLower.includes('per m2') || colLower.includes('per m²') ||
                                                 colLower.includes('(m2') || colLower.includes('(m²') ||
                                                 colLower.includes('m2.y') || colLower.includes('m².y') ||
                                                 colLower.includes('m2/year') || colLower.includes('m²/year');
                                // Check for emission indicators
                                const hasEmission = colLower.includes('kgco2eq') || colLower.includes('co2eq') || 
                                                  colLower.includes('emission') || colLower.includes('ghg') ||
                                                  colLower.includes('carbon') || colLower.includes('intensity');
                                
                                // Bulgaria Table 22 has "Expected reduction of annual operational greenhouse gas emissions (kgCO2eq/(m².y))"
                                // Croatia Table 3_12 has operational GHG emissions per area
                                // Spain Table 2_3_3 has "Expected reduction" columns
                                // Prioritize "reduction" or "savings" columns over "emissions" columns
                                const isReduction = colLower.includes('reduction') || colLower.includes('savings');
                                const isEmission = colLower.includes('emission') && !isReduction;
                                
                                // For Spain per-area chart, ONLY use emission columns (Table 2_3_2), skip reduction columns (Table 2_3_3)
                                if (countryCode === 'ESP' && unitType === 'per_area' && isReduction) {
                                    continue; // Skip reduction columns for Spain per-area chart - we want emissions
                                }
                                
                                // Check for reduction columns first (Bulgaria Table 22) - but not for Spain per-area
                                if (colLower.includes('reduction') && hasPerArea && !(countryCode === 'ESP' && unitType === 'per_area')) {
                                    yearColumns[year] = col;
                                    break;
                                }
                                
                                if (hasEmission && hasPerArea) {
                                    // For Spain per-area, ONLY use "GHG emissions per m²" columns, NOT "Total GHG emissions"
                                    // Skip columns that contain "total" and "ghg emissions" together (those are total emissions, not per-area)
                                    if (countryCode === 'ESP' && unitType === 'per_area') {
                                        // Spain per-area: use per-area emission columns, skip total emission columns
                                        // Skip columns like "Total GHG emissions 2030" (these are total, not per-area)
                                        if (colLower.includes('total') && colLower.includes('ghg emissions') && !colLower.includes('per m') && !colLower.includes('/m')) {
                                            continue; // Skip "Total GHG emissions" columns for Spain per-area
                                        }
                                        // Use ONLY "GHG emissions per m²" columns (e.g., "GHG emissions per m² 2030")
                                        // Must contain "per m" or "/m" to be per-area
                                        if ((colLower.includes('ghg emissions per m') || colLower.includes('emissions per m²') || 
                                             colLower.includes('per m²')) && colLower.includes(year)) {
                                            yearColumns[year] = col;
                                            console.log(`Spain: Selected column for ${year}: ${col}`);
                                            break;
                                        }
                                    } else if (isReduction) {
                                        // Others: prefer reduction columns
                                        yearColumns[year] = col;
                                        break;
                                    } else if (!yearColumns[year]) {
                                        // Fallback: use emission columns if no reduction column found
                                        yearColumns[year] = col;
                                    }
                                }
                                // Belgium Table 15 has format like "2030 [kgCO2eq/m2.year]"
                                if (colLower.includes('kgco2eq') && colLower.includes('m2') && !isEmission) {
                                    yearColumns[year] = col;
                                    break;
                                }
                                // Croatia Table 3_12: operational greenhouse gas emissions
                                if (colLower.includes('operational') && hasEmission && hasPerArea) {
                                    yearColumns[year] = col;
                                    break;
                                }
                            }
                            // For total: look for kt, Mt, tonnes, total emissions (but not per m2)
                            else if (unitType === 'total') {
                                const hasTotal = (colLower.includes('kt') || colLower.includes('mt') || 
                                                colLower.includes('tonnes') || (colLower.includes('total') && colLower.includes('ghg emissions'))) &&
                                                !colLower.includes('/m2') && !colLower.includes('/m²') && 
                                                !colLower.includes('per m2') && !colLower.includes('per m²') &&
                                                !colLower.includes('(m2') && !colLower.includes('(m²');
                                const hasEmission = colLower.includes('co2eq') || colLower.includes('co2 eq') ||
                                                  colLower.includes('emission') || colLower.includes('ghg') ||
                                                  colLower.includes('carbon');
                                
                                if (hasEmission && hasTotal) {
                                    yearColumns[year] = col;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            
            if (Object.keys(yearColumns).length === 0) continue;
            
            // Aggregate values for this country
            const values = {};
            for (const year of targetYears) {
                if (!yearColumns[year]) {
                    values[year] = null;
                    continue;
                }
                
                // Handle Romania Table 2_7_1 special structure
                let col, rowToUse;
                if (isRomaniaTable271Final && typeof yearColumns[year] === 'object') {
                    col = yearColumns[year].column;
                    rowToUse = yearColumns[year].row;
                    // Extract value directly from the matched row
                    const valStr = (rowToUse.row_data[col] || '0').toString().replace(/[%,]/g, '').trim();
                    const val = parseFloat(valStr);
                    // Allow 0 as a valid value (Spain has 0 in 2050)
                    // Use val directly (not Math.abs) to preserve negative values if needed, but for emissions we want positive
                    values[year] = !isNaN(val) ? (val < 0 ? Math.abs(val) : val) : null;
                    continue;
                }
                
                col = yearColumns[year];
                let sum = 0;
                let count = 0;
                
                // Special handling for Finland (combine residential + non-residential)
                if (countryCode === 'FIN' && (tableNum === '2_6' || tableNum === '2_7')) {
                    // Sum all rows (residential + non-residential) - look for numeric values in the column
                    points.forEach(row => {
                        const valStr = (row.row_data[col] || '0').toString().replace(/[%,]/g, '').trim();
                        // Skip text values like "Value in Year (2040) - value in Year (X-2)"
                        if (valStr.toLowerCase().includes('value in year') || valStr.toLowerCase().includes('savings')) {
                            return;
                        }
                        const val = parseFloat(valStr);
                        if (!isNaN(val) && val !== 0) {
                            sum += Math.abs(val);
                            count++;
                        }
                    });
                } else {
                    // For other countries, aggregate appropriately
                    // For Belgium Table 15, Bulgaria Table 22, Croatia Table 3_12: prefer "Total" row
                    let foundTotal = false;
                    
                    // First, try to find "Total" row for countries that have it
                    for (const row of points) {
                        // Check for "Total" row FIRST (before parsing value) - especially important for Spain
                        const sector = (row.row_data['SECTOR'] || row.row_data['Sector'] || '').toLowerCase();
                        const buildingType = (row.row_data['Building type'] || row.row_data['Building Type'] || row.row_data['Type of building'] || row.row_data['TYPE OF BUILDINGS'] || row.row_data['BUILDING TYPE'] || '').toLowerCase();
                        const typeOfBuildings = (row.row_data['TYPE OF BUILDINGS'] || '').toLowerCase();
                        
                        // Check if this is a Total row BEFORE parsing the value
                        const isBelgiumTotal = countryCode === 'BEL' && tableNum === '15' && 
                            (sector === 'total' || typeOfBuildings === 'total' || buildingType === 'total');
                        const isSpainTotal = countryCode === 'ESP' && tableNum === '2_3' && 
                            (buildingType === 'total' || buildingType === 't' || buildingType === 't total' || buildingType.trim() === 't' || buildingType.startsWith('total'));
                        const isCroatiaTotal = countryCode === 'HRV' && tableNum === '3_12' && 
                            (buildingType === 'total' || buildingType.includes('total'));
                        
                        if (isBelgiumTotal || isSpainTotal || isCroatiaTotal) {
                            // Now parse the value - handle empty strings as 0 for Spain
                            let valStr = (row.row_data[col] || '').toString().replace(/[%,]/g, '').trim();
                            
                            // Skip text values
                            if (valStr.toLowerCase().includes('value in year') || 
                                valStr.toLowerCase().includes('savings residential') ||
                                valStr.toLowerCase().includes('savings non-residential') ||
                                (valStr.toLowerCase().includes('savings') && valStr.toLowerCase().includes('+'))) {
                                continue;
                            }
                            
                            // Handle empty string as 0 (especially for Spain 2050)
                            if (valStr === '' || valStr === '0') {
                                valStr = '0';
                            }
                            
                            const val = parseFloat(valStr.replace(',', '.')); // Handle comma decimal separator
                            
                            // For Spain, explicitly handle 0 and empty strings
                            let actualVal = val;
                            if (isSpainTotal && (isNaN(val) || valStr === '' || valStr === '0')) {
                                actualVal = 0; // Spain 2050 is 0
                            } else if (!isNaN(val)) {
                                actualVal = val;
                            } else {
                                continue; // Skip if we can't parse and it's not explicitly 0
                            }
                            
                            // Use the Total row value directly
                            sum = actualVal; // Preserve 0 for Spain
                            count = 1;
                            foundTotal = true;
                            if (isSpainTotal) {
                                console.log(`Spain: Found Total row, year ${year}, column ${col}, raw value="${row.row_data[col]}", cleaned="${valStr}", parsed value=${actualVal}, buildingType="${buildingType}"`);
                            }
                            break;
                        }
                    }
                    
                    // If we didn't find a "Total" row, sum all valid rows (excluding text values)
                    // BUT for Spain, we MUST use the Total row - don't sum other rows
                    if (!foundTotal) {
                        if (countryCode === 'ESP' && unitType === 'per_area') {
                            // For Spain per-area, we MUST have a Total row - if not found, skip this year
                            console.warn(`Spain: Total row not found for year ${year}, column ${col}`);
                            values[year] = null;
                            continue;
                        }
                        
                        for (const row of points) {
                        let valStr = (row.row_data[col] || '').toString().replace(/[%,]/g, '').trim();
                        
                        // Skip text values
                        if (valStr.toLowerCase().includes('value in year') || 
                            valStr.toLowerCase().includes('savings residential') ||
                            valStr.toLowerCase().includes('savings non-residential') ||
                            (valStr.toLowerCase().includes('savings') && valStr.toLowerCase().includes('+'))) {
                            continue;
                        }
                        
                        // Handle empty string as 0 for Spain (2050 might be stored as empty string)
                        if (valStr === '' && countryCode === 'ESP') {
                            valStr = '0';
                        }
                        
                        const val = parseFloat(valStr.replace(',', '.')); // Handle comma decimal separator
                        // Allow 0 as a valid value (Spain has 0 in 2050)
                        // Check explicitly for 0 or valid number
                        if (!isNaN(val) || (valStr === '0' || valStr === '')) {
                            // If it's explicitly "0" or empty, treat as 0
                            const actualVal = (valStr === '0' || valStr === '') ? 0 : val;
                                sum += Math.abs(val);
                                count++;
                            }
                        }
                    }
                }
                
                // Convert units if needed
                // Allow 0 as a valid value (Spain has 0 in 2050)
                // If we found at least one row (count > 0), use the sum even if it's 0
                // For Spain, explicitly preserve 0 values
                let finalValue = count > 0 ? sum : null;
                
                // Debug: Log Spain values to see what we're getting
                if (countryCode === 'ESP' && unitType === 'per_area') {
                    console.log(`Spain: Year ${year}, finalValue=${finalValue}, sum=${sum}, count=${count}`);
                }
                if (finalValue !== null && col) {
                    // Convert tonnes to kt (divide by 1000)
                    if (col.toLowerCase().includes('tonnes') && !col.toLowerCase().includes('kt')) {
                        finalValue = finalValue / 1000;
                    }
                    // Convert Mt to kt (multiply by 1000)
                    if (col.toLowerCase().includes('mtco2eq') || col.toLowerCase().includes('mt co2')) {
                        finalValue = finalValue * 1000;
                    }
                }
                
                values[year] = finalValue;
            }
            
            // Only add if we have at least one valid value
            if (Object.values(values).some(v => v !== null)) {
                if (!countryData[countryName]) {
                    countryData[countryName] = {
                        code: countryCode,
                        values: {}
                    };
                }
                // Merge values (for countries with multiple tables like Finland)
                Object.assign(countryData[countryName].values, values);
            }
        }
        
        if (Object.keys(countryData).length === 0) return null;
        
        // Build chart data
        // Include years where at least one country has a value (including 0)
        const labels = targetYears.filter(y => 
            Object.values(countryData).some(c => c.values[y] !== null && c.values[y] !== undefined)
        );
        
        if (labels.length === 0) return null;
        
        const datasets = [];
        const colors = [
            'rgba(197, 183, 114, 1)', // E3G gold
            'rgba(168, 196, 216, 1)', // E3G blue
            'rgba(123, 140, 80, 1)',  // E3G olive
            'rgba(50, 48, 103, 1)',   // E3G dark blue
            'rgba(139, 172, 196, 1)', // Light blue
            'rgba(168, 183, 114, 1)', // Light gold
        ];
        let colorIndex = 0;
        
        Object.entries(countryData).forEach(([countryName, data]) => {
            // Use explicit null check to preserve 0 values (Spain has 0 in 2050)
            const values = labels.map(year => {
                const val = data.values[year];
                return (val !== null && val !== undefined) ? val : null;
            });
            
            const color = colors[colorIndex % colors.length];
            datasets.push({
                label: countryName,
                data: values,
                borderColor: color,
                backgroundColor: color.replace('1)', '0.2)'),
                tension: 0.1,
                fill: false,
                borderWidth: 2
            });
            colorIndex++;
        });
        
        return {
            title: category.title,
            labels: labels,
            datasets: datasets,
            unitLabel: unitLabel
        };
    } catch (error) {
        console.error('Error building GHG chart:', error);
        return null;
    }
}

// Build comparison chart data from tables
async function buildComparisonChartData(tables, title) {
    try {
        const countryData = {};
        
        for (const table of tables) {
            const { data: points, error } = await supabase
                .from('data_points')
                .select('row_data')
                .eq('data_table_id', table.id)
                .limit(100);
            
            if (error || !points || points.length === 0) continue;
            
            const countryName = table.countries?.name || 'Unknown';
            countryData[countryName] = {
                code: table.countries?.code || '',
                data: points,
                description: table.table_description
            };
        }
        
        if (Object.keys(countryData).length < 2) return null;
        
        // Extract time columns
        const firstCountry = Object.values(countryData)[0];
        const headers = Object.keys(firstCountry.data[0].row_data);
        const timeColumns = headers.filter(h => /\b(19|20)\d{2}\b/.test(h) || (h.includes('-') && /\d{4}/.test(h)));
        
        if (timeColumns.length === 0) return null;
        
        const labels = timeColumns.map(col => {
            const yearMatch = col.match(/\b(19|20)\d{2}\b/);
            if (yearMatch) return yearMatch[0];
            const rangeMatch = col.match(/(\d{4})-(\d{4})/);
            if (rangeMatch) return rangeMatch[1] + '-' + rangeMatch[2].slice(-2);
            return col;
        });
        
        const datasets = [];
        const colors = [
            'rgba(197, 183, 114, 1)', // E3G gold
            'rgba(168, 196, 216, 1)', // E3G blue
            'rgba(123, 140, 80, 1)',  // E3G olive
            'rgba(50, 48, 103, 1)',   // E3G dark blue
            'rgba(139, 172, 196, 1)', // Light blue
            'rgba(168, 183, 114, 1)', // Light gold
        ];
        let colorIndex = 0;
        
        Object.entries(countryData).forEach(([countryName, country]) => {
            // Aggregate values for this country
            const values = [];
            timeColumns.forEach(col => {
                let sum = 0;
                let count = 0;
                country.data.forEach(row => {
                    const val = parseFloat((row.row_data[col] || '0').toString().replace(/[%,]/g, ''));
                    if (!isNaN(val)) {
                        sum += Math.abs(val); // Use absolute value for aggregation
                        count++;
                    }
                });
                values.push(count > 0 ? sum / count : 0);
            });
            
            const color = colors[colorIndex % colors.length];
            datasets.push({
                label: countryName,
                data: values,
                borderColor: color,
                backgroundColor: color.replace('1)', '0.2)'),
                tension: 0.1,
                fill: false
            });
            colorIndex++;
        });
        
        return {
            title: title,
            labels: labels,
            datasets: datasets
        };
    } catch (error) {
        console.error('Error building chart data:', error);
        return null;
    }
}

// Render dashboard chart
function renderDashboardChart(canvasId, chartData) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    
    // Destroy previous chart if exists
    const existingChart = Chart.getChart(canvas);
    if (existingChart) {
        existingChart.destroy();
    }
    
    const ctx = canvas.getContext('2d');
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: chartData.labels,
            datasets: chartData.datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'right',
                    labels: {
                        boxWidth: 12,
                        padding: 8
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    title: {
                        display: true,
                        text: chartData.unitLabel || 'Value',
                        font: { size: 11 }
                    },
                    ticks: {
                        font: { size: 10 }
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Time Period',
                        font: { size: 11 }
                    },
                    ticks: {
                        font: { size: 10 },
                        maxRotation: 45,
                        minRotation: 0
                    }
                }
            }
        }
    });
}

// Load countries grid
async function loadCountriesGrid() {
    try {
        const { data: countries, error } = await supabase
            .from('countries')
            .select('*')
            .order('name');
        
        if (error) throw error;
        
        const container = document.getElementById('countriesGrid');
        container.innerHTML = '';
        
        for (const country of countries) {
            // Get stats for each country
            const [tablesData, measuresData] = await Promise.all([
                supabase.from('data_tables').select('id, has_time_series').eq('country_id', country.id),
                supabase.from('measures').select('id').eq('country_id', country.id)
            ]);
            
            const tables = tablesData.data || [];
            const measures = measuresData.data || [];
            const timeSeriesCount = tables.filter(t => t.has_time_series).length;
            
            const card = document.createElement('div');
            card.className = 'country-card';
            card.onclick = () => navigateToCountry(country.id, country.name);
            card.innerHTML = `
                <div class="country-card-header">
                    <div class="country-card-name">${country.name}</div>
                    <div class="country-card-code">${country.code}</div>
                </div>
                <div class="country-card-stats">
                    <div class="country-stat">
                        <div class="country-stat-value">${tables.length}</div>
                        <div class="country-stat-label">Tables</div>
                    </div>
                    <div class="country-stat">
                        <div class="country-stat-value">${timeSeriesCount}</div>
                        <div class="country-stat-label">Time-Series</div>
                    </div>
                    <div class="country-stat">
                        <div class="country-stat-value">${measures.length}</div>
                        <div class="country-stat-label">Measures</div>
                    </div>
                </div>
            `;
            container.appendChild(card);
        }
    } catch (error) {
        console.error('Error loading countries grid:', error);
    }
}

// Load country navigation
// The sidebar "Countries" section was removed (countries are accessible via the
// dashboard's Countries Overview grid). This function is kept as a no-op in case
// the container is re-introduced later.
async function loadCountryNavigation() {
    const container = document.getElementById('countryNavList');
    if (!container) return;
    try {
        const { data: countries, error } = await supabase
            .from('countries')
            .select('*')
            .order('name');

        if (error) throw error;

        container.innerHTML = '';
        countries.forEach(country => {
            const item = document.createElement('div');
            item.className = 'country-nav-item';
            item.textContent = country.name;
            item.onclick = () => navigateToCountry(country.id, country.name);
            container.appendChild(item);
        });
    } catch (error) {
        console.error('Error loading country navigation:', error);
    }
}

// Navigate to country
function navigateToCountry(countryId, countryName) {
    currentCountryId = countryId;
    currentCountryData = { id: countryId, name: countryName };
    
    // Update breadcrumb
    document.getElementById('countryBreadcrumb').textContent = countryName;
    document.getElementById('countryPageTitle').textContent = `${countryName} - Renovation Plan Data`;
    
    // Navigate to country page
    navigateToPage('country', countryId);
    
    // Update active nav item
    document.querySelectorAll('.country-nav-item').forEach(item => {
        item.classList.remove('active');
        if (item.textContent === countryName) {
            item.classList.add('active');
        }
    });
    
    // Load country data
    loadCountryPage(countryId);

    // Persist with the country name so a later reload can restore breadcrumb
    // and sidebar highlight without a round-trip to the DB.
    saveLastPageState('country', countryId, countryName);
}

// Load country page
async function loadCountryPage(countryId) {
    try {
        showLoading(true);
        
        // Load overview stats
        const [tablesData, measuresData] = await Promise.all([
            supabase.from('data_tables').select('id, has_time_series').eq('country_id', countryId),
            supabase.from('measures').select('id').eq('country_id', countryId)
        ]);
        
        const tables = tablesData.data || [];
        const measures = measuresData.data || [];
        const timeSeriesCount = tables.filter(t => t.has_time_series).length;
        
        document.getElementById('countryTableCount').textContent = tables.length;
        document.getElementById('countryTimeSeriesCount').textContent = timeSeriesCount;
        document.getElementById('countryMeasuresCount').textContent = measures.length;
        
        // Load data tables if on that tab
        const activeTab = document.querySelector('.tab-btn.active')?.getAttribute('data-tab');
        if (activeTab === 'data-tables') {
            await loadDataTables(countryId);
        } else if (activeTab === 'measures') {
            await loadMeasures(countryId);
        } else if (activeTab === 'stakeholders') {
            await loadStakeholderMapping(countryId);
        } else if (activeTab === 'analysis') {
            await loadComparisonTables();
        } else if (activeTab === 'overview') {
            // Load overview charts
            await loadCountryOverviewCharts(countryId);
        }
        
        showLoading(false);
    } catch (error) {
        console.error('Error loading country page:', error);
        showError('Failed to load country data');
        showLoading(false);
    }
}

// Load data tables
async function loadDataTables(countryId) {
    try {
        const { data: tables, error } = await supabase
            .from('data_tables')
            .select('*')
            .eq('country_id', countryId)
            .order('table_name');
        
        if (error) throw error;
        
        window.currentTables = tables;
        renderTables(tables, document.getElementById('dataTablesList'));
        
        // Setup search and filter
        setupTableFilters();
    } catch (error) {
        console.error('Error loading data tables:', error);
    }
}

// Render tables
function renderTables(tables, container) {
    container.innerHTML = '';
    
    if (tables.length === 0) {
        container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No data tables available</p>';
        return;
    }
    
    tables.forEach(table => {
        const card = document.createElement('div');
        card.className = 'table-card';
        
        const badge = table.has_time_series 
            ? '<span class="badge badge-time-series">Time-Series</span>' 
            : '<span class="badge badge-regular">Regular</span>';
        
        card.innerHTML = `
            <div class="table-card-header">
                <div style="flex: 1;">
                    ${badge}
                    <div class="table-card-title">${table.table_description || table.table_name.replace(/_/g, ' ')}</div>
                    ${table.table_number ? `<div class="table-card-meta">Table ${table.table_number}</div>` : ''}
                    <div class="table-card-meta">${table.num_columns || 0} columns</div>
                </div>
            </div>
            <button class="view-btn" onclick="viewTableData(${table.id}, '${escapeHtml(table.table_name)}')">
                View Data
            </button>
        `;
        container.appendChild(card);
    });
}

// Setup table filters
function setupTableFilters() {
    const searchInput = document.getElementById('tableSearch');
    const filterSelect = document.getElementById('tableFilter');
    
    if (searchInput) {
        searchInput.oninput = (e) => filterTables(e.target.value, filterSelect.value);
    }
    
    if (filterSelect) {
        filterSelect.onchange = (e) => filterTables(searchInput?.value || '', e.target.value);
    }
}

// Filter tables
function filterTables(searchTerm, filterType) {
    if (!window.currentTables) return;
    
    let filtered = window.currentTables;
    
    if (searchTerm) {
        const term = searchTerm.toLowerCase();
        filtered = filtered.filter(table => 
            (table.table_description || '').toLowerCase().includes(term) ||
            (table.table_name || '').toLowerCase().includes(term) ||
            (table.table_number || '').includes(term)
        );
    }
    
    if (filterType === 'time-series') {
        filtered = filtered.filter(table => table.has_time_series);
    } else if (filterType === 'regular') {
        filtered = filtered.filter(table => !table.has_time_series);
    }
    
    renderTables(filtered, document.getElementById('dataTablesList'));
}

// View table data (global function)
window.viewTableData = async function(tableId, tableName) {
    try {
        showLoading(true);
        
        // Get table metadata
        const { data: tableMeta, error: metaError } = await supabase
            .from('data_tables')
            .select('*')
            .eq('id', tableId)
            .single();
        
        if (metaError) throw metaError;
        
        // Get data points
        const { data, error } = await supabase
            .from('data_points')
            .select('row_data')
            .eq('data_table_id', tableId)
            .limit(500);
        
        if (error) throw error;
        
        currentTableData = data;
        currentTableMetadata = tableMeta;
        currentViewMode = 'table';
        
        // Show modal
        document.getElementById('modalTitle').textContent = tableMeta.table_description || tableName.replace(/_/g, ' ');
        document.getElementById('dataModal').classList.add('active');
        
        const container = document.getElementById('dataVisualization');
        const toggleBtn = document.getElementById('toggleView');
        
        if (tableMeta.has_time_series) {
            toggleBtn.style.display = 'inline-block';
            toggleBtn.textContent = 'Switch to Chart View';
        } else {
            toggleBtn.style.display = 'none';
        }
        
        renderTableView(data, tableMeta, container);
        
        showLoading(false);
    } catch (error) {
        console.error('Error loading table data:', error);
        showError('Failed to load table data');
        showLoading(false);
    }
};

// Render table view
function renderTableView(data, metadata, container) {
    container.innerHTML = '';
    
    if (metadata.table_number) {
        container.innerHTML += `<p class="table-meta">Table ${metadata.table_number} | ${data.length} rows</p>`;
    }
    
    const table = document.createElement('table');
    table.className = 'data-table';
    
    const headers = Object.keys(data[0].row_data);
    const headerRow = document.createElement('tr');
    headers.forEach(header => {
        const th = document.createElement('th');
        th.textContent = header;
        th.onclick = () => sortTable(table, Array.from(headers).indexOf(header));
        headerRow.appendChild(th);
    });
    table.appendChild(headerRow);
    
    data.forEach(point => {
        const row = document.createElement('tr');
        headers.forEach(header => {
            const td = document.createElement('td');
            const value = point.row_data[header] || '';
            
            if (value && !isNaN(value.toString().replace(/[%,]/g, ''))) {
                td.className = 'numeric';
                td.textContent = formatNumber(value);
            } else {
                td.textContent = value;
            }
            
            row.appendChild(td);
        });
        table.appendChild(row);
    });
    
    container.appendChild(table);
}

// Render chart view
function renderChartView(data, metadata, container) {
    container.innerHTML = '';
    
    // Destroy previous chart if it exists
    if (currentChart) {
        currentChart.destroy();
        currentChart = null;
    }
    
    if (metadata.table_number) {
        container.innerHTML += `<p class="table-meta">Table ${metadata.table_number} | ${data.length} rows</p>`;
    }
    
    if (!data || data.length === 0) {
        container.innerHTML += '<p>No data available for chart</p>';
        return;
    }
    
    const headers = Object.keys(data[0].row_data);
    const timeColumns = headers.filter(h => /\b(19|20)\d{2}\b/.test(h) || (h.includes('-') && /\d{4}/.test(h)));
    const nonTimeColumns = headers.filter(h => !timeColumns.includes(h));
    
    if (timeColumns.length === 0) {
        container.innerHTML += '<p>No time-series columns detected. Showing data as table.</p>';
        renderTableView(data, metadata, container);
        return;
    }
    
    const chartContainer = document.createElement('div');
    chartContainer.style.marginTop = '20px';
    chartContainer.style.position = 'relative';
    chartContainer.style.height = '400px';
    const canvas = document.createElement('canvas');
    canvas.id = 'timeSeriesChart';
    chartContainer.appendChild(canvas);
    container.appendChild(chartContainer);
    
    const labels = timeColumns.map(col => {
        const yearMatch = col.match(/\b(19|20)\d{2}\b/);
        if (yearMatch) return yearMatch[0];
        // Handle ranges like "2024-2030"
        const rangeMatch = col.match(/(\d{4})-(\d{4})/);
        if (rangeMatch) return rangeMatch[1] + '-' + rangeMatch[2].slice(-2);
        return col;
    });
    
    const groupedData = {};
    const colors = [
        'rgba(197, 183, 114, 1)', // E3G gold
        'rgba(168, 196, 216, 1)', // E3G blue
        'rgba(123, 140, 80, 1)',  // E3G olive
        'rgba(50, 48, 103, 1)',   // E3G dark blue
    ];
    let colorIndex = 0;
    
    data.forEach(row => {
        const key = nonTimeColumns.map(col => row.row_data[col]).join(' | ') || 'Data';
        if (!groupedData[key]) {
            const color = colors[colorIndex % colors.length];
            groupedData[key] = {
                label: key.length > 50 ? key.substring(0, 50) + '...' : key,
                data: [],
                borderColor: color,
                backgroundColor: color.replace('1)', '0.2)'),
                tension: 0.1,
                fill: false
            };
            colorIndex++;
        }
        timeColumns.forEach(col => {
            const value = parseFloat((row.row_data[col] || '0').toString().replace(/[%,]/g, ''));
            groupedData[key].data.push(isNaN(value) ? 0 : value);
        });
    });
    
    const ctx = canvas.getContext('2d');
    currentChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: Object.values(groupedData)
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: metadata.table_description || 'Time Series Data',
                    font: { size: 16, weight: 'bold' }
                },
                legend: {
                    display: true,
                    position: 'right',
                    labels: {
                        boxWidth: 12,
                        padding: 10
                    }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    title: {
                        display: true,
                        text: 'Value'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Time Period'
                    }
                }
            },
            interaction: {
                mode: 'nearest',
                axis: 'x',
                intersect: false
            }
        }
    });
}

// Toggle view mode
window.toggleViewMode = function() {
    if (!currentTableData || !currentTableMetadata) return;
    
    const container = document.getElementById('dataVisualization');
    const toggleBtn = document.getElementById('toggleView');
    
    if (currentViewMode === 'table') {
        currentViewMode = 'chart';
        toggleBtn.textContent = 'Switch to Table View';
        renderChartView(currentTableData, currentTableMetadata, container);
    } else {
        currentViewMode = 'table';
        toggleBtn.textContent = 'Switch to Chart View';
        renderTableView(currentTableData, currentTableMetadata, container);
    }
};

// Load measures
async function loadMeasures(countryId) {
    try {
        const { data, error } = await supabase
            .from('measures')
            .select('*')
            .eq('country_id', countryId)
            .order('measure_category, measure_id');
        
        if (error) throw error;
        
        const container = document.getElementById('measuresList');
        container.innerHTML = '';
        
        if (data.length === 0) {
            container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No policy measures available</p>';
            return;
        }
        
        const grouped = {};
        data.forEach(measure => {
            const category = measure.measure_category || 'Other';
            if (!grouped[category]) {
                grouped[category] = [];
            }
            grouped[category].push(measure);
        });
        
        // Store data globally for filtering
        window.currentMeasures = data;
        window.currentMeasuresGrouped = grouped;
        
        // Render measures
        renderMeasures(grouped);
        
        // Setup search functionality
        setupMeasuresSearch();
    } catch (error) {
        console.error('Error loading measures:', error);
    }
}

// Render measures (can be called with filtered data)
function renderMeasures(grouped) {
    const container = document.getElementById('measuresList');
    container.innerHTML = '';
    
    if (!grouped || Object.keys(grouped).length === 0) {
        container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No policy measures found</p>';
        return;
    }
    
    Object.keys(grouped).forEach(category => {
        const categoryDiv = document.createElement('div');
        categoryDiv.className = 'measure-category';
        categoryDiv.innerHTML = `<div class="measure-category-title">${category}</div>`;
        
        grouped[category].forEach(measure => {
            const status = getMeasureStatus(measure);
            const measureDiv = document.createElement('div');
            measureDiv.className = 'measure-item';
            measureDiv.style.cursor = 'pointer';
            measureDiv.onclick = () => showMeasureDetail(measure);
            measureDiv.innerHTML = `
                <div class="measure-header">
                    <span class="measure-id">${measure.measure_id || ''}</span>
                    <strong class="measure-name">${measure.measure_name || 'Unnamed Measure'}</strong>
                    <span class="measure-status ${status.class}">${status.label}</span>
                </div>
                ${measure.description ? `<p class="measure-description">${truncateText(unwrapJsonDisplay(measure.description), 200)}</p>` : ''}
                ${measure.implementation_period ? `<p class="measure-period">📅 ${measure.implementation_period}</p>` : ''}
                <p class="measure-click-hint" style="margin-top: 8px; font-size: 12px; color: var(--text-tertiary); font-style: italic;">Click to view full details</p>
            `;
            categoryDiv.appendChild(measureDiv);
        });
        
        container.appendChild(categoryDiv);
    });
}

// Setup measures search
function setupMeasuresSearch() {
    const searchInput = document.getElementById('measuresSearch');
    if (searchInput) {
        searchInput.oninput = (e) => filterMeasures(e.target.value);
    }
}

// Filter measures
function filterMeasures(searchTerm) {
    if (!window.currentMeasures || !window.currentMeasuresGrouped) return;
    
    const term = searchTerm.toLowerCase().trim();
    let filteredGrouped = {};
    
    if (!term) {
        // Show all if search is empty
        filteredGrouped = window.currentMeasuresGrouped;
    } else {
        // Filter measures
        Object.keys(window.currentMeasuresGrouped).forEach(category => {
            const filtered = window.currentMeasuresGrouped[category].filter(measure => {
                const name = (measure.measure_name || '').toLowerCase();
                const id = (measure.measure_id || '').toLowerCase();
                const description = (measure.description || '').toLowerCase();
                const categoryName = (measure.measure_category || '').toLowerCase();
                
                return name.includes(term) || 
                       id.includes(term) || 
                       description.includes(term) ||
                       categoryName.includes(term);
            });
            
            if (filtered.length > 0) {
                filteredGrouped[category] = filtered;
            }
        });
    }
    
    renderMeasures(filteredGrouped);
}

// Setup stakeholder matrix search
function setupStakeholderMatrixSearch() {
    const searchInput = document.getElementById('stakeholderMatrixSearch');
    if (searchInput) {
        searchInput.oninput = (e) => filterStakeholderMatrix(e.target.value);
    }
}

// Filter stakeholder matrix
function filterStakeholderMatrix(searchTerm) {
    if (!window.currentStakeholderMeasures) return;
    
    const term = searchTerm.toLowerCase().trim();
    let filtered = {};
    
    if (!term) {
        // Show all if search is empty
        filtered = window.currentStakeholderMeasures;
    } else {
        // Filter measures
        Object.entries(window.currentStakeholderMeasures).forEach(([measureKey, data]) => {
            const name = (data.measureName || '').toLowerCase();
            const id = (data.measureId || '').toLowerCase();
            const category = (data.category || '').toLowerCase();
            const stakeholders = data.stakeholders.map(sh => sh.toLowerCase()).join(' ');
            
            if (name.includes(term) || 
                id.includes(term) || 
                category.includes(term) ||
                stakeholders.includes(term)) {
                filtered[measureKey] = data;
            }
        });
    }
    
    renderStakeholderMatrix(filtered, window.currentStakeholderMap);
}

// Load stakeholder mapping
async function loadStakeholderMapping(countryId) {
    try {
        showLoading(true);
        
        // Load all measures for this country (include all columns)
        const { data: measures, error } = await supabase
            .from('measures')
            .select('*')
            .eq('country_id', countryId)
            .order('measure_category, measure_id');
        
        if (error) throw error;
        
        if (!measures || measures.length === 0) {
            document.getElementById('stakeholderSummary').innerHTML = 
                '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No measures available for stakeholder mapping</p>';
            document.getElementById('stakeholderChartContainer').innerHTML = '';
            document.getElementById('stakeholderMatrixContent').innerHTML = '';
            showLoading(false);
            return;
        }
        
        // Parse stakeholders and build mapping
        const stakeholderMap = {}; // stakeholder -> [measures]
        const measureStakeholderMap = {}; // measure_id -> [stakeholders]
        
        measures.forEach(measure => {
            // Priority order: participating_institutions (Bulgaria) > authorities_responsible (Croatia) > entities_responsible > stakeholders
            const stakeholderText = measure.participating_institutions || 
                                    measure.authorities_responsible || 
                                    measure.entities_responsible || 
                                    measure.stakeholders || '';
            const stakeholders = parseStakeholders(stakeholderText);
            const measureKey = `${measure.measure_id || measure.id} - ${measure.measure_name || 'Unnamed'}`;
            
            measureStakeholderMap[measureKey] = {
                stakeholders: stakeholders,
                category: measure.measure_category || 'Other',
                measureId: measure.measure_id || '',
                measureName: measure.measure_name || 'Unnamed Measure'
            };
            
            stakeholders.forEach(stakeholder => {
                if (!stakeholderMap[stakeholder]) {
                    stakeholderMap[stakeholder] = [];
                }
                stakeholderMap[stakeholder].push({
                    id: measure.measure_id || measure.id,
                    name: measure.measure_name || 'Unnamed Measure',
                    category: measure.measure_category || 'Other'
                });
            });
        });
        
        // Store data globally for filtering
        window.currentStakeholderMeasures = measureStakeholderMap;
        window.currentStakeholderMap = stakeholderMap;
        
        // Render summary
        renderStakeholderSummary(stakeholderMap, measures.length);
        
        // Render chart
        renderStakeholderChart(stakeholderMap);
        
        // Render matrix
        renderStakeholderMatrix(measureStakeholderMap, stakeholderMap);
        
        // Setup search functionality
        setupStakeholderMatrixSearch();
        
        showLoading(false);
    } catch (error) {
        console.error('Error loading stakeholder mapping:', error);
        showError('Failed to load stakeholder mapping');
        showLoading(false);
    }
}

// Parse stakeholders from text (handles comma, semicolon, and newline separators)
function parseStakeholders(stakeholdersText) {
    if (!stakeholdersText || !stakeholdersText.trim()) {
        return [];
    }
    
    // Split by common delimiters (semicolon is common for Bulgaria and Croatia)
    const stakeholders = stakeholdersText
        .split(/[,;\n\r|]/)
        .map(s => s.trim())
        .filter(s => s.length > 0);
    
    return stakeholders;
}

// Render stakeholder summary
function renderStakeholderSummary(stakeholderMap, totalMeasures) {
    const summaryContainer = document.getElementById('stakeholderSummary');
    const uniqueStakeholders = Object.keys(stakeholderMap).length;
    const totalStakeholderAssignments = Object.values(stakeholderMap).reduce((sum, measures) => sum + measures.length, 0);
    
    summaryContainer.innerHTML = `
        <div class="stats-grid" style="grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));">
            <div class="stat-card">
                <div class="stat-icon">👥</div>
                <div class="stat-content">
                    <div class="stat-value">${uniqueStakeholders}</div>
                    <div class="stat-label">Unique Stakeholders</div>
                </div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">📋</div>
                <div class="stat-content">
                    <div class="stat-value">${totalMeasures}</div>
                    <div class="stat-label">Total Measures</div>
                </div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">🔗</div>
                <div class="stat-content">
                    <div class="stat-value">${totalStakeholderAssignments}</div>
                    <div class="stat-label">Stakeholder Assignments</div>
                </div>
            </div>
            <div class="stat-card">
                <div class="stat-icon">📊</div>
                <div class="stat-content">
                    <div class="stat-value">${(totalStakeholderAssignments / totalMeasures).toFixed(1)}</div>
                    <div class="stat-label">Avg. Stakeholders per Measure</div>
                </div>
            </div>
        </div>
    `;
}

// Render stakeholder chart
function renderStakeholderChart(stakeholderMap) {
    // Sort stakeholders by measure count
    const sortedStakeholders = Object.entries(stakeholderMap)
        .map(([name, measures]) => ({ name, count: measures.length }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 20); // Top 20 stakeholders
    
    if (sortedStakeholders.length === 0) {
        document.getElementById('stakeholderChartContainer').innerHTML = 
            '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No stakeholder data available</p>';
        return;
    }
    
    const labels = sortedStakeholders.map(s => s.name.length > 30 ? s.name.substring(0, 27) + '...' : s.name);
    const data = sortedStakeholders.map(s => s.count);
    
    // Destroy previous chart if exists
    const canvas = document.getElementById('stakeholderChart');
    if (!canvas) return;
    
    const existingChart = Chart.getChart(canvas);
    if (existingChart) {
        existingChart.destroy();
    }
    
    const ctx = canvas.getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Number of Measures',
                data: data,
                backgroundColor: 'rgba(197, 183, 114, 0.8)', // E3G gold
                borderColor: 'rgba(197, 183, 114, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        afterLabel: function(context) {
                            const stakeholderName = sortedStakeholders[context.dataIndex].name;
                            const measures = stakeholderMap[stakeholderName];
                            return `Measures: ${measures.map(m => m.name).join(', ').substring(0, 100)}${measures.length > 0 ? '...' : ''}`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Number of Measures',
                        font: { size: 12 }
                    },
                    ticks: {
                        stepSize: 1
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Stakeholder',
                        font: { size: 12 }
                    },
                    ticks: {
                        font: { size: 10 },
                        maxRotation: 45,
                        minRotation: 0
                    }
                }
            }
        }
    });
}

// Render stakeholder-measure matrix
function renderStakeholderMatrix(measureStakeholderMap, stakeholderMap) {
    const matrixContainer = document.getElementById('stakeholderMatrixContent');
    
    if (!measureStakeholderMap || Object.keys(measureStakeholderMap).length === 0) {
        matrixContainer.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No stakeholder data available</p>';
        return;
    }
    
    // Get all unique stakeholders sorted by measure count (for reference, but we'll use the filtered map)
    const stakeholders = Object.keys(stakeholderMap || {})
        .sort((a, b) => (stakeholderMap[b]?.length || 0) - (stakeholderMap[a]?.length || 0));
    
    // Create table
    let html = `
        <div style="overflow-x: auto;">
            <table class="data-table" style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr>
                        <th style="text-align: left; padding: 12px; border-bottom: 2px solid var(--border-color); background: var(--bg-secondary); position: sticky; left: 0; z-index: 10;">
                            Measure
                        </th>
                        <th style="text-align: left; padding: 12px; border-bottom: 2px solid var(--border-color); background: var(--bg-secondary);">
                            Category
                        </th>
                        <th style="text-align: left; padding: 12px; border-bottom: 2px solid var(--border-color); background: var(--bg-secondary);">
                            Stakeholders
                        </th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    Object.entries(measureStakeholderMap).forEach(([measureKey, data]) => {
        const hasStakeholders = data.stakeholders && data.stakeholders.length > 0;
        
        html += `
            <tr style="border-bottom: 1px solid var(--border-color);">
                <td style="padding: 12px; font-weight: 500; position: sticky; left: 0; background: var(--bg-primary);">
                    ${data.measureId ? `<span style="color: var(--text-tertiary); font-size: 12px;">${data.measureId}</span><br>` : ''}
                    ${escapeHtml(data.measureName)}
                </td>
                <td style="padding: 12px;">
                    <span class="badge badge-regular">${escapeHtml(data.category)}</span>
                </td>
                <td style="padding: 12px;">
                    ${hasStakeholders ? `
                        <div style="display: flex; flex-wrap: wrap; gap: 6px;">
                            ${data.stakeholders.map(sh => 
                                `<span style="background: var(--accent-color); color: var(--primary-color); padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 500;">${escapeHtml(sh)}</span>`
                            ).join('')}
                        </div>
                    ` : '<span style="color: var(--text-tertiary); font-style: italic;">Not specified</span>'}
                </td>
            </tr>
        `;
    });
    
    html += `
                </tbody>
            </table>
        </div>
    `;
    
    matrixContainer.innerHTML = html;
}

// Get measure status
function getMeasureStatus(measure) {
    // First check if there's an explicit status field (Finland)
    if (measure.status) {
        const statusLower = measure.status.toLowerCase();
        if (statusLower.includes('ongoing') || statusLower.includes('active') || statusLower.includes('in progress')) {
            return { class: 'status-ongoing', label: measure.status };
        } else if (statusLower.includes('completed') || statusLower.includes('finished') || statusLower.includes('done')) {
            return { class: 'status-completed', label: measure.status };
        } else if (statusLower.includes('planned') || statusLower.includes('future')) {
            return { class: 'status-planned', label: measure.status };
        }
    }
    
    // Check state_of_execution field (Lithuania/Slovenia/Spain)
    if (measure.state_of_execution) {
        const stateLower = measure.state_of_execution.toLowerCase();
        if (stateLower.includes('ongoing') || stateLower.includes('active') || stateLower.includes('in progress') || stateLower.includes('implementation')) {
            return { class: 'status-ongoing', label: measure.state_of_execution };
        } else if (stateLower.includes('completed') || stateLower.includes('finished') || stateLower.includes('done')) {
            return { class: 'status-completed', label: measure.state_of_execution };
        } else if (stateLower.includes('planned') || stateLower.includes('future')) {
            return { class: 'status-planned', label: measure.state_of_execution };
        }
    }
    
    // Fall back to implementation_period analysis
    const period = measure.implementation_period || measure.time_limit || '';
    const now = new Date();
    const currentYear = now.getFullYear();
    
    // Check if period contains years
    const yearMatch = period.match(/\b(20\d{2})\b/g);
    
    if (!yearMatch || yearMatch.length === 0) {
        return { class: 'status-unknown', label: 'Status Unknown' };
    }
    
    const years = yearMatch.map(y => parseInt(y)).sort((a, b) => a - b);
    const startYear = years[0];
    const endYear = years[years.length - 1];
    
    if (currentYear < startYear) {
        return { class: 'status-planned', label: 'Planned' };
    } else if (currentYear >= startYear && currentYear <= endYear) {
        return { class: 'status-ongoing', label: 'Ongoing' };
    } else {
        return { class: 'status-completed', label: 'Completed' };
    }
}

// Show measure detail modal
function showMeasureDetail(measure) {
    const modal = document.getElementById('measureModal');
    const title = document.getElementById('measureModalTitle');
    const content = document.getElementById('measureDetailContent');
    // Unwrap any measure fields stored as JSON strings (e.g. Lithuania expected_impact)
    const m = { ...measure };
    for (const key of Object.keys(m)) {
        if (typeof m[key] === 'string') m[key] = unwrapJsonDisplay(m[key]);
    }
    // Build Additional Information HTML once so we never render raw JSON
    const additionalDataHtml = (m.additional_data != null) ? formatAdditionalDataForDisplay(m.additional_data) : '';
    m.additional_data = null; // avoid any chance of raw output in template
    measure = m;
    title.textContent = measure.measure_name || 'Measure Details';
    const status = getMeasureStatus(measure);
    
    content.innerHTML = `
        <div class="measure-detail">
            <div class="measure-detail-header">
                <div>
                    <span class="measure-id">${measure.measure_id || ''}</span>
                    <span class="measure-status ${status.class}" style="margin-left: 12px;">${status.label}</span>
                </div>
                <h3>${measure.measure_name || 'Unnamed Measure'}</h3>
            </div>
            
            ${measure.measure_category ? `
                <div class="detail-section">
                    <h4>Category</h4>
                    <p>${measure.measure_category}</p>
                </div>
            ` : ''}
            
            ${measure.description ? `
                <div class="detail-section">
                    <h4>Description</h4>
                    <p>${measure.description}</p>
                </div>
            ` : ''}
            
            ${measure.quantified_objectives ? `
                <div class="detail-section">
                    <h4>Quantified Objectives</h4>
                    <p>${measure.quantified_objectives}</p>
                </div>
            ` : ''}
            
            ${measure.type_of_policy_or_measure ? `
                <div class="detail-section">
                    <h4>Type of Policy or Measure</h4>
                    <p>${measure.type_of_policy_or_measure}</p>
                </div>
            ` : ''}
            
            ${measure.budget ? `
                <div class="detail-section">
                    <h4>Budget</h4>
                    <p>${measure.budget}</p>
                </div>
            ` : ''}
            
            ${(measure.participating_institutions || measure.authorities_responsible || measure.stakeholders || measure.entities_responsible) ? `
                <div class="detail-section">
                    <h4>Stakeholders / Entities Responsible</h4>
                    <p>${measure.participating_institutions || measure.authorities_responsible || measure.entities_responsible || measure.stakeholders || 'Not specified'}</p>
                </div>
            ` : ''}
            
            ${measure.participating_institutions ? `
                <div class="detail-section">
                    <h4>Participating Institutions</h4>
                    <p>${measure.participating_institutions}</p>
                </div>
            ` : ''}
            
            ${measure.authorities_responsible ? `
                <div class="detail-section">
                    <h4>Authorities Responsible</h4>
                    <p>${measure.authorities_responsible}</p>
                </div>
            ` : ''}
            
            ${measure.state_of_play ? `
                <div class="detail-section">
                    <h4>State of Play</h4>
                    <p>${measure.state_of_play}</p>
                </div>
            ` : ''}
            
            ${measure.implementation_period ? `
                <div class="detail-section">
                    <h4>Implementation Period</h4>
                    <p>${measure.implementation_period}</p>
                </div>
            ` : ''}
            
            ${measure.objective ? `
                <div class="detail-section">
                    <h4>Objective</h4>
                    <p>${measure.objective}</p>
                </div>
            ` : ''}
            
            ${measure.planned_budget_and_sources ? `
                <div class="detail-section">
                    <h4>Planned Budget and Sources</h4>
                    <p>${measure.planned_budget_and_sources}</p>
                </div>
            ` : ''}
            
            ${measure.state_of_execution ? `
                <div class="detail-section">
                    <h4>State of Execution</h4>
                    <p>${measure.state_of_execution}</p>
                </div>
            ` : ''}
            
            ${measure.date_of_entry_into_force ? `
                <div class="detail-section">
                    <h4>Date of Entry into Force</h4>
                    <p>${measure.date_of_entry_into_force}</p>
                </div>
            ` : ''}
            
            ${measure.directive ? `
                <div class="detail-section">
                    <h4>Directive</h4>
                    <p>${measure.directive}</p>
                </div>
            ` : ''}
            
            ${measure.status ? `
                <div class="detail-section">
                    <h4>Status</h4>
                    <p>${measure.status}</p>
                </div>
            ` : ''}
            
            ${measure.epbd_article_2a ? `
                <div class="detail-section">
                    <h4>EPBD Article 2a</h4>
                    <p>${measure.epbd_article_2a}</p>
                </div>
            ` : ''}
            
            ${measure.instrument_type ? `
                <div class="detail-section">
                    <h4>Instrument Type</h4>
                    <p>${measure.instrument_type}</p>
                </div>
            ` : ''}
            
            ${measure.source ? `
                <div class="detail-section">
                    <h4>Source</h4>
                    <p>${measure.source}</p>
                </div>
            ` : ''}
            
            ${measure.quantitative_target ? `
                <div class="detail-section">
                    <h4>Quantitative Target</h4>
                    <p>${measure.quantitative_target}</p>
                </div>
            ` : ''}
            
            ${measure.short_description ? `
                <div class="detail-section">
                    <h4>Short Description</h4>
                    <p>${measure.short_description}</p>
                </div>
            ` : ''}
            
            ${measure.quantified_objective ? `
                <div class="detail-section">
                    <h4>Quantified Objective</h4>
                    <p>${measure.quantified_objective}</p>
                </div>
            ` : ''}
            
            ${measure.authorities_responsible ? `
                <div class="detail-section">
                    <h4>Authorities Responsible</h4>
                    <p>${measure.authorities_responsible}</p>
                </div>
            ` : ''}
            
            ${measure.expected_impacts ? `
                <div class="detail-section">
                    <h4>Expected Impacts</h4>
                    <p>${measure.expected_impacts}</p>
                </div>
            ` : ''}
            
            ${measure.implementation_status ? `
                <div class="detail-section">
                    <h4>Implementation Status</h4>
                    <p>${measure.implementation_status}</p>
                </div>
            ` : ''}
            
            ${measure.effective_date ? `
                <div class="detail-section">
                    <h4>Effective Date</h4>
                    <p>${measure.effective_date}</p>
                </div>
            ` : ''}
            
            ${measure.section ? `
                <div class="detail-section">
                    <h4>Section</h4>
                    <p>${measure.section}</p>
                </div>
            ` : ''}
            
            ${measure.section_topic ? `
                <div class="detail-section">
                    <h4>Section Topic</h4>
                    <p>${measure.section_topic}</p>
                </div>
            ` : ''}
            
            ${measure.measure_number ? `
                <div class="detail-section">
                    <h4>Measure Number</h4>
                    <p>${measure.measure_number}</p>
                </div>
            ` : ''}
            
            ${measure.content ? `
                <div class="detail-section">
                    <h4>Content</h4>
                    <p>${measure.content}</p>
                </div>
            ` : ''}
            
            ${measure.amending_legislation ? `
                <div class="detail-section">
                    <h4>Amending Legislation</h4>
                    <p>${measure.amending_legislation}</p>
                </div>
            ` : ''}
            
            ${measure.lead_institution ? `
                <div class="detail-section">
                    <h4>Lead Institution</h4>
                    <p>${measure.lead_institution}</p>
                </div>
            ` : ''}
            
            ${measure.participating_institutions ? `
                <div class="detail-section">
                    <h4>Participating Institutions</h4>
                    <p>${measure.participating_institutions}</p>
                </div>
            ` : ''}
            
            ${measure.sources_of_funding ? `
                <div class="detail-section">
                    <h4>Sources of Funding</h4>
                    <p>${measure.sources_of_funding}</p>
                </div>
            ` : ''}
            
            ${measure.time_limit ? `
                <div class="detail-section">
                    <h4>Time Limit</h4>
                    <p>${measure.time_limit}</p>
                </div>
            ` : ''}
            
            ${additionalDataHtml ? `
                <div class="detail-section">
                    <h4>Additional Information</h4>
                    <div class="additional-data-content">${additionalDataHtml}</div>
                </div>
            ` : ''}
        </div>
    `;
    
    modal.classList.add('active');
}

// Utility functions
function sortTable(table, columnIndex) {
    const tbody = table.querySelector('tbody') || table;
    const rows = Array.from(tbody.querySelectorAll('tr')).slice(1);
    
    rows.sort((a, b) => {
        const aVal = a.cells[columnIndex].textContent.trim();
        const bVal = b.cells[columnIndex].textContent.trim();
        const aNum = parseFloat(aVal.replace(/[%,]/g, ''));
        const bNum = parseFloat(bVal.replace(/[%,]/g, ''));
        
        if (!isNaN(aNum) && !isNaN(bNum)) {
            return aNum - bNum;
        }
        return aVal.localeCompare(bVal);
    });
    
    rows.forEach(row => tbody.appendChild(row));
}

function formatNumber(value) {
    if (!value) return '';
    const num = parseFloat(value.toString().replace(/[%,]/g, ''));
    if (isNaN(num)) return value;
    
    if (value.toString().includes('%')) {
        return num.toFixed(2) + '%';
    }
    
    if (num >= 1000000) {
        return (num / 1000000).toFixed(2) + 'M';
    }
    if (num >= 1000) {
        return (num / 1000).toFixed(2) + 'K';
    }
    
    return num.toLocaleString();
}

function getRandomColor(alpha = 1) {
    const colors = [
        `rgba(30, 64, 175, ${alpha})`,
        `rgba(14, 165, 233, ${alpha})`,
        `rgba(16, 185, 129, ${alpha})`,
        `rgba(245, 158, 11, ${alpha})`,
        `rgba(239, 68, 68, ${alpha})`,
        `rgba(139, 92, 246, ${alpha})`,
        `rgba(236, 72, 153, ${alpha})`,
        `rgba(59, 130, 246, ${alpha})`
    ];
    return colors[Math.floor(Math.random() * colors.length)];
}

function closeModal() {
    document.getElementById('dataModal').classList.remove('active');
    if (currentChart) {
        currentChart.destroy();
        currentChart = null;
    }
}

function closeMeasureModal() {
    document.getElementById('measureModal').classList.remove('active');
}

function showLoading(show) {
    document.getElementById('loadingIndicator').style.display = show ? 'flex' : 'none';
}

function showError(message) {
    const container = document.querySelector('.page-content');
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-message';
    errorDiv.style.cssText = 'background: #fee; color: #c33; padding: 16px; border-radius: 8px; margin-bottom: 20px;';
    errorDiv.textContent = message;
    container.insertBefore(errorDiv, container.firstChild);
}

function truncateText(text, maxLength) {
    if (!text) return '';
    return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Unwrap values that are stored as JSON strings (e.g. Lithuania measure components: {"expected_impact": "..."})
function unwrapJsonDisplay(value) {
    if (value == null) return '';
    const s = typeof value === 'string' ? value.trim() : String(value);
    if (!s || s[0] !== '{') return s;
    try {
        const parsed = JSON.parse(s);
        if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
            const preferred = parsed.expected_impact ?? parsed.expected_impacts ?? parsed.description ?? parsed.content;
            if (typeof preferred === 'string') return preferred.trim();
            const parts = [];
            for (const k of Object.keys(parsed)) {
                const v = parsed[k];
                if (typeof v === 'string' && v.trim()) parts.push(v.trim());
            }
            if (parts.length) return parts.join('; ');
        }
    } catch (_) { /* try fallback extraction for malformed JSON */ }
    // Fallback: extract "expected_impact": "..." or 'expected_impact': '...' so we never show raw JSON
    const doubleQuoteMatch = s.match(/"expected_impact"\s*:\s*"((?:[^"\\]|\\.)*)"/);
    if (doubleQuoteMatch) return doubleQuoteMatch[1].replace(/\\"/g, '"');
    const singleQuoteMatch = s.match(/'expected_impact'\s*:\s*'((?:[^'\\]|\\.)*)'/);
    if (singleQuoteMatch) return singleQuoteMatch[1].replace(/\\'/g, "'");
    const genericMatch = s.match(/"([^"]+)"\s*:\s*"((?:[^"\\]|\\.)*)"/);
    if (genericMatch) return genericMatch[2].replace(/\\"/g, '"');
    return s;
}

// Format additional_data for display: show simple key-value as readable text, not raw JSON
function formatAdditionalDataForDisplay(additionalData) {
    if (additionalData == null) return '';
    if (typeof additionalData === 'string') {
        const unwrapped = unwrapJsonDisplay(additionalData);
        return unwrapped ? `<p>${escapeHtml(unwrapped)}</p>` : '';
    }
    if (typeof additionalData !== 'object' || Array.isArray(additionalData)) {
        return escapeHtml(String(additionalData));
    }
    const labels = {
        expected_impact: 'Expected impact',
        expected_impacts: 'Expected impacts',
        description: 'Description',
        content: 'Content'
    };
    const entries = [];
    for (const key of Object.keys(additionalData)) {
        const v = additionalData[key];
        if (v == null) continue;
        const text = typeof v === 'string' ? unwrapJsonDisplay(v) : (typeof v === 'object' ? JSON.stringify(v) : String(v));
        if (!text.trim()) continue;
        const label = labels[key] || key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        entries.push(`<p><strong>${escapeHtml(label)}</strong>: ${escapeHtml(text)}</p>`);
    }
    return entries.length ? entries.join('') : '';
}

// Load comparison tables for cross-country analysis
async function loadComparisonTables() {
    try {
        // Get all tables
        const { data: allTables, error } = await supabase
            .from('data_tables')
            .select('table_number, table_name, table_description, has_time_series, country_id')
            .order('table_number');
        
        if (error) throw error;
        
        // Get countries info
        const { data: countries } = await supabase.from('countries').select('id, name');
        const countryMap = {};
        if (countries) {
            countries.forEach(c => { countryMap[c.id] = c.name; });
        }
        
        // Group by table_number to find comparable tables
        const tableGroups = {};
        allTables.forEach(table => {
            const key = table.table_number || table.table_name;
            if (!tableGroups[key]) {
                tableGroups[key] = [];
            }
            tableGroups[key].push({
                ...table,
                country_name: countryMap[table.country_id] || 'Unknown'
            });
        });
        
        // Filter to only tables that exist in multiple countries and have time-series
        const comparableTables = Object.entries(tableGroups)
            .filter(([key, tables]) => tables.length > 1 && tables.some(t => t.has_time_series))
            .map(([key, tables]) => ({
                table_number: key,
                table_name: tables[0].table_name,
                table_description: tables[0].table_description,
                countries: tables.map(t => ({ id: t.country_id, name: t.country_name }))
            }));
        
        const select = document.getElementById('comparisonTableSelect');
        select.innerHTML = '<option value="">Select table to compare...</option>';
        
        comparableTables.forEach(table => {
            const option = document.createElement('option');
            option.value = table.table_number;
            option.textContent = `${table.table_description || table.table_name} (${table.countries.length} countries)`;
            select.appendChild(option);
        });
    } catch (error) {
        console.error('Error loading comparison tables:', error);
    }
}

// Load cross-country comparison chart
async function loadCrossCountryComparison(tableNumber) {
    try {
        showLoading(true);
        const container = document.getElementById('comparisonChartContainer');
        container.innerHTML = '<div class="chart-loading">Loading comparison...</div>';
        
        // Get all tables with this number
        const { data: tables, error } = await supabase
            .from('data_tables')
            .select('id, country_id, table_name, table_description')
            .eq('table_number', tableNumber);
        
        if (error) {
            console.error('Error loading tables:', error);
            container.innerHTML = '<div class="chart-error">Error loading data: ' + error.message + '</div>';
            showLoading(false);
            return;
        }
        
        if (!tables || tables.length === 0) {
            container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No comparable data found for this table across countries.</p>';
            showLoading(false);
            return;
        }
        
        // Get countries info
        const { data: countries, error: countriesError } = await supabase.from('countries').select('id, name, code');
        if (countriesError) {
            console.error('Error loading countries:', countriesError);
        }
        
        const countryMap = {};
        if (countries) {
            countries.forEach(c => { 
                countryMap[c.id] = { name: c.name, code: c.code }; 
            });
        }
        
        // Get data for each country
        const countryData = {};
        for (const table of tables) {
            const { data: points, error: pointsError } = await supabase
                .from('data_points')
                .select('row_data')
                .eq('data_table_id', table.id)
                .limit(100);
            
            if (!pointsError && points && points.length > 0) {
                const countryInfo = countryMap[table.country_id] || { name: 'Unknown', code: '' };
                countryData[countryInfo.name] = {
                    code: countryInfo.code,
                    data: points,
                    description: table.table_description
                };
            }
        }
        
        if (Object.keys(countryData).length === 0) {
            container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No data points found for comparison.</p>';
            showLoading(false);
            return;
        }
        
        // Render comparison chart
        renderComparisonChart(countryData, tables[0].table_description);
        
        showLoading(false);
    } catch (error) {
        console.error('Error loading comparison:', error);
        document.getElementById('comparisonChartContainer').innerHTML = 
            '<div class="chart-error">Failed to load cross-country comparison: ' + error.message + '</div>';
        showLoading(false);
    }
}

// Render cross-country comparison chart
function renderComparisonChart(countryData, tableDescription) {
    const container = document.getElementById('comparisonChartContainer');
    container.innerHTML = '';
    
    // Destroy previous chart if exists
    if (currentChart) {
        currentChart.destroy();
        currentChart = null;
    }
    
    // Find common time columns across all countries
    const allTimeColumns = new Set();
    Object.values(countryData).forEach(country => {
        if (country.data && country.data.length > 0) {
            const headers = Object.keys(country.data[0].row_data);
            headers.forEach(h => {
                if (/\b(19|20)\d{2}\b/.test(h) || (h.includes('-') && /\d{4}/.test(h))) {
                    allTimeColumns.add(h);
                }
            });
        }
    });
    
    if (allTimeColumns.size === 0) {
        container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No time-series data found for comparison.</p>';
        return;
    }
    
    const timeColumns = Array.from(allTimeColumns).sort();
    const labels = timeColumns.map(col => {
        const yearMatch = col.match(/\b(19|20)\d{2}\b/);
        if (yearMatch) return yearMatch[0];
        const rangeMatch = col.match(/(\d{4})-(\d{4})/);
        if (rangeMatch) return rangeMatch[1] + '-' + rangeMatch[2].slice(-2);
        return col;
    });
    
    // Create datasets for each country
    const datasets = [];
    const colors = [
        'rgba(197, 183, 114, 1)', // E3G gold
        'rgba(168, 196, 216, 1)', // E3G blue
        'rgba(123, 140, 80, 1)',  // E3G olive
        'rgba(50, 48, 103, 1)',   // E3G dark blue
        'rgba(139, 172, 196, 1)', // Light blue
        'rgba(168, 183, 114, 1)', // Light gold
    ];
    let colorIndex = 0;
    
    Object.entries(countryData).forEach(([countryName, country]) => {
        if (!country.data || country.data.length === 0) return;
        
        // Aggregate data for this country
        const values = [];
        timeColumns.forEach(col => {
            let sum = 0;
            let count = 0;
            country.data.forEach(row => {
                const val = parseFloat((row.row_data[col] || '0').toString().replace(/[%,]/g, ''));
                if (!isNaN(val)) {
                    sum += Math.abs(val); // Use absolute value
                    count++;
                }
            });
            values.push(count > 0 ? sum / count : 0);
        });
        
        const color = colors[colorIndex % colors.length];
        datasets.push({
            label: countryName,
            data: values,
            borderColor: color,
            backgroundColor: color.replace('1)', '0.2)'),
            tension: 0.1,
            fill: false,
            borderWidth: 2
        });
        colorIndex++;
    });
    
    if (datasets.length === 0) {
        container.innerHTML = '<p style="text-align: center; color: var(--text-secondary); padding: 40px;">No valid data found for comparison.</p>';
        return;
    }
    
    const chartDiv = document.createElement('div');
    chartDiv.className = 'chart-card';
    chartDiv.innerHTML = `
        <h3>${tableDescription}</h3>
        <div class="chart-container">
            <canvas id="comparisonChart"></canvas>
        </div>
    `;
    container.appendChild(chartDiv);
    
    // Wait for DOM to update
    setTimeout(() => {
        const canvas = document.getElementById('comparisonChart');
        if (!canvas) return;
        
        const ctx = canvas.getContext('2d');
        currentChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: `Cross-Country Comparison: ${tableDescription}`,
                        font: { size: 16, weight: 'bold' }
                    },
                    legend: {
                        display: true,
                        position: 'right',
                        labels: {
                            boxWidth: 12,
                            padding: 8
                        }
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: false,
                        title: {
                            display: true,
                            text: 'Value'
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Time Period'
                        }
                    }
                }
            }
        });
    }, 100);
}

// ═══════════════════════════════════════════════════════════════════════════
// Country profile
//
// One page that inventories every datapoint held for a single country. Before
// this existed, answering "what do we have for X?" meant hand-querying six
// tables, so the page deliberately reports coverage and freshness — not just
// values — and calls out the known source-mixing problem in gas demand.
// ═══════════════════════════════════════════════════════════════════════════

// Electricity lives in bidding zones, gas in countries. Three countries split
// into several zones, so a profile has to union them.
const CP_ZONES_BY_COUNTRY = {
    DK: ['DK1', 'DK2'],
    NO: ['NO1', 'NO2', 'NO3', 'NO4', 'NO5'],
    SE: ['SE1', 'SE2', 'SE3', 'SE4'],
};

// gas_demand_daily uses UK where the electricity tables use GB.
const CP_GAS_CODE = { GB: 'UK' };

// The renovation-plan tables key off ISO-3; everything else uses ISO-2.
const CP_ISO3_TO_ISO2 = {
    AUT: 'AT', BEL: 'BE', BGR: 'BG', CYP: 'CY', CZE: 'CZ', DEU: 'DE', DNK: 'DK',
    ESP: 'ES', EST: 'EE', FIN: 'FI', FRA: 'FR', GBR: 'GB', GRC: 'GR', HRV: 'HR',
    HUN: 'HU', IRL: 'IE', ITA: 'IT', LTU: 'LT', LUX: 'LU', LVA: 'LV', MLT: 'MT',
    NLD: 'NL', POL: 'PL', PRT: 'PT', ROU: 'RO', SVK: 'SK', SVN: 'SI', SWE: 'SE',
};

const CP_COUNTRIES = [
    'AT', 'BE', 'BG', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR', 'GB',
    'GR', 'HR', 'HU', 'IE', 'IT', 'LT', 'LU', 'LV', 'MT', 'NL', 'NO', 'PL', 'PT',
    'RO', 'SE', 'SI', 'SK',
];

const CP_LAST_COUNTRY_KEY = 'app.countryProfile.country';

let cpWired = false;
let cpLoadToken = 0;        // guards against out-of-order responses
let cpNbrpCountries = null; // countries table, fetched once

function cpZones(cc) {
    return CP_ZONES_BY_COUNTRY[cc] || [cc];
}

function cpSetStatus(msg) {
    const el = document.getElementById('cpStatus');
    if (el) el.textContent = msg || '';
}

// ── Freshness ──────────────────────────────────────────────────────────────
// Cadence-aware: a 15-minute feed that is a day old is broken, a daily feed
// that is a day old is perfectly normal.
function cpFreshness(lastIso, cadence) {
    if (!lastIso) return { key: 'none', label: 'No data', ageH: null };
    const ageH = (Date.now() - new Date(lastIso).getTime()) / 3600000;
    const [liveMax, lagMax] = cadence === 'daily' ? [72, 336] : [6, 168];
    if (ageH <= liveMax) return { key: 'live', label: 'Live', ageH };
    if (ageH <= lagMax) return { key: 'lagging', label: 'Lagging', ageH };
    return { key: 'stalled', label: 'Stalled', ageH };
}

function cpAgeText(ageH) {
    if (ageH == null) return '—';
    // Day-ahead prices are published for tomorrow, so the newest point is
    // legitimately in the future.
    if (ageH < 0) return `${Math.round(-ageH)} h ahead`;
    if (ageH < 1) return `${Math.max(1, Math.round(ageH * 60))} min ago`;
    if (ageH < 48) return `${Math.round(ageH)} h ago`;
    return `${Math.round(ageH / 24)} days ago`;
}

function cpFmtDate(iso) {
    if (!iso) return '—';
    return String(iso).slice(0, 10);
}

function cpFmtInt(n) {
    if (n == null) return '—';
    return Number(n).toLocaleString('en-GB');
}

// ── Query helpers ──────────────────────────────────────────────────────────
function cpApplyFilter(query, spec) {
    if (spec.zones) {
        return spec.zones.length > 1
            ? query.in(spec.col, spec.zones)
            : query.eq(spec.col, spec.zones[0]);
    }
    return query.eq(spec.col, spec.value);
}

// Exact counts on these tables are normally sub-second even at ~1M rows, but
// they occasionally trip the statement timeout under load. One retry clears
// that in practice; only then do we fall back to the planner estimate, which
// is imprecise enough (it read 1.12M for a zone pair holding 1.19M) that the
// UI has to mark it as approximate.
async function cpCountRows(spec, wantExact) {
    const attempt = async (mode) => {
        const q = supabase.from(spec.table).select(spec.col, { count: mode, head: true });
        return await cpApplyFilter(q, spec);
    };
    if (wantExact) {
        for (let tries = 0; tries < 2; tries++) {
            try {
                const r = await attempt('exact');
                if (!r.error) return { count: r.count, approx: false };
            } catch (_) { /* retry, then fall through to the estimate */ }
        }
    }
    try {
        const r = await attempt('planned');
        if (r.error) return { count: null, approx: false, error: r.error.message };
        return { count: r.count, approx: true };
    } catch (e) {
        return { count: null, approx: false, error: e.message };
    }
}

async function cpEdge(spec, ascending) {
    const q = supabase.from(spec.table)
        .select(spec.tsCol)
        .order(spec.tsCol, { ascending })
        .limit(1);
    const { data, error } = await cpApplyFilter(q, spec);
    if (error || !data || !data.length) return null;
    return data[0][spec.tsCol];
}

async function cpLatestRow(spec, cols) {
    const q = supabase.from(spec.table)
        .select(cols)
        .order(spec.tsCol, { ascending: false })
        .limit(1);
    const { data, error } = await cpApplyFilter(q, spec);
    if (error || !data || !data.length) return null;
    return data[0];
}

function cpDatasetSpecs(cc) {
    const zones = cpZones(cc);
    const gas = CP_GAS_CODE[cc] || cc;
    return [
        { id: 'generation', label: 'Electricity generation by fuel', source: 'ENTSO-E A75',
          table: 'electricity_generation_snapshots', col: 'zone_id', zones, tsCol: 'ts',
          cadence: '15min', grain: '15-min × fuel type', page: 'energy-meter' },
        { id: 'mix', label: 'Renewable share', source: 'ENTSO-E (derived)',
          table: 'energy_mix_snapshots', col: 'zone_id', zones, tsCol: 'ts',
          cadence: '15min', grain: '15-min', page: 'energy-meter' },
        { id: 'load', label: 'Electricity demand (load)', source: 'ENTSO-E A65',
          table: 'electricity_load_snapshots', col: 'zone_id', zones, tsCol: 'ts',
          cadence: '15min', grain: '15-min', page: 'energy-meter' },
        { id: 'price', label: 'Day-ahead electricity price', source: 'ENTSO-E A44',
          table: 'electricity_day_ahead_prices', col: 'zone_id', zones, tsCol: 'ts',
          cadence: '15min', grain: 'hourly / 15-min', page: 'energy-meter' },
        { id: 'flows', label: 'Cross-border flows (exports)', source: 'ENTSO-E A11',
          table: 'electricity_crossborder_flows', col: 'from_zone', zones, tsCol: 'ts',
          cadence: '15min', grain: 'per border', page: 'energy-meter' },
        { id: 'gasdemand', label: 'Gas demand by sector', source: 'ENTSOG / GIE / Eurostat',
          table: 'gas_demand_daily', col: 'country_code', value: gas, tsCol: 'gas_day',
          cadence: 'daily', grain: 'daily', page: 'gas-meter' },
        { id: 'gasstorage', label: 'Gas storage', source: 'GIE AGSI',
          table: 'gas_storage_country_daily', col: 'country', value: cc, tsCol: 'gas_day',
          cadence: 'daily', grain: 'daily', page: 'gas-meter' },
    ];
}

// ── Section builders ───────────────────────────────────────────────────────
async function cpBuildCoverage(cc, wantExact) {
    const specs = cpDatasetSpecs(cc);
    return await Promise.all(specs.map(async (spec) => {
        const [counted, first, last] = await Promise.all([
            cpCountRows(spec, wantExact),
            cpEdge(spec, true),
            cpEdge(spec, false),
        ]);
        return { spec, ...counted, first, last, fresh: cpFreshness(last, spec.cadence) };
    }));
}

async function cpGenerationMix(cc) {
    const zones = cpZones(cc);
    const withZones = (q) => (zones.length > 1 ? q.in('zone_id', zones) : q.eq('zone_id', zones[0]));

    const { data: tsRows } = await withZones(
        supabase.from('electricity_generation_snapshots').select('ts').order('ts', { ascending: false }).limit(1)
    );
    if (!tsRows || !tsRows.length) return null;
    const ts = tsRows[0].ts;

    const { data: rows } = await withZones(
        supabase.from('electricity_generation_snapshots').select('psr_type, mw').eq('ts', ts).limit(500)
    );
    if (!rows || !rows.length) return null;

    const byGroup = new Map();
    let total = 0;
    for (const r of rows) {
        const mw = Number(r.mw) || 0;
        if (!Number.isFinite(mw)) continue;
        total += mw;
        const group = ELEC_TYPE_GROUPS.find(g => g.types.includes(r.psr_type));
        const key = group ? group.key : 'other';
        const label = group ? group.label : 'Other';
        const color = group ? group.color : '#94a3b8';
        const prev = byGroup.get(key) || { label, color, mw: 0 };
        prev.mw += mw;
        byGroup.set(key, prev);
    }
    const items = [...byGroup.values()].filter(g => g.mw > 0).sort((a, b) => b.mw - a.mw);
    return { ts, total, items, fuelCount: new Set(rows.map(r => r.psr_type)).size };
}

// Surfaces the source-mixing defect: gas_demand_daily stitches together an
// ENTSOG-implied daily series and a Eurostat monthly figure spread across the
// month. Where the two disagree on level, the chart shows a sawtooth that is
// an artefact, not real demand.
async function cpGasQuality(cc) {
    const gas = CP_GAS_CODE[cc] || cc;
    let rows;
    try {
        rows = await gasFetchAllPaged(() =>
            supabase.from('gas_demand_daily')
                .select('gas_day, total_mwh, source_total')
                .eq('country_code', gas)
                .order('gas_day', { ascending: true })
        , 1000, 10000);
    } catch (_) {
        return null;
    }
    if (!rows || !rows.length) return null;

    const bySource = new Map();
    const byMonth = new Map();
    for (const r of rows) {
        const src = r.source_total || 'unknown';
        bySource.set(src, (bySource.get(src) || 0) + 1);
        const m = String(r.gas_day).slice(0, 7);
        if (!byMonth.has(m)) byMonth.set(m, []);
        byMonth.get(m).push(r);
    }

    // Compare the two families within the same month — that removes season.
    const median = (arr) => {
        if (!arr.length) return null;
        const s = [...arr].sort((a, b) => a - b);
        const mid = s.length >> 1;
        return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
    };
    const ratios = [];
    let mixedMonths = 0;
    for (const [, monthRows] of byMonth) {
        const uncal = monthRows.filter(r => r.source_total === 'entsog_gie_implied_daily')
            .map(r => Number(r.total_mwh) || 0).filter(v => v > 0);
        const euro = monthRows.filter(r => String(r.source_total || '').startsWith('eurostat'))
            .map(r => Number(r.total_mwh) || 0).filter(v => v > 0);
        const families = new Set(monthRows.map(r => String(r.source_total || '').split('_')[0]));
        if (families.size > 1) mixedMonths += 1;
        if (uncal.length && euro.length) {
            const mu = median(uncal);
            const me = median(euro);
            if (mu > 0) ratios.push(me / mu);
        }
    }

    const uncalCount = bySource.get('entsog_gie_implied_daily') || 0;
    return {
        total: rows.length,
        bySource: [...bySource.entries()].sort((a, b) => b[1] - a[1]),
        mixedMonths,
        monthCount: byMonth.size,
        uncalCount,
        scaleGap: ratios.length ? median(ratios) : null,
    };
}

async function cpNbrp(cc) {
    if (!cpNbrpCountries) {
        const { data } = await supabase.from('countries').select('id, code, name');
        cpNbrpCountries = data || [];
    }
    const match = cpNbrpCountries.find(c => (CP_ISO3_TO_ISO2[c.code] || c.code) === cc);
    const name = CB_COUNTRY_NAMES[cc] || cc;

    // Countries absent from the corpus can still appear as comparator rows in
    // other countries' EU-wide tables, which is worth surfacing.
    let mentions = 0;
    try {
        const { count } = await supabase.from('data_points')
            .select('id', { count: 'exact', head: true })
            .eq('row_data->>Country', name);
        mentions = count || 0;
    } catch (_) { /* comparator lookup is best-effort */ }

    if (!match) return { covered: false, mentions, name };

    const [tables, measures] = await Promise.all([
        supabase.from('data_tables').select('id', { count: 'exact', head: true }).eq('country_id', match.id),
        supabase.from('measures').select('id', { count: 'exact', head: true }).eq('country_id', match.id),
    ]);
    return {
        covered: true,
        countryId: match.id,
        name: match.name || name,
        tables: tables.count || 0,
        measures: measures.count || 0,
        mentions,
    };
}

// ── Rendering ──────────────────────────────────────────────────────────────
function cpRenderCoverage(coverage) {
    const rows = coverage.map(c => {
        const approx = c.approx && c.count != null ? '~' : '';
        const countTxt = c.count == null ? '—' : `${approx}${cpFmtInt(c.count)}`;
        const range = c.first ? `${cpFmtDate(c.first)} → ${cpFmtDate(c.last)}` : '—';
        return `
            <tr>
                <td>
                    <div class="cp-ds-name">${escapeHtml(c.spec.label)}</div>
                    <div class="cp-ds-meta">${escapeHtml(c.spec.source)} · ${escapeHtml(c.spec.grain)}</div>
                </td>
                <td class="cp-num">${countTxt}</td>
                <td class="cp-range">${escapeHtml(range)}</td>
                <td><span class="cp-badge cp-badge-${c.fresh.key}">${escapeHtml(c.fresh.label)}</span></td>
                <td class="cp-age">${escapeHtml(cpAgeText(c.fresh.ageH))}</td>
            </tr>`;
    }).join('');

    return `
        <section class="cp-section">
            <h2 class="cp-section-title">Data coverage</h2>
            <div class="table-container">
                <table class="data-table cp-table">
                    <thead>
                        <tr>
                            <th>Dataset</th><th class="cp-num">Rows</th><th>Coverage</th>
                            <th>Status</th><th>Last point</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </section>`;
}

function cpRenderHeadline(coverage, latest) {
    const totalRows = coverage.reduce((s, c) => s + (c.count || 0), 0);
    const anyApprox = coverage.some(c => c.approx && c.count != null);
    const withData = coverage.filter(c => c.count).length;
    const problems = coverage.filter(c => c.fresh.key === 'stalled' || c.fresh.key === 'lagging').length;

    const tile = (value, label, cls) =>
        `<div class="cp-tile ${cls || ''}"><div class="cp-tile-value">${value}</div>
         <div class="cp-tile-label">${escapeHtml(label)}</div></div>`;

    const tiles = [
        tile(`${anyApprox ? '~' : ''}${cpFmtInt(totalRows)}`, 'Datapoints held'),
        tile(`${withData} / ${coverage.length}`, 'Datasets with data'),
        tile(String(problems), 'Feeds behind schedule', problems ? 'cp-tile-warn' : ''),
        tile(latest.renewable == null ? '—' : `${latest.renewable.toFixed(1)}%`, 'Renewable share (latest)'),
        tile(latest.price == null ? '—' : `€${latest.price.toFixed(1)}`, 'Day-ahead €/MWh'),
        tile(latest.load == null ? '—' : `${cpFmtInt(Math.round(latest.load))}`, 'Electricity load (MW)'),
        tile(latest.gasDemand == null ? '—' : `${cpFmtInt(Math.round(latest.gasDemand / 1000))}`, 'Gas demand (GWh/day)'),
        tile(latest.storage == null ? '—' : `${latest.storage.toFixed(1)}%`, 'Gas storage full'),
    ].join('');

    return `<div class="cp-tiles">${tiles}</div>`;
}

function cpRenderMix(mix) {
    if (!mix || !mix.total) {
        return `<section class="cp-section">
            <h2 class="cp-section-title">Generation mix</h2>
            <div class="cp-empty">No generation snapshot available.</div>
        </section>`;
    }
    const bars = mix.items.map(it => {
        const pct = (it.mw / mix.total) * 100;
        return `
            <div class="cp-mix-row">
                <div class="cp-mix-label">${escapeHtml(it.label)}</div>
                <div class="cp-mix-track">
                    <div class="cp-mix-fill" style="width:${pct.toFixed(1)}%;background:${escapeHtml(it.color)}"></div>
                </div>
                <div class="cp-mix-val">${pct.toFixed(1)}%</div>
                <div class="cp-mix-mw">${cpFmtInt(Math.round(it.mw))} MW</div>
            </div>`;
    }).join('');

    return `
        <section class="cp-section">
            <h2 class="cp-section-title">Generation mix
                <span class="cp-section-note">latest snapshot · ${escapeHtml(new Date(mix.ts).toLocaleString('en-GB'))}
                · ${mix.fuelCount} fuel types · ${cpFmtInt(Math.round(mix.total))} MW total</span>
            </h2>
            <div class="cp-mix">${bars}</div>
        </section>`;
}

function cpRenderGasQuality(q) {
    if (!q) return '';
    const srcRows = q.bySource.map(([src, n]) => {
        const bad = src === 'entsog_gie_implied_daily';
        return `<tr class="${bad ? 'cp-row-bad' : ''}">
            <td><code>${escapeHtml(src)}</code></td>
            <td class="cp-num">${cpFmtInt(n)}</td>
            <td class="cp-num">${((n / q.total) * 100).toFixed(1)}%</td>
        </tr>`;
    }).join('');

    let warning = '';
    if (q.uncalCount && q.scaleGap && q.scaleGap > 2) {
        warning = `<div class="cp-warn">
            <strong>Scale mismatch in gas demand.</strong>
            ${cpFmtInt(q.uncalCount)} days use the uncalibrated
            <code>entsog_gie_implied_daily</code> series, which sits about
            <strong>${q.scaleGap.toFixed(1)}× below</strong> the Eurostat-budgeted days in the
            same months. Because the two are interleaved day by day
            (${q.mixedMonths} of ${q.monthCount} months mix sources), the demand chart shows a
            sawtooth that is an artefact of the source switch rather than real demand.
        </div>`;
    } else if (q.mixedMonths) {
        warning = `<div class="cp-note">${q.mixedMonths} of ${q.monthCount} months combine more than one
            source, but the sources agree on level, so the series is consistent.</div>`;
    }

    return `
        <section class="cp-section">
            <h2 class="cp-section-title">Gas demand provenance</h2>
            ${warning}
            <div class="table-container">
                <table class="data-table cp-table">
                    <thead><tr><th>Source</th><th class="cp-num">Days</th><th class="cp-num">Share</th></tr></thead>
                    <tbody>${srcRows}</tbody>
                </table>
            </div>
        </section>`;
}

function cpRenderNbrp(nbrp) {
    const mentionLine = nbrp.mentions
        ? `<p class="cp-note">Appears as a comparator row in ${cpFmtInt(nbrp.mentions)}
           table row(s) belonging to other countries.</p>`
        : '';

    if (!nbrp.covered) {
        return `
            <section class="cp-section">
                <h2 class="cp-section-title">National building renovation plan</h2>
                <div class="cp-empty">Not covered — no NBRP tables or policy measures for this country.</div>
                ${mentionLine}
            </section>`;
    }
    return `
        <section class="cp-section">
            <h2 class="cp-section-title">National building renovation plan</h2>
            <div class="cp-tiles cp-tiles-sm">
                <div class="cp-tile"><div class="cp-tile-value">${cpFmtInt(nbrp.tables)}</div>
                    <div class="cp-tile-label">Data tables</div></div>
                <div class="cp-tile"><div class="cp-tile-value">${cpFmtInt(nbrp.measures)}</div>
                    <div class="cp-tile-label">Policy measures</div></div>
            </div>
            ${mentionLine}
            <button class="cp-btn cp-btn-link" type="button"
                data-cp-country-id="${nbrp.countryId}">Open full renovation-plan profile →</button>
        </section>`;
}

// ── Orchestration ──────────────────────────────────────────────────────────
async function cpLoadCountry(cc) {
    const token = ++cpLoadToken;
    const body = document.getElementById('cpBody');
    if (!body) return;

    const wantExact = document.getElementById('cpExactCounts')?.checked !== false;
    body.innerHTML = '<div class="cp-empty">Building profile…</div>';
    cpSetStatus('Querying…');

    try {
        const zones = cpZones(cc);
        const specs = cpDatasetSpecs(cc);
        const specById = (id) => specs.find(s => s.id === id);

        const [coverage, mix, gasQuality, nbrp, mixRow, priceRow, loadRow, storageRow, gasRow] =
            await Promise.all([
                cpBuildCoverage(cc, wantExact),
                cpGenerationMix(cc),
                cpGasQuality(cc),
                cpNbrp(cc),
                cpLatestRow(specById('mix'), 'ts, renewable_percent'),
                cpLatestRow(specById('price'), 'ts, price_eur_per_mwh'),
                cpLatestRow(specById('load'), 'ts, load_mw'),
                cpLatestRow(specById('gasstorage'), 'gas_day, full_pct, gas_in_storage_twh'),
                cpLatestRow(specById('gasdemand'), 'gas_day, total_mwh'),
            ]);

        if (token !== cpLoadToken) return; // a newer request has taken over

        const latest = {
            renewable: mixRow ? Number(mixRow.renewable_percent) : null,
            price: priceRow ? Number(priceRow.price_eur_per_mwh) : null,
            load: loadRow ? Number(loadRow.load_mw) : null,
            storage: storageRow ? Number(storageRow.full_pct) : null,
            gasDemand: gasRow ? Number(gasRow.total_mwh) : null,
        };

        const flag = cbCountryFlag(cc);
        const name = CB_COUNTRY_NAMES[cc] || cc;
        const zoneNote = zones.length > 1
            ? `Electricity bidding zones: ${zones.join(', ')}`
            : `Electricity bidding zone: ${zones[0]}`;

        body.innerHTML = `
            <div class="cp-head">
                <div class="cp-head-flag">${escapeHtml(flag)}</div>
                <div>
                    <h2 class="cp-head-name">${escapeHtml(name)} <span class="cp-head-code">${escapeHtml(cc)}</span></h2>
                    <div class="cp-head-sub">${escapeHtml(zoneNote)}</div>
                </div>
            </div>
            ${cpRenderHeadline(coverage, latest)}
            ${cpRenderCoverage(coverage)}
            ${cpRenderMix(mix)}
            ${cpRenderGasQuality(gasQuality)}
            ${cpRenderNbrp(nbrp)}
        `;

        body.querySelector('[data-cp-country-id]')?.addEventListener('click', (e) => {
            navigateToPage('country', e.currentTarget.getAttribute('data-cp-country-id'));
        });

        cpSetStatus(`Updated ${new Date().toLocaleTimeString('en-GB')}`);
    } catch (err) {
        if (token !== cpLoadToken) return;
        console.error('Country profile failed:', err);
        body.innerHTML = `<div class="cp-empty cp-empty-error">Could not build the profile: ${escapeHtml(err.message)}</div>`;
        cpSetStatus('Failed');
    }
}

function loadCountryProfilePage() {
    const select = document.getElementById('cpCountrySelect');
    if (!select) return;

    if (!cpWired) {
        select.innerHTML = CP_COUNTRIES.map(cc => {
            const name = CB_COUNTRY_NAMES[cc] || cc;
            return `<option value="${escapeHtml(cc)}">${escapeHtml(`${cbCountryFlag(cc)} ${name}`)}</option>`;
        }).join('');

        let initial = 'PL';
        try {
            const saved = localStorage.getItem(CP_LAST_COUNTRY_KEY);
            if (saved && CP_COUNTRIES.includes(saved)) initial = saved;
        } catch (_) { /* storage may be blocked */ }
        select.value = initial;

        select.addEventListener('change', () => {
            try { localStorage.setItem(CP_LAST_COUNTRY_KEY, select.value); } catch (_) { /* ignore */ }
            cpLoadCountry(select.value);
        });
        document.getElementById('cpRefreshBtn')?.addEventListener('click', () => cpLoadCountry(select.value));
        document.getElementById('cpExactCounts')?.addEventListener('change', () => cpLoadCountry(select.value));

        cpWired = true;
    }

    cpLoadCountry(select.value);
}

// ═══════════════════════════════════════════════════════════════════════════
// Heatwaves page
//
// Reads the heatwave analysis views live. Those views were originally written
// for ad-hoc SQL and re-aggregated the 15-minute tables on every call, which
// is far too slow behind a page load; heatwave_fast_mvs.sql materializes the
// daily grain so everything here returns in well under a second.
//
// Colour: chrome follows the E3G brand tokens, but the data marks keep the
// validated categorical/diverging palette. An eight-hue categorical set has to
// clear colour-vision-deficiency separation checks as a set, and deriving one
// from two brand hues without re-running that validation would be guesswork.
// ═══════════════════════════════════════════════════════════════════════════

const HW_NS = 'http://www.w3.org/2000/svg';
const HW_FUEL_ORDER = ['solar', 'wind', 'hydro', 'nuclear', 'gas', 'coal', 'biomass', 'other'];
const HW_FUEL_COLOR = {
    solar: '#eda100', wind: '#2a78d6', hydro: '#1baf7a', nuclear: '#4a3aa7',
    gas: '#eb6834', coal: '#e34948', biomass: '#008300', other: '#e87ba4',
};
const HW_POS = '#2a78d6';   // diverging cool pole
const HW_NEG = '#c2562f';   // diverging warm pole
const HW_MIN_DAYS = 10;     // fewest heatwave days a country may be charted on
const HW_ACCENT = '#262958';
const HW_DEEMPH = '#d5d4cd';
const HW_SEQ = ['#9ec5f4', '#6da7ec', '#3987e5', '#256abf', '#184f95', '#0d366b'];

let hwWired = false;
let hwData = null;
let hwLoadToken = 0;

function hwEl(p, tag, attrs = {}, text) {
    const n = document.createElementNS(HW_NS, tag);
    for (const k in attrs) n.setAttribute(k, attrs[k]);
    if (text !== undefined) n.textContent = text;
    p.appendChild(n);
    return n;
}
function hwClear(s) { while (s && s.firstChild) s.removeChild(s.firstChild); }
function hwName(cc) { return CB_COUNTRY_NAMES[cc] || cc; }
function hwFmt(v, d = 0) {
    if (v === null || v === undefined || v === '') return '—';
    return Number(v).toLocaleString('en-GB', {minimumFractionDigits: d, maximumFractionDigits: d});
}
function hwSign(v, d = 0) { return (Number(v) > 0 ? '+' : '') + hwFmt(v, d); }
function hwCap(s) { return String(s).charAt(0).toUpperCase() + String(s).slice(1); }

function hwTip(node, html) {
    let tip = document.getElementById('hwTip');
    if (!tip) {
        tip = document.createElement('div');
        tip.id = 'hwTip';
        tip.className = 'hw-tip';
        document.body.appendChild(tip);
    }
    node.addEventListener('mousemove', (e) => {
        tip.innerHTML = html;
        tip.style.opacity = 1;
        const r = tip.getBoundingClientRect();
        let x = e.clientX + 14, y = e.clientY - 10;
        if (x + r.width > window.innerWidth - 8) x = e.clientX - r.width - 14;
        if (y + r.height > window.innerHeight - 8) y = window.innerHeight - r.height - 8;
        tip.style.left = x + 'px';
        tip.style.top = Math.max(8, y) + 'px';
    });
    node.addEventListener('mouseleave', () => { tip.style.opacity = 0; });
}

function hwTable(hostId, cols, rows) {
    const host = document.getElementById(hostId);
    if (!host) return;
    host.innerHTML = `<table class="hw-table"><thead><tr>${
        cols.map(c => `<th>${escapeHtml(c)}</th>`).join('')}</tr></thead><tbody>${
        rows.map(r => `<tr>${r.map(v => `<td>${escapeHtml(String(v))}</td>`).join('')}</tr>`).join('')
    }</tbody></table>`;
}

function hwLegend(hostId, items) {
    const host = document.getElementById(hostId);
    if (!host) return;
    host.innerHTML = items.map(i =>
        `<span class="hw-lg"><span class="hw-sw" style="background:${i.c}"></span>${escapeHtml(i.t)}</span>`
    ).join('');
}

// Shared horizontal diverging bar — used by fuels, renewable, price, balance.
function hwDiverging(svgId, rows, opts) {
    const svg = document.getElementById(svgId);
    if (!svg) return;
    hwClear(svg);
    if (!rows.length) {
        svg.setAttribute('height', 60);
        hwEl(svg, 'text', {x: 12, y: 32, class: 'hw-lbl'}, opts.empty || 'No data for this selection.');
        return;
    }
    const rowH = opts.rowH || 22;
    const H = rows.length * rowH + 34;
    svg.setAttribute('height', H);
    const W = svg.clientWidth || 640;
    const m = {t: 8, r: opts.right || 62, b: 22, l: opts.left || 92};
    const iw = Math.max(80, W - m.l - m.r);
    // The scale must cover the whiskers too — they used to run straight off
    // the plot — but one extreme national outcome must not crush every mean
    // bar to a sliver. So the domain stretches to at most 3x the largest mean;
    // whiskers beyond that are clamped and drawn as an arrow with the true
    // value printed at the cut.
    const maxV = Math.max(...rows.map(r => Math.abs(r.v)), 0.001);
    const maxW = Math.max(0, ...rows.flatMap(r => [Math.abs(r.lo ?? 0), Math.abs(r.hi ?? 0)]));
    const mx = Math.max(maxV * 1.12, Math.min(maxW * 1.06, maxV * 3));
    const X = v => m.l + iw / 2 + (v / mx) * (iw / 2);
    const bh = Math.min(14, rowH - 8);

    hwEl(svg, 'line', {x1: X(0), x2: X(0), y1: m.t - 4, y2: m.t + rows.length * rowH - 6,
        stroke: '#c3c2b7', 'stroke-width': 1});

    rows.forEach((r, i) => {
        const y = m.t + i * rowH;
        const neg = r.v < 0;
        const x = neg ? X(r.v) : X(0);
        const w = Math.max(2, Math.abs(X(r.v) - X(0)));
        if (r.lo !== undefined && r.hi !== undefined) {
            const clamp = v => Math.max(-mx * 0.995, Math.min(mx * 0.995, v));
            const loC = clamp(r.lo), hiC = clamp(r.hi);
            const loCut = loC !== r.lo, hiCut = hiC !== r.hi;
            hwEl(svg, 'line', {x1: X(loC), x2: X(hiC), y1: y + bh / 2, y2: y + bh / 2,
                stroke: '#c3c2b7', 'stroke-width': 1});
            [[loC, loCut], [hiC, hiCut]].forEach(([b, cut]) => {
                if (cut) return;   // a cut end gets its arrow label, not a tick
                hwEl(svg, 'line', {x1: X(b), x2: X(b), y1: y + 2, y2: y + bh - 2,
                    stroke: '#c3c2b7', 'stroke-width': 1});
            });
            // The whisker ends carry their numbers on the chart, not only in
            // the tooltip — skipped when the whisker is too short to fit them.
            const rf = opts.rangeFmt || (v => hwFmt(v, 0));
            if (loCut) {
                hwEl(svg, 'text', {x: X(loC) - 2, y: y + bh - 1, class: 'hw-tick',
                    'text-anchor': 'end'}, '« ' + rf(r.lo));
            } else if (Math.abs(X(r.v) - X(loC)) > 48) {
                hwEl(svg, 'text', {x: X(loC) - 4, y: y + bh - 1, class: 'hw-tick',
                    'text-anchor': 'end'}, rf(r.lo));
            }
            if (hiCut) {
                hwEl(svg, 'text', {x: X(hiC) + 2, y: y + bh - 1, class: 'hw-tick'},
                    rf(r.hi) + ' »');
            } else if (Math.abs(X(hiC) - X(r.v)) > 48) {
                hwEl(svg, 'text', {x: X(hiC) + 4, y: y + bh - 1, class: 'hw-tick'}, rf(r.hi));
            }
        }
        hwEl(svg, 'rect', {x, y, width: w, height: bh, rx: 4,
            fill: neg ? (opts.negColor || HW_NEG) : (opts.posColor || HW_POS)});
        hwEl(svg, 'text', {x: m.l - 8, y: y + bh - 1, class: 'hw-lbl', 'text-anchor': 'end'}, r.label);
        hwEl(svg, 'text', {x: neg ? x - 7 : x + w + 7, y: y + bh - 1, class: 'hw-val',
            'text-anchor': neg ? 'end' : 'start'}, r.vlabel);
        const hit = hwEl(svg, 'rect', {x: m.l, y: y - 3, width: iw, height: bh + 6, fill: 'transparent'});
        hwTip(hit, r.tip);
    });
    hwEl(svg, 'text', {x: m.l + iw / 2, y: H - 5, class: 'hw-lbl', 'text-anchor': 'middle'}, opts.axis);
}

// ── Europe: renewable share vs how much of the continent is hot ────────────
// A dot plot, not bars: the y-axis is truncated (the values sit in a 44-50%
// band) and bar LENGTH must be read from zero, so bars here would overstate
// the difference. Position encodes the value instead, which a truncated scale
// supports honestly.
function hwRenderEu() {
    const svg = document.getElementById('hwEu');
    if (!svg) return;
    hwClear(svg);
    const order = ['0', '1-3', '4-7', '8+'];
    const rows = order.map(b => hwData.eu.find(r => r.bucket === b)).filter(Boolean);
    if (!rows.length) return;

    const W = svg.clientWidth || 800, H = 300;
    svg.setAttribute('height', H);
    const m = {t: 20, r: 28, b: 66, l: 54};
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    // Domain covers the day ranges, not only the means, so the min-max bands
    // fit inside the plot.
    const vals = rows.flatMap(r => [+r.mean_renewable_pct,
        +r.min_renewable_pct || +r.mean_renewable_pct,
        +r.max_renewable_pct || +r.mean_renewable_pct]);
    const y0 = Math.floor(Math.min(...vals) - 2), y1 = Math.ceil(Math.max(...vals) + 2);
    const X = i => m.l + (i + 0.5) * (iw / rows.length);
    const Y = v => m.t + ih - (v - y0) / (y1 - y0) * ih;

    for (let k = 0; k <= 4; k++) {
        const v = y0 + (y1 - y0) * k / 4;
        hwEl(svg, 'line', {x1: m.l, x2: m.l + iw, y1: Y(v), y2: Y(v), stroke: '#e1e0d9', 'stroke-width': 1});
        hwEl(svg, 'text', {x: m.l - 8, y: Y(v) + 4, class: 'hw-tick', 'text-anchor': 'end'},
            hwFmt(v, 0) + '%');
    }

    hwEl(svg, 'polyline', {
        points: rows.map((r, i) => `${X(i)},${Y(+r.mean_renewable_pct)}`).join(' '),
        fill: 'none', stroke: HW_ACCENT, 'stroke-width': 2, 'stroke-linejoin': 'round',
    });

    rows.forEach((r, i) => {
        const v = +r.mean_renewable_pct;
        // Min-max band across the bucket's days, values printed at both ends.
        const bMin = +r.min_renewable_pct, bMax = +r.max_renewable_pct;
        if (Number.isFinite(bMin) && Number.isFinite(bMax)) {
            hwEl(svg, 'rect', {x: X(i) - 4, y: Y(bMax), width: 8, rx: 4,
                height: Math.max(2, Y(bMin) - Y(bMax)), fill: 'rgba(148,163,184,0.30)'});
            hwEl(svg, 'text', {x: X(i) + 10, y: Y(bMax) + 4, class: 'hw-tick'}, hwFmt(bMax, 0) + '%');
            hwEl(svg, 'text', {x: X(i) + 10, y: Y(bMin) + 4, class: 'hw-tick'}, hwFmt(bMin, 0) + '%');
        }
        hwEl(svg, 'circle', {cx: X(i), cy: Y(v), r: 7, fill: HW_ACCENT,
            stroke: '#ffffff', 'stroke-width': 2});
        hwEl(svg, 'text', {x: X(i) - 10, y: Y(v) + 4, class: 'hw-val', 'text-anchor': 'end'},
            hwFmt(v, 1) + '%');
        hwEl(svg, 'text', {x: X(i), y: m.t + ih + 22, class: 'hw-lbl', 'text-anchor': 'middle'}, r.bucket);
        // Sample size sits with the category, not hidden in a tooltip: the
        // buckets range from 299 days to 51 and that governs how much weight
        // each point can carry.
        hwEl(svg, 'text', {x: X(i), y: m.t + ih + 38, class: 'hw-tick', 'text-anchor': 'middle'},
            hwFmt(r.days) + ' days');
        const hit = hwEl(svg, 'circle', {cx: X(i), cy: Y(v), r: 18, fill: 'transparent'});
        hwTip(hit, `<b>${r.bucket} countries in a heatwave</b><br>Renewable share ${hwFmt(v, 1)}%<br>
            EU demand ${hwFmt(r.mean_eu_load_mw)} MW<br>${hwFmt(r.days)} days`);
    });

    hwEl(svg, 'text', {x: m.l + iw / 2, y: H - 8, class: 'hw-lbl', 'text-anchor': 'middle'},
        'Number of countries in a heatwave on the same day');

    hwTable('hwEuTbl',
        ['Countries hot', 'Days', 'Renewable min %', 'Renewable mean %', 'Renewable max %', 'EU demand MW'],
        rows.map(r => [r.bucket, r.days, r.min_renewable_pct, r.mean_renewable_pct,
            r.max_renewable_pct, hwFmt(r.mean_eu_load_mw)]));
}

// EU generation mix as a 100% stacked area across temperature.
//
// Band order is chosen for readability, not taste: the two biggest movers sit
// against the flat edges, where a changing width is easiest to judge. Gas is on
// the baseline (rising), wind against the 100% ceiling (falling). The four that
// barely move are buried in the middle where nothing is lost.
const HW_MIX_ORDER = ['gas', 'coal', 'other', 'biomass', 'nuclear', 'hydro', 'solar', 'wind'];

function hwRenderMixTemp() {
    const svg = document.getElementById('hwMixTemp');
    if (!svg) return;
    hwClear(svg);
    const rows = hwData.mixTemp || [];
    if (!rows.length) { svg.setAttribute('height', 60); return; }

    const bins = [...new Set(rows.map(r => Number(r.bin_c)))].sort((a, b) => a - b);
    const byBin = {};
    rows.forEach(r => {
        (byBin[Number(r.bin_c)] ||= {})[r.fuel] = Number(r.share_pct);
        byBin[Number(r.bin_c)].__days = Number(r.days);
    });

    const W = svg.clientWidth || 800, H = 400;
    svg.setAttribute('height', H);
    const m = {t: 14, r: 96, b: 62, l: 44};
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    const X = i => m.l + (bins.length === 1 ? iw / 2 : (i / (bins.length - 1)) * iw);
    const Y = v => m.t + ih - (v / 100) * ih;

    [0, 25, 50, 75, 100].forEach(v => {
        hwEl(svg, 'line', {x1: m.l, x2: m.l + iw, y1: Y(v), y2: Y(v),
            stroke: '#e1e0d9', 'stroke-width': 1});
        hwEl(svg, 'text', {x: m.l - 8, y: Y(v) + 4, class: 'hw-tick', 'text-anchor': 'end'}, v + '%');
    });

    // Cumulative bottoms per bin, stacked in the fixed order.
    const base = bins.map(() => 0);
    HW_MIX_ORDER.forEach(fuel => {
        const top = bins.map((b, i) => base[i] + (byBin[b][fuel] || 0));
        const fwd = bins.map((b, i) => `${X(i)},${Y(top[i])}`);
        const back = bins.map((b, i) => `${X(i)},${Y(base[i])}`).reverse();
        hwEl(svg, 'polygon', {points: [...fwd, ...back].join(' '),
            fill: HW_FUEL_COLOR[fuel] || '#8a8f98', stroke: '#ffffff', 'stroke-width': 1});
        // Direct label at the right edge for any band thick enough to hold text.
        const last = top.length - 1;
        const mid = (top[last] + base[last]) / 2, thick = top[last] - base[last];
        if (thick >= 5) {
            hwEl(svg, 'text', {x: m.l + iw + 8, y: Y(mid) + 4, class: 'hw-tick'},
                `${hwCap(fuel)} ${hwFmt(byBin[bins[last]][fuel], 0)}%`);
        }
        bins.forEach((b, i) => { base[i] = top[i]; });
    });

    bins.forEach((b, i) => {
        hwEl(svg, 'text', {x: X(i), y: m.t + ih + 20, class: 'hw-lbl', 'text-anchor': 'middle'},
            b + '°');
        hwEl(svg, 'text', {x: X(i), y: m.t + ih + 36, class: 'hw-tick', 'text-anchor': 'middle'},
            byBin[b].__days + 'd');
        const hit = hwEl(svg, 'rect', {x: X(i) - iw / (bins.length * 2), y: m.t,
            width: iw / bins.length, height: ih, fill: 'transparent'});
        const mix = HW_MIX_ORDER.slice().reverse()
            .map(f => `${hwCap(f)} ${hwFmt(byBin[b][f] || 0, 1)}%`).join('<br>');
        hwTip(hit, `<b>${b}°C — ${byBin[b].__days} days</b><br>${mix}`);
    });

    hwEl(svg, 'text', {x: m.l + iw / 2, y: H - 10, class: 'hw-lbl', 'text-anchor': 'middle'},
        'EU temperature (°C), weighted by each country’s electricity demand');

    hwLegend('hwMixTempLegend', HW_MIX_ORDER.slice().reverse()
        .map(f => ({c: HW_FUEL_COLOR[f] || '#8a8f98', t: hwCap(f)})));
    hwTable('hwMixTempTbl',
        ['EU temp °C', 'Days', ...HW_MIX_ORDER.slice().reverse().map(f => hwCap(f) + ' %')],
        bins.map(b => [b, byBin[b].__days,
            ...HW_MIX_ORDER.slice().reverse().map(f => hwFmt(byBin[b][f] || 0, 1))]));
}

// What solar does to the price, hour by hour.
//
// Two panels on one shared hour axis rather than one chart with two y-scales:
// a dual axis would let the eye read a correlation out of an arbitrary choice
// of scaling. Stacked, the mirror image is visible without that trick.
const HW_SP_COUNTRIES = ['DE', 'ES', 'IT'];
const HW_SP_COLOR = {DE: '#2a78d6', ES: '#e8a33d', IT: '#c2562f'};

function hwRenderSolarPrice() {
    const svg = document.getElementById('hwSolarPrice');
    if (!svg) return;
    hwClear(svg);
    const all = hwData.solarPrice || [];
    const rows = all.filter(r => HW_SP_COUNTRIES.includes(r.country_code));
    if (!rows.length) { svg.setAttribute('height', 60); return; }

    const by = {};
    rows.forEach(r => { (by[r.country_code] ||= {})[Number(r.hour)] = r; });
    const hours = [...Array(24).keys()];

    const W = svg.clientWidth || 800, H = 460;
    svg.setAttribute('height', H);
    const m = {t: 16, r: 54, b: 44, l: 54};
    const iw = W - m.l - m.r;
    const gap = 34;
    const ph = (H - m.t - m.b - gap) * 0.58;   // price panel
    const sh = (H - m.t - m.b - gap) * 0.42;   // solar panel
    const sTop = m.t + ph + gap;

    const prices = rows.map(r => Number(r.price_eur));
    const pLo = Math.min(0, ...prices), pHi = Math.max(...prices);
    const X = h => m.l + (h / 23) * iw;
    const YP = v => m.t + ph - ((v - pLo) / ((pHi - pLo) || 1)) * ph;
    const YS = v => sTop + sh - (v / 100) * sh;

    // Price panel
    const pTicks = [];
    for (let v = Math.ceil(pLo / 50) * 50; v <= pHi; v += 50) pTicks.push(v);
    pTicks.forEach(v => {
        hwEl(svg, 'line', {x1: m.l, x2: m.l + iw, y1: YP(v), y2: YP(v),
            stroke: v === 0 ? '#c3c2b7' : '#eceae2', 'stroke-width': 1});
        hwEl(svg, 'text', {x: m.l - 8, y: YP(v) + 4, class: 'hw-tick', 'text-anchor': 'end'},
            '€' + v);
    });
    hwEl(svg, 'text', {x: m.l, y: m.t - 4, class: 'hw-lbl'}, 'Day-ahead price (€/MWh)');

    // Solar panel
    [0, 25, 50, 75].forEach(v => {
        hwEl(svg, 'line', {x1: m.l, x2: m.l + iw, y1: YS(v), y2: YS(v),
            stroke: '#eceae2', 'stroke-width': 1});
        hwEl(svg, 'text', {x: m.l - 8, y: YS(v) + 4, class: 'hw-tick', 'text-anchor': 'end'},
            v + '%');
    });
    hwEl(svg, 'text', {x: m.l, y: sTop - 6, class: 'hw-lbl'}, 'Solar share of generation (%)');

    HW_SP_COUNTRIES.forEach(cc => {
        const d = by[cc];
        if (!d) return;
        const col = HW_SP_COLOR[cc];
        const pts = hours.filter(h => d[h]);
        hwEl(svg, 'polyline', {fill: 'none', stroke: col, 'stroke-width': 2,
            'stroke-linejoin': 'round',
            points: pts.map(h => `${X(h)},${YP(Number(d[h].price_eur))}`).join(' ')});
        hwEl(svg, 'polyline', {fill: 'none', stroke: col, 'stroke-width': 2,
            'stroke-linejoin': 'round',
            points: pts.map(h => `${X(h)},${YS(Number(d[h].solar_share_pct))}`).join(' ')});
        // Direct label at the midday extreme, which is the point of the chart.
        const noon = d[13] || d[12];
        if (noon) {
            hwEl(svg, 'text', {x: X(13) + 6, y: YP(Number(noon.price_eur)) + 4,
                class: 'hw-val', fill: col}, `${hwName(cc)} €${hwFmt(noon.price_eur, 0)}`);
        }
    });

    hours.filter(h => h % 3 === 0).forEach(h => {
        hwEl(svg, 'text', {x: X(h), y: H - 22, class: 'hw-tick', 'text-anchor': 'middle'},
            String(h).padStart(2, '0') + ':00');
    });
    hwEl(svg, 'text', {x: m.l + iw / 2, y: H - 6, class: 'hw-lbl', 'text-anchor': 'middle'},
        'Hour of day (local, CEST)');

    // One hover column per hour covering both panels.
    hours.forEach(h => {
        const hit = hwEl(svg, 'rect', {x: X(h) - iw / 46, y: m.t, width: iw / 23,
            height: ph + gap + sh, fill: 'transparent'});
        const lines = HW_SP_COUNTRIES.filter(cc => by[cc] && by[cc][h]).map(cc => {
            const r = by[cc][h];
            return `${hwName(cc)}: €${hwFmt(r.price_eur, 0)} · solar ${hwFmt(r.solar_share_pct, 0)}%`;
        }).join('<br>');
        hwTip(hit, `<b>${String(h).padStart(2, '0')}:00</b><br>${lines}`);
    });

    hwLegend('hwSolarPriceLegend',
        HW_SP_COUNTRIES.map(cc => ({c: HW_SP_COLOR[cc], t: hwName(cc)})));
    hwTable('hwSolarPriceTbl',
        ['Hour', ...HW_SP_COUNTRIES.flatMap(cc => [hwName(cc) + ' €/MWh', hwName(cc) + ' solar %'])],
        hours.filter(h => HW_SP_COUNTRIES.some(cc => by[cc] && by[cc][h])).map(h => [
            String(h).padStart(2, '0') + ':00',
            ...HW_SP_COUNTRIES.flatMap(cc => by[cc] && by[cc][h]
                ? [by[cc][h].price_eur, by[cc][h].solar_share_pct] : ['—', '—'])]));
}

// Average hourly demand and generation by source during the 28 Jul - 12 Aug
// 2026 heatwave, one panel per country. Generation stacks above zero, net
// exports below it, demand rides on top as a line.
const HW_EP_ORDER = ['nuclear', 'fossil', 'other_renewables', 'solar', 'storage', 'net_import'];
const HW_EP_COLOR = {
    solar: '#22c55e', storage: '#f5c542', nuclear: '#6b74a8',
    other_renewables: '#a5e8e0', fossil: '#c4c4c4', net_import: '#7cc3ea',
};
const HW_EP_LABEL = {
    solar: 'Solar', storage: 'Storage', nuclear: 'Nuclear',
    other_renewables: 'Other renewables', fossil: 'Fossil', net_import: 'Net import',
};
const HW_EP_DEMAND = '#1e3a6d';

function hwRenderEventProfile() {
    const host = document.getElementById('hwEventProfile');
    if (!host) return;
    host.innerHTML = '';
    const rows = hwData.eventProfile || [];
    if (!rows.length) return;

    const by = {};
    rows.forEach(r => {
        const cc = r.country_code, h = Number(r.hour);
        ((by[cc] ||= {})[h] ||= {})[r.category] = Number(r.gwh);
    });
    // Biggest system first so the eye starts where the megawatts are.
    const order = Object.keys(by).sort((a, b) => {
        const pk = cc => Math.max(...Object.values(by[cc]).map(o => o.demand || 0));
        return pk(b) - pk(a);
    });

    hwLegend('hwEventProfileLegend', [
        {c: HW_EP_DEMAND, t: 'Demand'},
        ...HW_EP_ORDER.slice().reverse().map(k => ({c: HW_EP_COLOR[k], t: HW_EP_LABEL[k]})),
    ]);

    order.forEach(cc => {
        const panel = document.createElement('div');
        panel.className = 'hw-ep-panel';
        const h3 = document.createElement('h4');
        const peak = Math.max(...Object.values(by[cc]).map(o => o.demand || 0));
        h3.textContent = `${hwName(cc)} — peak ${hwFmt(peak, 1)} GWh`;
        panel.appendChild(h3);
        // Fixed viewBox instead of measuring the panel: clientWidth is read
        // before CSS grid has resolved, so the first panels drew far wider than
        // their cell and bled across their neighbours. In a viewBox the
        // coordinates are our own and the browser scales them to fit.
        const W = 360, H = 230;
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('role', 'img');
        svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
        svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');
        svg.setAttribute('aria-label', `${hwName(cc)} hourly demand and generation`);
        panel.appendChild(svg);
        host.appendChild(panel);

        const hours = [...Array(24).keys()].filter(h => by[cc][h]);
        const m = {t: 10, r: 10, b: 24, l: 40};
        const iw = W - m.l - m.r, ih = H - m.t - m.b;

        // Domain must hold the positive stack, the negative stack and demand.
        let hi = 0, lo = 0;
        hours.forEach(h => {
            const d = by[cc][h];
            let p = 0, n = 0;
            HW_EP_ORDER.forEach(k => {
                const v = d[k] || 0;
                if (v >= 0) p += v; else n += v;
            });
            hi = Math.max(hi, p, d.demand || 0);
            lo = Math.min(lo, n);
        });
        const pad = (hi - lo) * 0.06 || 1;
        hi += pad;
        const X = h => m.l + (h / 23) * iw;
        const Y = v => m.t + ih - ((v - lo) / ((hi - lo) || 1)) * ih;

        // About four gridlines on a 1/2/5 scale, always including zero.
        const raw = (hi - lo) / 4;
        const mag = Math.pow(10, Math.floor(Math.log10(Math.max(raw, 1e-6))));
        const norm = raw / mag;
        const step = (norm > 5 ? 10 : norm > 2 ? 5 : norm > 1 ? 2 : 1) * mag;
        for (let v = Math.ceil(lo / step) * step; v <= hi; v += step) {
            const zero = Math.abs(v) < step / 1000;
            hwEl(svg, 'line', {x1: m.l, x2: m.l + iw, y1: Y(v), y2: Y(v),
                stroke: zero ? '#b9b8ae' : '#eceae2', 'stroke-width': 1});
            hwEl(svg, 'text', {x: m.l - 5, y: Y(v) + 3.5, class: 'hw-tick', 'text-anchor': 'end'},
                zero ? '0' : hwFmt(v, 0));
        }

        // Positive bands stack up from zero, negative bands down.
        const accP = {}, accN = {};
        hours.forEach(h => { accP[h] = 0; accN[h] = 0; });
        HW_EP_ORDER.forEach(k => {
            const present = hours.some(h => Math.abs(by[cc][h][k] || 0) > 1e-9);
            if (!present) return;
            const pts = [], back = [];
            hours.forEach(h => {
                const v = by[cc][h][k] || 0;
                const from = v >= 0 ? accP[h] : accN[h];
                const to = from + v;
                pts.push(`${X(h)},${Y(to)}`);
                back.push(`${X(h)},${Y(from)}`);
                if (v >= 0) accP[h] = to; else accN[h] = to;
            });
            hwEl(svg, 'polygon', {points: [...pts, ...back.reverse()].join(' '),
                fill: HW_EP_COLOR[k], stroke: 'none'});
        });

        hwEl(svg, 'polyline', {fill: 'none', stroke: HW_EP_DEMAND, 'stroke-width': 2.5,
            'stroke-linejoin': 'round',
            points: hours.map(h => `${X(h)},${Y(by[cc][h].demand || 0)}`).join(' ')});

        // Hour labels only; the axis is named once on the card, not five times.
        [0, 6, 12, 18].forEach(h => {
            hwEl(svg, 'text', {x: X(h), y: H - 8, class: 'hw-tick', 'text-anchor': 'middle'},
                String(h).padStart(2, '0') + 'h');
        });

        hours.forEach(h => {
            const d = by[cc][h];
            const hit = hwEl(svg, 'rect', {x: X(h) - iw / 46, y: m.t, width: iw / 23,
                height: ih, fill: 'transparent'});
            const lines = HW_EP_ORDER.slice().reverse()
                .filter(k => Math.abs(d[k] || 0) > 1e-9)
                .map(k => `${HW_EP_LABEL[k]} ${hwFmt(d[k], 1)}`).join('<br>');
            hwTip(hit, `<b>${hwName(cc)} · ${String(h).padStart(2, '0')}:00</b><br>
                Demand ${hwFmt(d.demand, 1)} GWh<br>${lines}`);
        });
    });

    hwTable('hwEventProfileTbl',
        ['Country', 'Hour', 'Demand', ...HW_EP_ORDER.map(k => HW_EP_LABEL[k])],
        order.flatMap(cc => [...Array(24).keys()].filter(h => by[cc][h]).map(h =>
            [hwName(cc), String(h).padStart(2, '0') + ':00',
             hwFmt(by[cc][h].demand, 1),
             ...HW_EP_ORDER.map(k => by[cc][h][k] != null ? hwFmt(by[cc][h][k], 1) : '—')])));
}

// Two maps: what fell most in each country, and what rose most.
//
// Values are POWER, not percentages. A percentage is unusable here because the
// imports component crosses zero: Bulgaria's net imports fall 4.9 GWh/day on a
// negative base, which arithmetic reports as +36%. Gigawatts and megawatts are
// unambiguous for every component and are the unit the subject is read in.
//
// Country fill is a light tint of the first-ranked component, so the map reads
// as a soft categorical field rather than a saturated quilt; the numbers live
// in a badge with its own ground so they stay legible over any fill.

// Adaptive: 4,073 MW reads better as 4.1 GW, 155 MW as itself.
function hwPower(mw) {
    const v = Math.abs(Number(mw) || 0);
    return v >= 1000 ? (v / 1000).toFixed(1) + ' GW' : Math.round(v) + ' MW';
}

// Desaturated companions to HW_FUEL_COLOR, for the map fill.
const HW_FUEL_TINT = {
    gas: '#f6d9cc', coal: '#ded9d3', other: '#e8d9ea', biomass: '#d5e8d5',
    nuclear: '#dcdcef', hydro: '#d3e6e4', solar: '#f7ecc9', wind: '#d5e2f5',
    imports: '#d2ece0',
};

// Push overlapping badges apart instead of nudging them by hand.
//
// Hand-tuned offsets could not hold: Belgium ended up behind the Netherlands and
// Austria's name under Germany's badge. This separates boxes along whichever
// axis they overlap least, largest country first so the big ones keep their
// centroid, then clamps everything inside the frame. Anything displaced far
// enough gets a leader line back to where it belongs.
function hwLayoutBadges(items, W, H, margin) {
    const boxes = items.map(it => ({...it, x: it.ax, y: it.ay}));
    boxes.sort((a, b) => b.weight - a.weight);
    const overlap = (a, b) => {
        const dx = (a.w + b.w) / 2 + 3 - Math.abs(a.x - b.x);
        const dy = (a.h + b.h) / 2 + 3 - Math.abs(a.y - b.y);
        return dx > 0 && dy > 0 ? {dx, dy} : null;
    };
    for (let pass = 0; pass < 90; pass++) {
        let moved = false;
        for (let i = 0; i < boxes.length; i++) {
            for (let j = i + 1; j < boxes.length; j++) {
                const a = boxes[i], b = boxes[j];
                const o = overlap(a, b);
                if (!o) continue;
                moved = true;
                // Separate along the cheaper axis; the later box yields more so
                // the biggest systems stay put.
                if (o.dx < o.dy) {
                    const s = (a.x <= b.x ? -1 : 1) * o.dx;
                    a.x += s * 0.35; b.x -= s * 0.65;
                } else {
                    const s = (a.y <= b.y ? -1 : 1) * o.dy;
                    a.y += s * 0.35; b.y -= s * 0.65;
                }
            }
        }
        boxes.forEach(v => {
            v.x = Math.min(W - margin - v.w / 2, Math.max(margin + v.w / 2, v.x));
            v.y = Math.min(H - margin - v.h / 2, Math.max(margin + v.h / 2, v.y));
        });
        if (!moved) break;
    }
    return boxes;
}

async function hwRenderImpactMaps() {
    const rows = hwData.impact || [];
    if (!rows.length) return;
    const geo = await fetchEuropeCountriesGeoJsonOnce().catch(() => null);
    const feats = Array.isArray(geo?.features) ? geo.features : [];

    const byCc = {};
    rows.forEach(r => { (byCc[r.country_code] ||= []).push(r); });

    [['down', 'hwMapDown'], ['up', 'hwMapUp']].forEach(([dir, hostId]) => {
        const host = document.getElementById(hostId);
        if (!host) return;
        // Wider than the countries themselves: badges sit around the shapes and
        // were being clipped at the frame, Portugal and Greece worst.
        const W = 1060, H = 660, pad = 8;
        const bounds = {minLon: -16, maxLon: 36, minLat: 33, maxLat: 63};

        if (!feats.length) {
            host.innerHTML = '<p class="hw-foot">Map outlines unavailable — see the data table below.</p>';
            return;
        }
        host.innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" class="hw-impact-svg"
            aria-label="Top two components ${dir === 'down' ? 'reduced' : 'increased'} per country"></svg>`;
        const svg = host.querySelector('svg');

        // Which components actually appear, so the legend lists only those.
        const used = new Set();
        const pending = [];
        const pick = cc => {
            const list = (byCc[cc] || []).slice()
                .sort((a, b) => dir === 'down'
                    ? Number(a.delta_gwh) - Number(b.delta_gwh)
                    : Number(b.delta_gwh) - Number(a.delta_gwh))
                // Only genuine movement in the asked-for direction.
                .filter(r => dir === 'down' ? Number(r.delta_gwh) < -0.05 : Number(r.delta_gwh) > 0.05);
            return list.slice(0, 2);
        };

        feats.forEach(f => {
            const iso2 = String(f?.properties?.ISO2 || '').toUpperCase();
            if (!iso2) return;
            const cc = iso2GeoToDataKey(iso2);
            const top = pick(cc);
            const geom = f.geometry;
            if (!geom) return;
            const paths = [];
            if (geom.type === 'Polygon') paths.push(polygonToPath(geom.coordinates[0], W, H, bounds, pad));
            else if (geom.type === 'MultiPolygon') {
                geom.coordinates.forEach(p => { if (p?.[0]) paths.push(polygonToPath(p[0], W, H, bounds, pad)); });
            } else return;

            const first = top[0];
            if (first) used.add(first.component);
            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path.setAttribute('d', paths.join(' '));
            // A light tint, not the full hue: the badge sits on top and needs a
            // quiet ground, and twenty saturated fills read as a quilt.
            path.setAttribute('fill', first ? (HW_FUEL_TINT[first.component] || '#e4e6ea') : NO_DATA_FILL);
            path.setAttribute('stroke', '#ffffff');
            path.setAttribute('stroke-width', '0.9');
            svg.appendChild(path);

            if (!top.length) return;
            // Collect the badge; placement happens once all are known.
            let ring = geom.type === 'Polygon' ? geom.coordinates[0]
                : geom.coordinates.map(p => p[0]).sort((a, b) => b.length - a.length)[0];
            if (!ring || !ring.length) return;
            let sx = 0, sy = 0;
            ring.forEach(([lon, lat]) => {
                const [x, y] = projectLonLat(lon, lat, W, H, bounds, pad);
                sx += x; sy += y;
            });

            const lines = top.map(r => ({
                fuel: r.component === 'imports' ? 'Trade' : hwCap(r.component),
                val: hwPower(r.delta_mw != null ? r.delta_mw : Number(r.delta_gwh) * 1000 / 24),
                col: HW_FUEL_COLOR[r.component] || '#8a8f98',
            }));
            // Estimate the box from the longest row; SVG cannot measure text
            // before it is laid out.
            const widest = Math.max(hwName(cc).length + 1,
                ...lines.map(l => l.fuel.length + l.val.length + 4));
            pending.push({
                cc, lines,
                ax: sx / ring.length, ay: sy / ring.length,
                w: Math.max(56, widest * 5.0 + 16),
                h: 15 + lines.length * 11.5,
                weight: Math.abs(Number(top[0].delta_mw) || 0),
            });

            const tip = top.map(r => `${hwCap(r.component === 'imports' ? 'net trade' : r.component)} `
                + `${hwSign(r.delta_gwh, 1)} GWh/day (${hwPower(r.delta_mw)})`).join('<br>');
            hwTip(path, `<b>${hwName(cc)}</b><br>${tip}`);
        });

        // Second pass: separate the badges, then draw them over the shapes.
        const arrow = dir === 'down' ? '▼' : '▲';
        hwLayoutBadges(pending, W, H, 4).forEach(b => {
            const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            g.setAttribute('class', 'hw-map-badge');
            // A leader line only where the badge had to travel far enough that
            // its country is no longer obvious.
            const dist = Math.hypot(b.x - b.ax, b.y - b.ay);
            if (dist > b.w / 2 + 6) {
                const ln = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                ln.setAttribute('x1', b.ax); ln.setAttribute('y1', b.ay);
                ln.setAttribute('x2', b.x); ln.setAttribute('y2', b.y);
                ln.setAttribute('class', 'hw-map-leader');
                g.appendChild(ln);
                const dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                dot.setAttribute('cx', b.ax); dot.setAttribute('cy', b.ay);
                dot.setAttribute('r', 1.8);
                dot.setAttribute('class', 'hw-map-anchor');
                g.appendChild(dot);
            }
            const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
            rect.setAttribute('x', b.x - b.w / 2); rect.setAttribute('y', b.y - b.h / 2);
            rect.setAttribute('width', b.w); rect.setAttribute('height', b.h);
            rect.setAttribute('rx', 5);
            g.appendChild(rect);

            const mk = (txt, x, y, cls, fill) => {
                const t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                t.setAttribute('x', x); t.setAttribute('y', y);
                t.setAttribute('class', cls);
                if (fill) t.setAttribute('fill', fill);
                t.textContent = txt;
                g.appendChild(t);
            };
            const left = b.x - b.w / 2 + 7;
            mk(hwName(b.cc), left, b.y - b.h / 2 + 11, 'hw-map-cc');
            b.lines.forEach((l, i) => {
                const y = b.y - b.h / 2 + 22.5 + i * 11.5;
                mk(arrow, left, y, 'hw-map-arrow', l.col);
                mk(`${l.fuel} ${l.val}`, left + 10, y, i === 0 ? 'hw-map-l1' : 'hw-map-l2');
            });
            svg.appendChild(g);
        });

        hwLegend(dir === 'down' ? 'hwMapDownLegend' : 'hwMapUpLegend',
            [...used].sort().map(c => ({
                c: HW_FUEL_COLOR[c] || '#8a8f98',
                t: c === 'imports' ? 'Net trade' : hwCap(c),
            })));
    });

    const label = c => (c === 'imports' ? 'Net trade' : hwCap(c));
    const tbl = (dir, id) => hwTable(id,
        ['Country', 'Largest ' + (dir === 'down' ? 'fall' : 'rise'), 'Power', 'GWh/day',
         'Second', 'Power', 'GWh/day'],
        Object.keys(byCc).sort().map(cc => {
            const list = byCc[cc].slice().sort((a, b) => dir === 'down'
                ? Number(a.delta_gwh) - Number(b.delta_gwh)
                : Number(b.delta_gwh) - Number(a.delta_gwh))
                .filter(r => dir === 'down' ? Number(r.delta_gwh) < -0.05 : Number(r.delta_gwh) > 0.05);
            const cell = r => r ? [label(r.component), hwPower(r.delta_mw), hwFmt(r.delta_gwh, 1)]
                               : ['—', '—', '—'];
            return [hwName(cc), ...cell(list[0]), ...cell(list[1])];
        }).filter(r => r[1] !== '—'));
    tbl('down', 'hwMapDownTbl');
    tbl('up', 'hwMapUpTbl');
}

function hwRenderFuels() {
    const src = hwData.fuels.slice().sort((a, b) => b.avg - a.avg);
    const rows = src.map(r => ({
        label: hwCap(r.fuel), v: r.avg, lo: r.worst, hi: r.best,
        vlabel: hwSign(r.avg, 1) + '%',
        tip: `<b>${hwCap(r.fuel)}</b><br>EU-wide ${hwSign(r.avg, 1)}%<br>
              ${hwFmt(r.normal_mw)} → ${hwFmt(r.heatwave_mw)} MW<br>
              Per-country range ${hwFmt(r.worst, 1)}% to ${hwFmt(r.best, 1)}%<br>
              ${r.countries} countries`,
    }));
    hwDiverging('hwFuels', rows, {axis: 'Change in output during heatwaves (%)', rowH: 30, left: 76});
    hwTable('hwFuelsTbl', ['Fuel', 'Normal MW', 'Heatwave MW', 'Change %', 'Countries'],
        src.map(s => [hwCap(s.fuel), hwFmt(s.normal_mw), hwFmt(s.heatwave_mw),
            hwFmt(s.avg, 1), s.countries]));
}

// Min–mean–max strips: both day-pools drawn IN the chart — normal above,
// heatwave below — band spanning min to max across days, dot at the mean.
// This is the ranges made visible rather than hidden in tooltips and tables.
function hwRangeStrip(svgId, rows, opts) {
    const svg = document.getElementById(svgId);
    if (!svg) return;
    hwClear(svg);
    if (!rows.length) {
        svg.setAttribute('height', 60);
        hwEl(svg, 'text', {x: 12, y: 32, class: 'hw-lbl'}, 'No data.');
        return;
    }
    const rowH = 44;
    const H = rows.length * rowH + 48;
    svg.setAttribute('height', H);
    const W = svg.clientWidth || 800;
    const m = {t: 10, r: 86, b: 36, l: opts.left || 104};
    const iw = Math.max(80, W - m.l - m.r);
    const all = rows.flatMap(r => [...r.n, ...r.h]).filter(Number.isFinite);
    let lo = Math.min(...all), hi = Math.max(...all);
    const pad = (hi - lo) * 0.05 || 1;
    lo -= pad; hi += pad;
    const X = v => m.l + ((v - lo) / ((hi - lo) || 1)) * iw;
    const fmt = opts.fmt || (v => Math.round(v));

    const TICKS = 4;
    for (let i = 0; i <= TICKS; i++) {
        const v = lo + (hi - lo) * i / TICKS;
        hwEl(svg, 'line', {x1: X(v), x2: X(v), y1: m.t - 2, y2: m.t + rows.length * rowH - 12,
            stroke: '#eceae2', 'stroke-width': 1});
        hwEl(svg, 'text', {x: X(v), y: H - 26, class: 'hw-tick', 'text-anchor': 'middle'}, fmt(v));
    }

    // Every point carries its number ON the chart: min at the left end of the
    // band, max at the right end, mean at the dot — not only in hover or table.
    const bandLabels = (vals, yBand, yText) => {
        const [vMin, vMean, vMax] = vals;
        hwEl(svg, 'text', {x: X(vMin) - 4, y: yText, class: 'hw-tick', 'text-anchor': 'end'}, fmt(vMin));
        hwEl(svg, 'text', {x: X(vMax) + 4, y: yText, class: 'hw-tick'}, fmt(vMax));
        // The mean label needs room on both sides of the dot or it collides
        // with the min/max labels; when squeezed the dot and hover carry it.
        if (X(vMean) - X(vMin) > 30 && X(vMax) - X(vMean) > 30) {
            hwEl(svg, 'text', {x: X(vMean), y: yText, class: 'hw-tick',
                'text-anchor': 'middle', 'font-weight': 600}, fmt(vMean));
        }
    };

    rows.forEach((r, i) => {
        const y = m.t + i * rowH;
        hwEl(svg, 'text', {x: m.l - 8, y: y + 22, class: 'hw-lbl', 'text-anchor': 'end'}, r.label);
        // Normal pool: band, dot, and its three numbers above.
        hwEl(svg, 'rect', {x: X(r.n[0]), y: y + 10, height: 5, rx: 2.5, fill: '#c9c8bf',
            width: Math.max(2, X(r.n[2]) - X(r.n[0]))});
        hwEl(svg, 'circle', {cx: X(r.n[1]), cy: y + 12.5, r: 4, fill: '#6b6a62',
            stroke: '#ffffff', 'stroke-width': 1.5});
        bandLabels(r.n, y + 10, y + 6);
        // Heatwave pool: band, dot, numbers below.
        hwEl(svg, 'rect', {x: X(r.h[0]), y: y + 22, height: 5, rx: 2.5, fill: 'rgba(194,86,47,0.30)',
            width: Math.max(2, X(r.h[2]) - X(r.h[0]))});
        hwEl(svg, 'circle', {cx: X(r.h[1]), cy: y + 24.5, r: 4, fill: HW_NEG,
            stroke: '#ffffff', 'stroke-width': 1.5});
        bandLabels(r.h, y + 22, y + 38);
        hwEl(svg, 'text', {x: m.l + iw + 10, y: y + 22, class: 'hw-val'}, r.vlabel);
        const hit = hwEl(svg, 'rect', {x: m.l, y, width: iw + 78, height: rowH - 4, fill: 'transparent'});
        hwTip(hit, r.tip);
    });
    hwEl(svg, 'text', {x: m.l + iw / 2, y: H - 8, class: 'hw-lbl', 'text-anchor': 'middle'}, opts.axis);
}

const HW_RANGE_LEGEND = [
    {c: '#6b6a62', t: 'Normal days — band min–max, dot mean'},
    {c: HW_NEG, t: 'Heatwave days — band min–max, dot mean'},
];

function hwRenderRenewable() {
    const src = hwData.renewable.slice().sort((a, b) => a.delta_pp - b.delta_pp).slice(0, 14);
    hwRangeStrip('hwRen', src.map(r => ({
        label: hwName(r.country_code),
        n: [+r.normal_min_pct, +r.normal_renewable_pct, +r.normal_max_pct],
        h: [+r.heatwave_min_pct, +r.heatwave_renewable_pct, +r.heatwave_max_pct],
        vlabel: hwSign(r.delta_pp, 1) + ' pp',
        tip: `<b>${hwName(r.country_code)}</b><br>
              Normal: min ${r.normal_min_pct} · mean ${r.normal_renewable_pct} ·
              max ${r.normal_max_pct}%<br>
              Heatwave: min ${r.heatwave_min_pct} · mean ${r.heatwave_renewable_pct} ·
              max ${r.heatwave_max_pct}%<br>${r.heatwave_days} heatwave days`,
    })), {axis: 'Renewable share of generation (%) — daily min · mean · max', fmt: v => Math.round(v) + '%'});
    hwLegend('hwRenLegend', HW_RANGE_LEGEND);
    hwTable('hwRenTbl',
        ['Country', 'Normal min %', 'Normal mean %', 'Normal max %',
         'Heatwave min %', 'Heatwave mean %', 'Heatwave max %', 'Δ pp'],
        src.map(r => [hwName(r.country_code), r.normal_min_pct, r.normal_renewable_pct,
            r.normal_max_pct, r.heatwave_min_pct, r.heatwave_renewable_pct,
            r.heatwave_max_pct, r.delta_pp]));
}

function hwRenderPrice() {
    const src = hwData.price.slice().sort((a, b) => b.change_pct - a.change_pct).slice(0, 14);
    hwRangeStrip('hwPrice', src.map(r => ({
        label: hwName(r.country_code),
        n: [+r.normal_min_eur, +r.normal_price_eur, +r.normal_max_eur],
        h: [+r.heatwave_min_eur, +r.heatwave_price_eur, +r.heatwave_max_eur],
        vlabel: hwSign(r.change_pct, 0) + '%',
        tip: `<b>${hwName(r.country_code)}</b><br>
              Normal: min €${r.normal_min_eur} · mean €${r.normal_price_eur} ·
              max €${r.normal_max_eur}<br>
              Heatwave: min €${r.heatwave_min_eur} · mean €${r.heatwave_price_eur} ·
              max €${r.heatwave_max_eur}<br>
              ${hwSign(r.delta_eur, 1)} €/MWh on ${r.heatwave_days} days`,
    })), {axis: 'Day-ahead price (€/MWh) — daily min · mean · max', fmt: v => '€' + Math.round(v)});
    hwLegend('hwPriceLegend', HW_RANGE_LEGEND);
    hwTable('hwPriceTbl',
        ['Country', 'Normal min €', 'Normal mean €', 'Normal max €',
         'Heatwave min €', 'Heatwave mean €', 'Heatwave max €', 'Δ €', 'Δ %'],
        src.map(r => [hwName(r.country_code), r.normal_min_eur, r.normal_price_eur,
            r.normal_max_eur, r.heatwave_min_eur, r.heatwave_price_eur,
            r.heatwave_max_eur, r.delta_eur, r.change_pct]));
}

function hwRenderGas() {
    const svg = document.getElementById('hwGas');
    if (!svg) return;
    hwClear(svg);
    const rows = hwData.gas.slice().sort((a, b) => +b.delta_power_gwh - +a.delta_power_gwh).slice(0, 10);
    const segs = [['delta_power_gwh', HW_FUEL_COLOR.gas, 'Power'],
                  ['delta_household_gwh', HW_POS, 'Households'],
                  ['delta_industry_gwh', '#4a3aa7', 'Industry']];
    const H = rows.length * 30 + 36;
    svg.setAttribute('height', H);
    const W = svg.clientWidth || 640;
    const m = {t: 8, r: 54, b: 24, l: 92};
    const iw = Math.max(80, W - m.l - m.r);
    const mx = Math.max(...rows.flatMap(r => segs.map(s => Math.abs(+r[s[0]] || 0))), 0.001) * 1.12;
    const X = v => m.l + iw / 2 + (v / mx) * (iw / 2);

    hwEl(svg, 'line', {x1: X(0), x2: X(0), y1: m.t - 4, y2: m.t + rows.length * 30 - 8,
        stroke: '#c3c2b7', 'stroke-width': 1});
    rows.forEach((r, i) => {
        const y = m.t + i * 30;
        segs.forEach(([key, col, nm], k) => {
            const v = +r[key] || 0;
            if (!v) return;
            const x = v < 0 ? X(v) : X(0), w = Math.max(2, Math.abs(X(v) - X(0)));
            const rect = hwEl(svg, 'rect', {x, y: y + k * 8, width: w, height: 7, rx: 2, fill: col});
            hwTip(rect, `<b>${hwName(r.country_code)} · ${nm}</b><br>${hwSign(v, 1)} GWh/day`);
        });
        hwEl(svg, 'text', {x: m.l - 8, y: y + 14, class: 'hw-lbl', 'text-anchor': 'end'},
            hwName(r.country_code));
    });
    hwEl(svg, 'text', {x: m.l + iw / 2, y: H - 5, class: 'hw-lbl', 'text-anchor': 'middle'},
        'Change in gas demand by sector (GWh/day)');
    hwLegend('hwGasLegend', segs.map(([, c, n]) => ({c, t: n})));
    hwTable('hwGasTbl', ['Country', 'Δ power', 'Δ households', 'Δ industry', 'Power Δ%'],
        rows.map(r => [hwName(r.country_code), r.delta_power_gwh, r.delta_household_gwh,
            r.delta_industry_gwh, r.power_change_pct]));
}

function hwRenderResponse(sel) {
    const svg = document.getElementById('hwResp');
    if (!svg) return;
    hwClear(svg);
    const by = {};
    hwData.response.forEach(r => { (by[r.country_code] ||= []).push(r); });
    Object.values(by).forEach(a => a.sort((p, q) => p.tmax_bin - q.tmax_bin));

    const W = svg.clientWidth || 800, H = 340;
    svg.setAttribute('height', H);
    const m = {t: 14, r: 52, b: 44, l: 46};
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    const xs = hwData.response.map(r => +r.tmax_bin), ys = hwData.response.map(r => +r.demand_index);
    const x0 = Math.min(...xs), x1 = Math.max(...xs);
    const y0 = Math.floor(Math.min(...ys) / 10) * 10, y1 = Math.ceil(Math.max(...ys) / 10) * 10;
    const X = v => m.l + (v - x0) / (x1 - x0) * iw;
    const Y = v => m.t + ih - (v - y0) / (y1 - y0) * ih;

    for (let v = y0; v <= y1; v += 10) {
        hwEl(svg, 'line', {x1: m.l, x2: m.l + iw, y1: Y(v), y2: Y(v), stroke: '#e1e0d9', 'stroke-width': 1});
        hwEl(svg, 'text', {x: m.l - 8, y: Y(v) + 4, class: 'hw-tick', 'text-anchor': 'end'}, v);
    }
    hwEl(svg, 'line', {x1: m.l, x2: m.l + iw, y1: Y(100), y2: Y(100), stroke: '#c3c2b7', 'stroke-width': 1.5});
    for (let v = x0; v <= x1; v += 3)
        hwEl(svg, 'text', {x: X(v), y: m.t + ih + 20, class: 'hw-tick', 'text-anchor': 'middle'}, v + '°');
    hwEl(svg, 'text', {x: m.l + iw / 2, y: H - 6, class: 'hw-lbl', 'text-anchor': 'middle'},
        'Daily maximum temperature (°C)');

    const line = pts => pts.map((p, i) =>
        (i ? 'L' : 'M') + X(+p.tmax_bin) + ' ' + Y(+p.demand_index)).join(' ');

    Object.entries(by).forEach(([cc, pts]) => {
        if (cc === sel || pts.length < 2) return;
        hwEl(svg, 'path', {d: line(pts), fill: 'none', stroke: HW_DEEMPH, 'stroke-width': 1.4});
    });
    const pts = by[sel] || [];
    if (pts.length > 1) {
        hwEl(svg, 'path', {d: line(pts), fill: 'none', stroke: HW_ACCENT, 'stroke-width': 2.5,
            'stroke-linejoin': 'round', 'stroke-linecap': 'round'});
        pts.forEach(p => {
            hwEl(svg, 'circle', {cx: X(+p.tmax_bin), cy: Y(+p.demand_index), r: 4.5,
                fill: HW_ACCENT, stroke: '#ffffff', 'stroke-width': 2});
            const hit = hwEl(svg, 'circle', {cx: X(+p.tmax_bin), cy: Y(+p.demand_index), r: 13,
                fill: 'transparent'});
            hwTip(hit, `<b>${hwName(sel)}</b><br>${p.tmax_bin}°C band<br>
                Demand index ${hwFmt(p.demand_index, 1)}<br>${hwFmt(p.days)} days`);
        });
    }
    hwLegend('hwRespLegend', [{c: HW_ACCENT, t: hwName(sel)}, {c: HW_DEEMPH, t: 'Other countries'}]);
    hwTable('hwRespTbl', ['Temperature band', 'Demand index', 'Days'],
        pts.map(p => [p.tmax_bin + '°C', p.demand_index, p.days]));
}

// ── Heat burden this year (sequential: magnitude, no identity to encode) ───
function hwRenderBurden() {
    const svg = document.getElementById('hwBurden');
    if (!svg) return;
    hwClear(svg);
    const rows = hwData.burden.slice().sort((a, b) => b.heatwave_days - a.heatwave_days);
    if (!rows.length) { svg.setAttribute('height', 50); return; }

    const H = rows.length * 17 + 30;
    svg.setAttribute('height', H);
    const W = svg.clientWidth || 800;
    const m = {t: 6, r: 118, b: 20, l: 96};
    const iw = Math.max(80, W - m.l - m.r);
    const mx = Math.max(...rows.map(r => +r.heatwave_days)) * 1.03;
    const bh = 11;

    rows.forEach((r, i) => {
        const y = m.t + i * 17;
        const w = Math.max(2, (+r.heatwave_days / mx) * iw);
        const step = HW_SEQ[Math.min(HW_SEQ.length - 1,
            Math.floor((+r.heatwave_days / mx) * HW_SEQ.length))];
        hwEl(svg, 'rect', {x: m.l, y, width: w, height: bh, rx: 4, fill: step});
        hwEl(svg, 'text', {x: m.l - 8, y: y + bh - 1, class: 'hw-lbl', 'text-anchor': 'end'},
            hwName(r.country_code));
        hwEl(svg, 'text', {x: m.l + w + 8, y: y + bh - 1, class: 'hw-val'},
            `${r.heatwave_days} d · ${r.peak_tmax_c}°C`);
        const hit = hwEl(svg, 'rect', {x: m.l, y: y - 3, width: iw, height: bh + 6, fill: 'transparent'});
        hwTip(hit, `<b>${hwName(r.country_code)}</b><br>${r.heatwave_days} heatwave days in
            ${r.events} events<br>Peak ${r.peak_tmax_c}°C (+${r.peak_anomaly_c}°C anomaly)`);
    });
    hwTable('hwBurdenTbl', ['Country', 'Heatwave days', 'Events', 'Peak °C', 'Peak anomaly °C'],
        rows.map(r => [hwName(r.country_code), r.heatwave_days, r.events, r.peak_tmax_c, r.peak_anomaly_c]));
}

// ── Anatomy of one event: three panels on ONE shared time axis ─────────────
function hwRenderEvent(sel) {
    const svg = document.getElementById('hwEvent');
    const title = document.getElementById('hwEventTitle');
    if (!svg) return;
    hwClear(svg);

    const ev = hwData.events.find(e => e.country_code === sel);
    const series = hwData.eventSeries.filter(r => r.country_code === sel)
        .sort((a, b) => String(a.date).localeCompare(String(b.date)));

    if (!ev || series.length < 3) {
        svg.setAttribute('height', 50);
        hwEl(svg, 'text', {x: 8, y: 28, class: 'hw-lbl'},
            `No heatwave event recorded for ${hwName(sel)} this year.`);
        if (title) title.textContent = 'Anatomy of the largest event';
        hwLegend('hwEventLegend', []);
        hwTable('hwEventTbl', ['Date'], []);
        return;
    }
    if (title) {
        title.textContent = `Anatomy of the largest event — ${hwName(sel)}, ` +
            `${ev.start_date} to ${ev.end_date} (${ev.length_days} days, ` +
            `peak ${ev.peak_tmax_c}°C, +${ev.peak_anomaly_c}°C above threshold)`;
    }

    const W = svg.clientWidth || 800;
    const m = {t: 18, r: 18, b: 34, l: 54};
    const iw = Math.max(80, W - m.l - m.r);
    const n = series.length;
    const bw = iw / n;
    const X = i => m.l + i * bw + bw / 2;
    const panelH = 148, gap = 36;

    const fuels = HW_FUEL_ORDER.filter(f => series.some(r => Number(r[f + '_mw']) > 0));

    function panel(top, label, y0, y1, unit, draw) {
        hwEl(svg, 'text', {x: m.l, y: top - 6, class: 'hw-lbl'}, label);
        const Y = v => top + panelH - (v - y0) / (y1 - y0 || 1) * panelH;
        for (let k = 0; k <= 2; k++) {
            const v = y0 + (y1 - y0) * k / 2;
            hwEl(svg, 'line', {x1: m.l, x2: m.l + iw, y1: Y(v), y2: Y(v),
                stroke: '#e1e0d9', 'stroke-width': 1});
            hwEl(svg, 'text', {x: m.l - 8, y: Y(v) + 4, class: 'hw-tick', 'text-anchor': 'end'},
                hwFmt(v) + (k === 2 ? ' ' + unit : ''));
        }
        draw(Y);
    }

    // 1 — temperature against the local threshold
    const temps = series.map(r => Number(r.tmax_c));
    const thr = Number(series[0].threshold_c);
    panel(m.t, 'Daily maximum temperature',
        Math.floor(Math.min(...temps, thr) - 3), Math.ceil(Math.max(...temps) + 2), '°C', Y => {
        hwEl(svg, 'line', {x1: m.l, x2: m.l + iw, y1: Y(thr), y2: Y(thr),
            stroke: HW_NEG, 'stroke-width': 1.5});
        // `threshold_c` is the EFFECTIVE threshold — the lower of the local 90th
        // percentile and the 30 C floor — so the line matches the rule that
        // actually flags the orange days. Name which rule is binding, otherwise
        // a France line at 30 C looks arbitrary next to a p90 of 31.6.
        const p90 = Number(series[0].threshold_p90_c);
        const label = Number.isFinite(p90) && p90 > thr
            ? `heatwave threshold ${thr}°C (30°C floor; local p90 ${p90}°C)`
            : `heatwave threshold ${thr}°C (local p90)`;
        hwEl(svg, 'text', {x: m.l + iw - 2, y: Y(thr) - 6, class: 'hw-tick',
            'text-anchor': 'end', fill: HW_NEG}, label);
        hwEl(svg, 'polyline', {points: series.map((r, i) => `${X(i)},${Y(Number(r.tmax_c))}`).join(' '),
            fill: 'none', stroke: HW_ACCENT, 'stroke-width': 2, 'stroke-linejoin': 'round'});
        series.forEach((r, i) => {
            hwEl(svg, 'circle', {cx: X(i), cy: Y(Number(r.tmax_c)), r: r.in_heatwave ? 5 : 3.5,
                fill: r.in_heatwave ? HW_NEG : HW_ACCENT, stroke: '#ffffff', 'stroke-width': 2});
            const hit = hwEl(svg, 'circle', {cx: X(i), cy: Y(Number(r.tmax_c)), r: 13, fill: 'transparent'});
            hwTip(hit, `<b>${r.date}</b><br>${r.tmax_c}°C${r.in_heatwave ? '<br>in heatwave' : ''}`);
        });
    });

    // 2 — demand
    const top2 = m.t + panelH + gap;
    const dem = series.map(r => Number(r.avg_load_mw)).filter(Number.isFinite);
    if (dem.length) {
        panel(top2, 'Average electricity demand',
            Math.floor(Math.min(...dem) * 0.94), Math.ceil(Math.max(...dem) * 1.03), 'MW', Y => {
            hwEl(svg, 'polyline', {
                points: series.filter(r => Number.isFinite(Number(r.avg_load_mw)))
                    .map(r => `${X(series.indexOf(r))},${Y(Number(r.avg_load_mw))}`).join(' '),
                fill: 'none', stroke: '#1baf7a', 'stroke-width': 2, 'stroke-linejoin': 'round'});
            series.forEach((r, i) => {
                const v = Number(r.avg_load_mw);
                if (!Number.isFinite(v)) return;
                hwEl(svg, 'circle', {cx: X(i), cy: Y(v), r: 3.5, fill: '#1baf7a',
                    stroke: '#ffffff', 'stroke-width': 2});
                const hit = hwEl(svg, 'circle', {cx: X(i), cy: Y(v), r: 13, fill: 'transparent'});
                hwTip(hit, `<b>${r.date}</b><br>${hwFmt(v)} MW average demand`);
            });
        });
    }

    // 3 — generation stack
    const top3 = top2 + panelH + gap;
    const totals = series.map(r => fuels.reduce((s, f) => s + (Number(r[f + '_mw']) || 0), 0));
    panel(top3, 'Generation by source', 0, Math.ceil(Math.max(...totals, 1) * 1.06), 'MW', Y => {
        series.forEach((r, i) => {
            let acc = 0;
            fuels.forEach(f => {
                const v = Number(r[f + '_mw']) || 0;
                if (!v) return;
                // 2px surface gap so segments read as separate without a border
                const h = Math.max(1, Y(acc) - Y(acc + v) - 2);
                const rect = hwEl(svg, 'rect', {x: X(i) - bw * 0.34, y: Y(acc + v),
                    width: bw * 0.68, height: h, rx: 2, fill: HW_FUEL_COLOR[f]});
                hwTip(rect, `<b>${r.date}</b><br>${hwCap(f)}: ${hwFmt(v)} MW`);
                acc += v;
            });
        });
    });

    series.forEach((r, i) => {
        if (i % Math.ceil(n / 8)) return;
        hwEl(svg, 'text', {x: X(i), y: top3 + panelH + 20, class: 'hw-tick', 'text-anchor': 'middle'},
            String(r.date).slice(5));
    });
    svg.setAttribute('height', top3 + panelH + 34);

    hwLegend('hwEventLegend', fuels.map(f => ({c: HW_FUEL_COLOR[f], t: hwCap(f)}))
        .concat([{c: HW_NEG, t: 'Heatwave day'}]));
    hwTable('hwEventTbl',
        ['Date', 'Tmax °C', 'Demand MW'].concat(fuels.map(hwCap)),
        series.map(r => [r.date, r.tmax_c, hwFmt(r.avg_load_mw)]
            .concat(fuels.map(f => hwFmt(r[f + '_mw'])))));
}

// ── Dumbbell: normal vs heatwave trade position ────────────────────────────
// "Before → after per item" is a dumbbell. A bar of the delta alone would hide
// the level, and the level is half the story: Italy already imports 64 GWh/day,
// so its +2.9 means its interconnectors were near their normal ceiling, while
// Portugal's +11.2 comes off a similar base and is a real swing.
function hwRenderTradeDumbbell(svgId, tblId, legendId, mode) {
    const key = mode === 'export' ? 'export' : 'import';
    // The view stores extremes as net IMPORTS; the export reading is the same
    // series negated, which also swaps min and max.
    const sgn = key === 'export' ? -1 : 1;
    const rows = hwData.trade.slice()
        .map(r => {
            const nmin = Number(r.normal_min_net_import_gwh), nmax = Number(r.normal_max_net_import_gwh);
            const hmin = Number(r.heatwave_min_net_import_gwh), hmax = Number(r.heatwave_max_net_import_gwh);
            return {
                cc: r.country_code,
                normal: Number(r[`normal_net_${key}_gwh`]),
                hw: Number(r[`heatwave_net_${key}_gwh`]),
                delta: Number(r[`delta_net_${key}_gwh`]),
                n: sgn > 0 ? [nmin, Number(r.normal_net_import_gwh), nmax]
                           : [-nmax, Number(r.normal_net_export_gwh), -nmin],
                h: sgn > 0 ? [hmin, Number(r.heatwave_net_import_gwh), hmax]
                           : [-hmax, Number(r.heatwave_net_export_gwh), -hmin],
            };
        })
        .filter(r => r.n.every(Number.isFinite) && r.h.every(Number.isFinite))
        .sort((a, b) => b.delta - a.delta)
        .slice(0, 12);

    hwRangeStrip(svgId, rows.map(r => ({
        label: hwName(r.cc),
        n: r.n, h: r.h,
        vlabel: hwSign(r.delta, 1),
        tip: `<b>${hwName(r.cc)}</b><br>
            Normal: min ${hwFmt(r.n[0], 1)} · mean ${hwFmt(r.n[1], 1)} · max ${hwFmt(r.n[2], 1)} GWh/day<br>
            Heatwave: min ${hwFmt(r.h[0], 1)} · mean ${hwFmt(r.h[1], 1)} · max ${hwFmt(r.h[2], 1)} GWh/day<br>
            Change in the mean ${hwSign(r.delta, 1)} GWh/day`,
    })), {axis: `Net ${key}s (GWh/day) — daily min · mean · max`, left: 92,
          fmt: v => Math.round(v)});

    hwLegend(legendId, HW_RANGE_LEGEND);
    hwTable(tblId,
        ['Country', `Normal min`, `Normal mean`, `Normal max`,
         `Heatwave min`, `Heatwave mean`, `Heatwave max`, 'Change'],
        rows.map(r => [hwName(r.cc), hwFmt(r.n[0], 1), hwFmt(r.n[1], 1), hwFmt(r.n[2], 1),
            hwFmt(r.h[0], 1), hwFmt(r.h[1], 1), hwFmt(r.h[2], 1), hwSign(r.delta, 1)]));
}

// ── Where the extra demand came from (stacked, terms sum to the demand change)
// ── Imports vs gas ─────────────────────────────────────────────────────────
function hwRenderGasImports() {
    const svg = document.getElementById('hwGasImp');
    if (!svg) return;
    hwClear(svg);
    // Explicit null checks: Number(null) is 0, not NaN, so isFinite alone let
    // Switzerland through with "gas — GWh/day" — a country with no gas fleet
    // has nothing to say about gas displacement and is excluded.
    const rows = hwData.sources.slice().filter(r =>
        r.extra_imports_gwh != null && r.extra_gas_gwh != null
        && Number.isFinite(Number(r.extra_imports_gwh)) && Number.isFinite(Number(r.extra_gas_gwh)));
    if (!rows.length) { svg.setAttribute('height', 50); return; }

    const W = svg.clientWidth || 800, H = 380;
    svg.setAttribute('height', H);
    const m = {t: 20, r: 24, b: 52, l: 62};
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    const xs = rows.map(r => Number(r.extra_imports_gwh));
    const ys = rows.map(r => Number(r.extra_gas_gwh));
    const xr = Math.max(...xs.map(Math.abs)) * 1.15 || 1;
    const yr = Math.max(...ys.map(Math.abs)) * 1.15 || 1;
    const X = v => m.l + iw / 2 + (v / xr) * (iw / 2);
    const Y = v => m.t + ih / 2 - (v / yr) * (ih / 2);

    hwEl(svg, 'line', {x1: m.l, x2: m.l + iw, y1: Y(0), y2: Y(0), stroke: '#c3c2b7', 'stroke-width': 1});
    hwEl(svg, 'line', {x1: X(0), x2: X(0), y1: m.t, y2: m.t + ih, stroke: '#c3c2b7', 'stroke-width': 1});
    // "imported more →" was wrong for exporters: France sat on that caption
    // while never importing on a single day — its exports fell. "Kept more at
    // home" is true in both cases: importing more, or exporting less.
    hwEl(svg, 'text', {x: m.l + iw, y: Y(0) + 16, class: 'hw-tick', 'text-anchor': 'end'},
        'kept more power at home →');
    hwEl(svg, 'text', {x: m.l, y: Y(0) + 16, class: 'hw-tick'}, '← sent more abroad');
    hwEl(svg, 'text', {x: X(0) + 6, y: m.t + 10, class: 'hw-tick'}, '↑ burned more gas');

    rows.forEach(r => {
        const x = X(Number(r.extra_imports_gwh)), y = Y(Number(r.extra_gas_gwh));
        // Bottom-right = imported more, burned less: interconnection displacing gas.
        const displacing = Number(r.extra_imports_gwh) > 0 && Number(r.extra_gas_gwh) < 0;
        hwEl(svg, 'circle', {cx: x, cy: y, r: 6,
            fill: displacing ? '#1baf7a' : HW_POS, stroke: '#ffffff', 'stroke-width': 2});
        hwEl(svg, 'text', {x: x + 9, y: y + 4, class: 'hw-tick'}, r.country_code);
        const hit = hwEl(svg, 'circle', {cx: x, cy: y, r: 16, fill: 'transparent'});
        // Phrase the trade change from the country's actual position: France's
        // +67.7 is exports falling, not imports appearing.
        const tp = hwData.trade.find(t => t.country_code === r.country_code);
        const d = Number(r.extra_imports_gwh);
        const tradeLine = tp && Number(tp.normal_net_export_gwh) > 0
            ? `Exports ${d >= 0 ? 'fell' : 'rose'} ${hwFmt(Math.abs(d), 1)} GWh/day`
            : `Imports ${d >= 0 ? 'rose' : 'fell'} ${hwFmt(Math.abs(d), 1)} GWh/day`;
        hwTip(hit, `<b>${hwName(r.country_code)}</b><br>${tradeLine}<br>
            Gas ${hwSign(r.extra_gas_gwh, 1)} GWh/day<br>Extra demand ${hwFmt(r.extra_demand_gwh, 1)}`);
    });
    hwEl(svg, 'text', {x: m.l + iw / 2, y: H - 8, class: 'hw-lbl', 'text-anchor': 'middle'},
        'Change in net trade position (GWh/day) — right = imported more or exported less');
    hwEl(svg, 'text', {x: 14, y: m.t + ih / 2, class: 'hw-lbl', 'text-anchor': 'middle',
        transform: `rotate(-90 14 ${m.t + ih / 2})`}, 'Change in gas generation (GWh/day)');

    hwTable('hwGasImpTbl', ['Country', 'Δ net imports GWh/d', 'Δ gas GWh/d', 'Extra demand GWh/d'],
        rows.slice().sort((a, b) => Number(a.extra_gas_gwh) - Number(b.extra_gas_gwh))
            .map(r => [hwName(r.country_code), r.extra_imports_gwh, r.extra_gas_gwh, r.extra_demand_gwh]));
}

// ── Share of the extra demand met by gas ───────────────────────────────────
// Replaces a stacked decomposition of the whole mix. That chart needed a
// visible "unexplained" segment, which is not something to put in front of a
// reader — and reconstructing every term invited error. Both terms here are
// measured directly: gas is one ENTSO-E production type, demand is metered
// load. Nothing is inferred, so nothing is left over.
function hwRenderGasShare() {
    const svg = document.getElementById('hwGasShare');
    if (!svg) return;
    hwClear(svg);
    // A ratio on a tiny denominator is volatile and must not outrank a real one:
    // Czechia's 90% sits on a 2.1 GWh/day demand change against Italy's 88% on
    // 60.8. Countries below the floor are dropped, and the magnitude is printed
    // beside every bar so the reader can weight what remains.
    const MIN_DEMAND_GWH = 3;
    // Also require the demand rise to be material in RELATIVE terms: Germany's
    // demand barely moves in a heatwave (+0.6%), so "X% of the extra demand"
    // is a share of nothing and produced a 509% bar.
    const MIN_UPLIFT_PCT = 2;
    const rows = hwData.sources.slice()
        // != null before Number(): Number(null) is 0, which let Switzerland —
        // a country with no gas fleet — render a "-244%" bar of pure noise.
        .filter(r => Number(r.extra_demand_gwh) >= MIN_DEMAND_GWH
                  && Number(r.uplift_pct) >= MIN_UPLIFT_PCT
                  && r.gas_pct_of_extra_demand != null
                  && Number.isFinite(Number(r.gas_pct_of_extra_demand)))
        .sort((a, b) => (Number(b.gas_pct_of_extra_demand) + 100*Number(b.extra_imports_gwh)/Number(b.extra_demand_gwh))
                      - (Number(a.gas_pct_of_extra_demand) + 100*Number(a.extra_imports_gwh)/Number(a.extra_demand_gwh)))
        .slice(0, 14);
    if (!rows.length) { svg.setAttribute('height', 50); return; }

    const H = rows.length * 24 + 44;
    svg.setAttribute('height', H);
    const W = svg.clientWidth || 800;
    const m = {t: 10, r: 168, b: 30, l: 104};
    const iw = Math.max(80, W - m.l - m.r);
    // Scale has to cover each segment AND their stacked total: Portugal runs
    // -121% gas against +260% imports.
    const vals = rows.flatMap(r => {
        const g = Number(r.gas_pct_of_extra_demand);
        const im = 100 * Number(r.extra_imports_gwh) / Number(r.extra_demand_gwh);
        return [g, im, Math.max(g, 0) + Math.max(im, 0), Math.min(g, 0) + Math.min(im, 0)];
    });
    const lo = Math.min(0, ...vals), hi = Math.max(100, ...vals);
    const X = v => m.l + ((v - lo) / ((hi - lo) || 1)) * iw;
    const bh = 14;

    // 100% reference: gas alone covering the entire demand increase.
    [0, 50, 100].filter(v => v >= lo && v <= hi).forEach(v => {
        hwEl(svg, 'line', {x1: X(v), x2: X(v), y1: m.t - 4, y2: m.t + rows.length * 24 - 8,
            stroke: v === 100 ? '#9a9a93' : '#e1e0d9', 'stroke-width': 1,
            'stroke-dasharray': v === 100 ? '' : ''});
        hwEl(svg, 'text', {x: X(v), y: H - 10, class: 'hw-tick', 'text-anchor': 'middle'}, v + '%');
    });

    // "Net imports 113%" under France — which never imported on a single
    // heatwave day — read as a false statement. The green segment is the
    // TRADE response: extra imports for an importer, an export cut for an
    // exporter. Name it per country from the country's actual position.
    const tradeName = (cc, v) => {
        const tp = hwData.trade.find(t => t.country_code === cc);
        const exporter = tp && Number(tp.normal_net_export_gwh) > 0;
        if (exporter) return v >= 0 ? 'Exports cut' : 'Exports raised';
        return v >= 0 ? 'Extra imports' : 'Imports cut';
    };

    rows.forEach((r, i) => {
        const y = m.t + i * 24;
        const gasPct = Number(r.gas_pct_of_extra_demand);
        const impPct = Math.round(100 * Number(r.extra_imports_gwh) / Number(r.extra_demand_gwh));
        const total = gasPct + impPct;
        const tn = tradeName(r.country_code, impPct);
        // Stack gas and trade from zero, each on its own side. Gas alone made
        // a falling bar look like an unexplained win when the trade response
        // rose in its place, and only the pair shows that.
        let accPos = 0, accNeg = 0;
        [[gasPct, HW_FUEL_COLOR.gas, 'Gas'], [impPct, '#1baf7a', tn]].forEach(([v, col, nm]) => {
            if (!v) return;
            const from = v > 0 ? accPos : accNeg + v;
            const w = Math.max(2, Math.abs(X(v) - X(0)) - 1);
            const rect = hwEl(svg, 'rect', {x: X(from), y, width: w, height: bh, rx: 3, fill: col});
            hwTip(rect, `<b>${hwName(r.country_code)} · ${nm}</b><br>${hwSign(v, 0)}% of the demand increase`);
            if (v > 0) accPos += v; else accNeg += v;
        });
        hwEl(svg, 'text', {x: m.l - 8, y: y + bh - 2, class: 'hw-lbl', 'text-anchor': 'end'},
            hwName(r.country_code));
        const endX = X(Math.max(accPos, 0)) + 8;
        hwEl(svg, 'text', {x: endX, y: y + bh - 2, class: 'hw-val'}, total + '%');
        // Magnitude beside the ratio: 92% of 60.8 GWh/day is a different fact
        // from 117% of 4.7, and the bar length alone cannot say which is which.
        hwEl(svg, 'text', {x: endX + 44, y: y + bh - 2, class: 'hw-tick'},
            `on ${hwFmt(r.extra_demand_gwh, 1)} GWh/day`);
        const hit = hwEl(svg, 'rect', {x: m.l, y: y - 3, width: iw, height: bh + 6, fill: 'transparent'});
        hwTip(hit, `<b>${hwName(r.country_code)}</b><br>
            Extra demand ${hwFmt(r.extra_demand_gwh, 1)} GWh/day<br>
            Gas ${hwSign(gasPct, 0)}% · ${tn} ${hwSign(impPct, 0)}%<br>
            Together ${hwSign(total, 0)}% of the increase`);
    });
    hwEl(svg, 'text', {x: m.l + iw / 2, y: H - 26, class: 'hw-lbl', 'text-anchor': 'middle'},
        'Share of the extra demand met by gas and by the trade response');

    hwLegend('hwGasShareLegend', [
        {c: HW_FUEL_COLOR.gas, t: 'Gas'},
        {c: '#1baf7a', t: 'Trade — extra imports, or exports cut'},
    ]);
    hwTable('hwGasShareTbl',
        ['Country', 'Extra demand GWh/d', 'Gas %', 'Trade %', 'Trade means', 'Together %'],
        rows.map(r => {
            const g = Number(r.gas_pct_of_extra_demand);
            const im = Math.round(100 * Number(r.extra_imports_gwh) / Number(r.extra_demand_gwh));
            return [hwName(r.country_code), r.extra_demand_gwh, g + '%', im + '%',
                tradeName(r.country_code, im).toLowerCase(), (g + im) + '%'];
        }));
}

// ── Which countries' demand rises most ─────────────────────────────────────
function hwRenderUplift() {
    const rows = hwData.uplift.slice()
        .filter(r => Number.isFinite(Number(r.mean_demand_uplift_pct))
                  && Number(r.normal_mean_mw) > 0)
        .sort((a, b) => Number(b.mean_demand_uplift_pct) - Number(a.mean_demand_uplift_pct));

    // Indexed to each country's normal-day mean (=100): Cyprus runs 0.9 GW and
    // France 45 GW, so raw MW on one axis would flatten every small country to
    // a sliver. Indexed, the two bands and the gap between the dots read the
    // same way for every row, and the ranges stay honest.
    hwRangeStrip('hwUplift', rows.map(r => {
        const f = 100 / Number(r.normal_mean_mw);
        const weak = hwBaselineNote(r.country_code);
        return {
            label: hwName(r.country_code) + (weak ? ' †' : ''),
            n: [Number(r.normal_min_mw) * f, 100, Number(r.normal_max_mw) * f],
            h: [Number(r.heatwave_min_mw) * f, Number(r.heatwave_mean_mw) * f,
                Number(r.heatwave_max_mw) * f],
            vlabel: hwSign(r.mean_demand_uplift_pct, 1) + '%',
            tip: `<b>${hwName(r.country_code)}</b><br>Mean demand ${hwSign(r.mean_demand_uplift_pct, 1)}%<br>
                Normal: min ${hwFmt(r.normal_min_mw)} · mean ${hwFmt(r.normal_mean_mw)} ·
                max ${hwFmt(r.normal_max_mw)} MW (${r.normal_days} days)<br>
                Heatwave: min ${hwFmt(r.heatwave_min_mw)} · mean ${hwFmt(r.heatwave_mean_mw)} ·
                max ${hwFmt(r.heatwave_max_mw)} MW (${r.heatwave_days} days)<br>
                Peak demand ${hwSign(r.peak_demand_uplift_pct, 1)}%
                ${weak ? '<br><br>† ' + weak : ''}`,
        };
    }), {axis: 'Daily demand, indexed — each country’s normal-day mean = 100',
         fmt: v => Math.round(v)});
    hwLegend('hwUpliftLegend', HW_RANGE_LEGEND);

    const flagged = rows.filter(r => hwBaselineNote(r.country_code))
        .map(r => hwName(r.country_code));
    const foot = document.getElementById('hwUpliftFoot');
    if (foot) {
        foot.textContent = flagged.length
            ? `† ${flagged.join(', ')}: nearly all reference days fall before July — after that, `
              + `every day was a heatwave. The heat contrast is normal, but non-heat seasonal `
              + `effects (holidays, shutdowns, tourism) are uncontrolled for these countries.`
            : '';
    }

    // Min / mean / max of both pools, so every percentage can be checked
    // against the raw levels it came from.
    hwTable('hwUpliftTbl',
        ['Country', 'Mean %', 'Peak %',
         'Normal MW min', 'Normal MW mean', 'Normal MW max',
         'Heatwave MW min', 'Heatwave MW mean', 'Heatwave MW max',
         'Normal days', 'Heatwave days'],
        rows.map(r => [hwName(r.country_code), r.mean_demand_uplift_pct, r.peak_demand_uplift_pct,
            hwFmt(r.normal_min_mw), hwFmt(r.normal_mean_mw), hwFmt(r.normal_max_mw),
            hwFmt(r.heatwave_min_mw), hwFmt(r.heatwave_mean_mw), hwFmt(r.heatwave_max_mw),
            r.normal_days, r.heatwave_days]));
}

// ── How the gap was covered, fuel by fuel — ONE country ────────────────────
// Was a multi-country stack, but only six countries' terms closed tightly
// enough to draw, which made it look arbitrary and left the rows too thin to
// read. One country per view fits the country selector, gives every component
// its own labelled row, and lets a country with a loose closure still be shown
// with that closure stated rather than silently dropped.
function hwRenderCoverage(sel) {
    const svg = document.getElementById('hwCover');
    const title = document.getElementById('hwCoverTitle');
    const note = document.getElementById('hwCoverClosure');
    if (!svg) return;
    hwClear(svg);

    const parts = hwData.coverage.filter(r => r.country_code === sel);
    if (!parts.length) {
        svg.setAttribute('height', 50);
        hwEl(svg, 'text', {x: 8, y: 28, class: 'hw-lbl'},
            `No matched heatwave days for ${hwName(sel)}.`);
        if (title) title.textContent = 'How the gap was covered, fuel by fuel';
        if (note) note.textContent = '';
        hwLegend('hwCoverLegend', []);
        hwTable('hwCoverTbl', ['Component', 'Δ GWh/day'], []);
        return;
    }

    const demand = Number(parts[0].extra_demand_gwh);
    const gapPct = Number(parts[0].gap_pct);
    if (title) {
        title.textContent = `How the gap was covered — ${hwName(sel)}, demand ${hwSign(demand, 1)} GWh/day`;
    }
    if (note) {
        // Report the gap itself. "accounts for 0%" was the old wording whenever
        // the gap exceeded 100%, which read as "nothing is explained" when the
        // real statement is that the components miss the target by more than
        // the target itself.
        const sum = parts.reduce((s, p) => s + Number(p.delta_gwh || 0), 0);
        note.textContent = gapPct <= 35
            ? `The components sum to ${hwFmt(sum, 1)} GWh/day against a demand change of `
              + `${hwFmt(demand, 1)} — they account for it to within ${gapPct}%.`
            : `Treat with caution: the components sum to ${hwFmt(sum, 1)} GWh/day against a demand `
              + `change of ${hwFmt(demand, 1)}, a discrepancy of ${gapPct}%. The difference is `
              + `pumped-storage consumption (counted as generation but never netted off), transmission `
              + `losses, and plant that reports on some days but not others.`;
        const bn = hwBaselineNote(sel);
        if (bn) note.textContent += ' ' + bn;
    }

    const COMP_COLOR = Object.assign({}, HW_FUEL_COLOR, {imports: '#1baf7a'});
    const rows = parts
        .map(p => ({c: p.component, v: Number(p.delta_gwh)}))
        .filter(p => Number.isFinite(p.v) && Math.abs(p.v) > 0.05)
        .sort((a, b) => b.v - a.v);

    const H = rows.length * 26 + 46;
    svg.setAttribute('height', H);
    const W = svg.clientWidth || 800;
    const m = {t: 10, r: 74, b: 28, l: 104};
    const iw = Math.max(80, W - m.l - m.r);
    const span = Math.max(...rows.map(r => Math.abs(r.v)), Math.abs(demand)) * 1.12 || 1;
    const X = v => m.l + iw / 2 + (v / span) * (iw / 2);
    const bh = 16;

    hwEl(svg, 'line', {x1: X(0), x2: X(0), y1: m.t - 4, y2: m.t + rows.length * 26 - 6,
        stroke: '#c3c2b7', 'stroke-width': 1});
    // The demand increase to be met, spanning the whole plot.
    hwEl(svg, 'line', {x1: X(demand), x2: X(demand), y1: m.t - 4, y2: m.t + rows.length * 26 - 6,
        stroke: '#0b0b0b', 'stroke-width': 2});

    // The trade bar is a CHANGE in net imports. Labelling it "Imports" made
    // France — which exports right through a heatwave — look like it suddenly
    // started importing, when what happened is that its exports fell. Name the
    // bar after the country's actual position and put the levels in the tooltip.
    const tp = hwData.trade.find(t => t.country_code === sel);
    const isExporter = tp && Number(tp.normal_net_export_gwh) > 0;
    const tradeLabel = isExporter ? 'Net exports (fall)' : 'Net imports (rise)';

    rows.forEach((r, i) => {
        const y = m.t + i * 26;
        const neg = r.v < 0;
        const x = neg ? X(r.v) : X(0);
        const w = Math.max(2, Math.abs(X(r.v) - X(0)));
        const isTrade = r.c === 'imports';
        hwEl(svg, 'rect', {x, y, width: w, height: bh, rx: 3, fill: COMP_COLOR[r.c] || '#8a8f98'});
        hwEl(svg, 'text', {x: m.l - 8, y: y + bh - 3, class: 'hw-lbl', 'text-anchor': 'end'},
            isTrade ? tradeLabel : hwCap(r.c));
        hwEl(svg, 'text', {x: neg ? x - 7 : x + w + 7, y: y + bh - 3, class: 'hw-val',
            'text-anchor': neg ? 'end' : 'start'}, hwSign(r.v, 1));
        const hit = hwEl(svg, 'rect', {x: m.l, y: y - 3, width: iw, height: bh + 6, fill: 'transparent'});
        const levels = isTrade && tp
            ? `<br>${isExporter ? 'Exports' : 'Imports'} ${hwFmt(Math.abs(Number(
                  isExporter ? tp.normal_net_export_gwh : tp.normal_net_import_gwh)), 1)} → `
              + `${hwFmt(Math.abs(Number(
                  isExporter ? tp.heatwave_net_export_gwh : tp.heatwave_net_import_gwh)), 1)} GWh/day`
            : '';
        hwTip(hit, `<b>${isTrade ? tradeLabel : hwCap(r.c)}</b><br>
            ${hwSign(r.v, 1)} GWh/day on heatwave days${levels}<br>
            Demand change ${hwSign(demand, 1)} GWh/day`);
    });
    hwEl(svg, 'text', {x: m.l + iw / 2, y: H - 6, class: 'hw-lbl', 'text-anchor': 'middle'},
        '← fell, had to be replaced    |    rose to cover the gap → (GWh/day)');

    hwLegend('hwCoverLegend', [{c: '#0b0b0b', t: `Demand increase to be met (${hwSign(demand, 1)} GWh/day)`}]);
    hwTable('hwCoverTbl', ['Component', 'Δ GWh/day', 'Share of demand change'],
        rows.map(r => [hwCap(r.c), hwFmt(r.v, 1),
            demand ? Math.round(100 * r.v / demand) + '%' : '—']));
}

function hwRenderKpis() {
    const k = hwData.kpi;
    const cells = [
        ['🗓️', hwFmt(k.calendar_days), 'Days with a heatwave in Europe (2026)'],
        ['🌍', hwFmt(k.countries) + ' / 30', 'Countries affected'],
        ['🔥', hwFmt(k.max_simultaneous), 'Most countries hot at once'],
        ['🌡️', hwFmt(k.peak_tmax, 1) + '°C', 'Highest temperature'],
    ];
    document.getElementById('hwKpis').innerHTML = cells.map(([icon, v, l]) => `
        <div class="stat-card">
            <div class="stat-icon">${icon}</div>
            <div class="stat-content">
                <div class="stat-value">${escapeHtml(v)}</div>
                <div class="stat-label">${escapeHtml(l)}</div>
            </div>
        </div>`).join('');
}

async function hwFetchAll() {
    const sb = supabase;
    // The two big ones must be paged: PostgREST caps a response at 1000 rows,
    // and these run to ~7k and ~15k. Unpaged they silently truncate, which
    // showed up as a response curve with two countries in it instead of thirty.
    const [eu, fuels, renewable, price, gas, helpers, weatherRows, loadRows, burden, trade, sources, uplift, coverage, quality, mixTemp, solarPrice, eventProfile, impact, events, eventSeries] = await Promise.all([
        sb.from('v_eu_heatwave_response').select('*'),
        // Ten-day floor: Sweden and Ireland had 3 heatwave days in 2026, Latvia 4,
        // Lithuania 6, Denmark 8. A percentage built on three days is noise, so
        // those countries are excluded rather than drawn as if they were findings.
        sb.from('v_heatwave_fuel_resilience').select('*').gte('heatwave_days', HW_MIN_DAYS),
        sb.from('v_heatwave_renewable').select('*').gte('heatwave_days', HW_MIN_DAYS),
        sb.from('v_heatwave_price').select('*').gte('heatwave_days', HW_MIN_DAYS),
        sb.from('v_heatwave_gas_sector').select('*').gte('heatwave_days', HW_MIN_DAYS),
        sb.from('v_heatwave_helpers').select('*'),
        gasFetchAllPaged(() => sb.from('weather_country_daily')
            .select('country_code, date, tmax_c, heatwave_id, heatwave_length')
            .gte('date', '2026-01-01').order('date', {ascending: true}), 1000, 40000),
        sb.from('v_heatwave_response_curve').select('*'),
        sb.from('v_heatwave_burden').select('*'),
        // One floor everywhere (HW_MIN_DAYS): the old mix of 10/15/20 made
        // Poland (16 days) and Portugal (18) vanish from some charts while
        // appearing in others, which read as an error rather than a rule.
        sb.from('v_heatwave_trade_position').select('*').gte('heatwave_days', HW_MIN_DAYS),
        sb.from('v_heatwave_demand_sources').select('*').gte('heatwave_days', HW_MIN_DAYS),
        sb.from('v_heatwave_demand_uplift').select('*').gte('heatwave_days', HW_MIN_DAYS),
        sb.from('v_heatwave_gap_coverage').select('*').gte('heatwave_days', HW_MIN_DAYS),
        sb.from('v_heatwave_baseline_quality').select('*'),
        sb.from('v_eu_mix_by_temp').select('*').order('bin_c', {ascending: true}),
        sb.from('v_solar_price_intraday').select('*').eq('scope', 'heatwave')
            .order('hour', {ascending: true}),
        sb.from('v_hw_event_profile').select('*').order('hour', {ascending: true}),
        sb.from('mv_heatwave_component_delta')
            .select('country_code, component, delta_gwh, delta_mw')
            .gte('heatwave_days', HW_MIN_DAYS),
        sb.from('v_heatwave_event_top').select('*'),
        gasFetchAllPaged(() => sb.from('v_heatwave_event_series').select('*')
            .order('date', {ascending: true}), 1000, 20000),
    ]);
    const err = [eu, fuels, renewable, price, gas, helpers, loadRows, burden, trade, sources, uplift, coverage, events].find(r => r && r.error);
    if (err) throw new Error(err.error.message);

    // KPIs, from the weather rows already fetched.
    const w = weatherRows || [];
    const hwRows = w.filter(r => r.heatwave_id);
    const byDate = {};
    hwRows.forEach(r => { byDate[r.date] = (byDate[r.date] || 0) + 1; });
    const kpi = {
        calendar_days: Object.keys(byDate).length,
        country_days: hwRows.length,
        countries: new Set(hwRows.map(r => r.country_code)).size,
        max_simultaneous: Math.max(0, ...Object.values(byDate)),
        peak_tmax: Math.max(...w.map(r => Number(r.tmax_c) || 0)),
    };

    // Fuel resilience is per country; roll it up for the EU-wide chart.
    // Weight by fleet size — averaging percentages lets a 200 MW fleet count as
    // much as a 60 GW one, which put EU coal at +16% when the actual EU-wide
    // change was +4.5%. Sum the MW, then take the ratio.
    const fg = {};
    (fuels.data || []).forEach(r => {
        const pct = Number(r.output_change_pct);
        const nrm = Number(r.mean_mw_normal), hot = Number(r.mean_mw_heatwave);
        if (!Number.isFinite(pct) || !Number.isFinite(nrm) || !Number.isFinite(hot)) return;
        const g = (fg[r.fuel] ||= {pcts: [], normal: 0, heatwave: 0});
        g.pcts.push(pct); g.normal += nrm; g.heatwave += hot;
    });
    const fuelRows = Object.entries(fg).map(([fuel, g]) => ({
        fuel,
        avg: g.normal > 0 ? 100 * (g.heatwave / g.normal - 1) : 0,
        normal_mw: g.normal, heatwave_mw: g.heatwave,
        worst: Math.min(...g.pcts), best: Math.max(...g.pcts), countries: g.pcts.length,
    }));

    const response = (loadRows.data || []).slice()
        .sort((a, b) => a.country_code.localeCompare(b.country_code) || a.tmax_bin - b.tmax_bin);

    return {
        kpi, eu: eu.data || [], fuels: fuelRows, renewable: renewable.data || [],
        price: price.data || [], gas: gas.data || [], helpers: helpers.data || [],
        response,
        burden: burden.data || [],
        trade: trade.data || [],
        sources: sources.data || [],
        uplift: uplift.data || [],
        coverage: coverage.data || [],
        // Keyed by country so any chart can qualify its own figure.
        quality: Object.fromEntries((quality.data || []).map(r => [r.country_code, r])),
        mixTemp: mixTemp.data || [],
        solarPrice: solarPrice.data || [],
        eventProfile: eventProfile.data || [],
        impact: impact.data || [],
        events: events.data || [],
        eventSeries: eventSeries || [],
    };
}

// Cyprus, Italy and Spain have 100% of their reference days before July, and
// Greece 90% — after that, every day was a heatwave. Their temperature contrast
// is normal (the reference pool is the warm end of what exists), but seasonal
// factors other than heat — holidays, industrial shutdown, tourism — are not
// controlled for them, in either direction.
function hwBaselineNote(cc) {
    const q = hwData.quality?.[cc];
    if (!q) return '';
    const early = Number(q.pct_ref_before_jul);
    if (!(early >= 85)) return '';
    return `${early}% of ${hwName(cc)}'s reference days fall before July — after that, `
         + `every day was a heatwave. Heat contrast vs the reference is `
         + `${hwFmt(q.temp_gap_c, 1)} °C (normal), but non-heat seasonal effects `
         + `(holidays, shutdowns, tourism) are uncontrolled.`;
}

function hwRenderScoped() {
    const sel = document.getElementById('hwCountry')?.value;
    if (!sel) return;
    hwRenderResponse(sel);
    hwRenderCoverage(sel);
    hwRenderEvent(sel);
}

function hwRenderAll() {
    if (!hwData) return;
    hwRenderKpis();
    hwRenderEu();
    hwRenderMixTemp();
    hwRenderSolarPrice();
    hwRenderEventProfile();
    // Async: it waits on the shared GeoJSON fetch. Failure degrades to the
    // data tables rather than taking the rest of the page down with it.
    hwRenderImpactMaps().catch(() => {});
    hwRenderFuels();
    hwRenderRenewable();
    hwRenderPrice();
    hwRenderGas();
    hwRenderBurden();
    hwRenderTradeDumbbell('hwImp', 'hwImpTbl', 'hwImpLegend', 'import');
    hwRenderTradeDumbbell('hwExp', 'hwExpTbl', 'hwExpLegend', 'export');
    hwRenderUplift();
    hwRenderGasShare();
    hwRenderGasImports();
    hwRenderScoped();
}

async function loadHeatwavesPage() {
    const token = ++hwLoadToken;
    const fresh = document.getElementById('hwFreshness');

    if (!hwWired) {
        document.querySelectorAll('[data-hwt]').forEach(btn => {
            btn.addEventListener('click', () => {
                const t = document.getElementById(btn.getAttribute('data-hwt'));
                if (!t) return;
                const hidden = t.classList.toggle('hw-hidden');
                btn.textContent = hidden ? 'Show data table' : 'Hide data table';
            });
        });
        document.getElementById('hwCountry')?.addEventListener('change', hwRenderScoped);
        let rt;
        window.addEventListener('resize', () => {
            if (!document.getElementById('heatwavesPage')?.classList.contains('active')) return;
            clearTimeout(rt);
            rt = setTimeout(hwRenderAll, 180);
        });
        hwWired = true;
    }

    if (hwData) { hwRenderAll(); return; }

    if (fresh) fresh.textContent = 'Loading…';
    try {
        const data = await hwFetchAll();
        if (token !== hwLoadToken) return;
        hwData = data;

        const sel = document.getElementById('hwCountry');
        if (sel) {
            const ccs = [...new Set(hwData.response.map(r => r.country_code))]
                .sort((a, b) => hwName(a).localeCompare(hwName(b)));
            sel.innerHTML = ccs.map(c =>
                `<option value="${escapeHtml(c)}">${escapeHtml(cbCountryFlag(c) + ' ' + hwName(c))}</option>`
            ).join('');
            sel.value = ccs.includes('FR') ? 'FR' : ccs[0];
        }
        if (fresh) fresh.textContent = '';
        hwRenderAll();
    } catch (e) {
        console.error('Heatwaves page failed:', e);
        if (fresh) fresh.textContent = 'Could not load heatwave data: ' + e.message;
    }
}

})(); // End IIFE
