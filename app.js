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
            await loadDashboard();
        }
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

// Setup navigation handlers
function setupNavigation() {
    // Sidebar toggle
    document.getElementById('sidebarToggle')?.addEventListener('click', () => {
        document.getElementById('sidebar').classList.toggle('active');
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
function navigateToPage(page, countryId = null) {
    if (page !== 'energy-meter') {
        teardownEnergyRealtimeSubscription();
    }
    track('page_view', { page: countryId ? `country/${countryId}` : page });
    // Hide all pages
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));

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
        return;
    }

    saveLastPageState(page, countryId);
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
function fetchEntsoeZonesGeoJsonOnce() {
    if (__energyEntsoeZonesGeoJsonPromise) return __energyEntsoeZonesGeoJsonPromise;
    // Electricity Maps zone geometry (includes bidding zones like NO1..NO5, SE1..SE4, DK1..DK2).
    const url = 'https://raw.githubusercontent.com/electricitymaps/electricitymaps-contrib/master/geo/world.geojson';
    __energyEntsoeZonesGeoJsonPromise = fetch(url).then(r => {
        if (!r.ok) throw new Error(`Zone GeoJSON HTTP ${r.status}`);
        return r.json();
    });
    return __energyEntsoeZonesGeoJsonPromise;
}

let __energyEuropeCountriesGeoJsonPromise = null;
function fetchEuropeCountriesGeoJsonOnce() {
    if (__energyEuropeCountriesGeoJsonPromise) return __energyEuropeCountriesGeoJsonPromise;
    const url = 'https://raw.githubusercontent.com/leakyMirror/map-of-europe/master/GeoJSON/europe.geojson';
    __energyEuropeCountriesGeoJsonPromise = fetch(url).then(r => {
        if (!r.ok) throw new Error(`GeoJSON HTTP ${r.status}`);
        return r.json();
    });
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
        fetchEntsoeZonesGeoJsonOnce(),
    ]);

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
        if (iso2 === 'DK' || iso2 === 'SE' || iso2 === 'NO') continue;
        // GB rendered via zone overlay below (same as DK/SE/NO); skip base layer to avoid double-draw
        if (iso2 === 'GB' || iso2 === 'UK') continue;

        const dataKey = iso2GeoToDataKey(iso2);
        const val = byCountry[dataKey]?.pct;
        const fill = Number.isFinite(val) ? mixColorRedToGreen(val) : 'rgba(148,163,184,0.18)';

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
        const fill = Number.isFinite(val) ? mixColorRedToGreen(val) : 'rgba(148,163,184,0.18)';

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
let elecEuChartLoadInFlight = null;

let elecZoneChart = null;
let elecZoneRange = 'day';
let elecZoneChartLoadInFlight = null;

let elecTabInited = false;
let elecLatestRows = [];            // latest rows per zone with totalMw extracted
let elecSelectedZone = null;        // separate from renewable selection so the tabs don't fight
let elecSelectedSource = null;

// Demand (load) tab
let loadEuChart = null;
let loadEuRange = '1y';
let loadEuChartLoadInFlight = null;

let loadZoneChart = null;
let loadZoneRange = 'day';
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

// Chart builder tab (multi-series, per-series metric)
let chartBuilderTabInited = false;
let chartBuilderChart = null;
let cbComposerMetric = 'renewable'; // metric applied to next country click
let cbComposerGenFilter = null;     // psr_type group key when generation is active (null = total)
let cbRange = 'day'; // 'day' | 'week' | 'month' | '6m' | '1y' | '5y'
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
    } else if (target === 'chart-builder') {
        document.getElementById('chartBuilderEmTab')?.classList.add('active');
        if (!chartBuilderTabInited) {
            chartBuilderTabInited = true;
            initChartBuilderControls();
        }
        loadChartBuilder();
    } else {
        document.getElementById('renewableEmTab')?.classList.add('active');
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

    const refreshBtn = document.getElementById('elecRefreshBtn');
    if (refreshBtn && !refreshBtn.dataset.bound) {
        refreshBtn.dataset.bound = '1';
        refreshBtn.addEventListener('click', () => loadElectricityTabData(true));
    }
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
        fetchEntsoeZonesGeoJsonOnce(),
    ]);

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
        if (iso2 === 'DK' || iso2 === 'SE' || iso2 === 'NO') continue;

        const dataKey = iso2GeoToDataKey(iso2);
        const mw = byCountry[dataKey]?.mw;
        const t = Number.isFinite(mw) && countryMax > 0 ? Math.max(0, Math.min(1, mw / countryMax)) : null;
        const fill = t == null ? 'rgba(148,163,184,0.18)' : gasBlueScale(t);

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
        const fill = t == null ? 'rgba(148,163,184,0.18)' : gasBlueScale(t);

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
            const since = euRangeToSinceIso(range);
            const useRaw = range === 'day' || range === 'week';
            const useWeekly = range === '5y';
            const fmtVal = useRaw ? fmtMwShort : fmtGWh;
            const unit = useRaw ? 'MW' : 'GWh';

            setStatus(`Loading EU demand (${range})…`);
            if (titleEl) titleEl.textContent = `EU — Total electricity demand (${unit})`;

            let points;
            if (useRaw) {
                // Online behaviour: sum per-zone snapshots by timestamp (15m-ish) for day/week.
                const rawRows = await gasFetchAllPaged(() =>
                    supabase
                        .from('electricity_load_snapshots')
                        .select('ts, load_mw')
                        .eq('source', 'entsoe')
                        .gte('ts', since)
                        .order('ts', { ascending: true })
                );
                const byTs = new Map();
                for (const r of rawRows) {
                    if (r.ts && Number.isFinite(Number(r.load_mw)) && Number(r.load_mw) > 0)
                        byTs.set(r.ts, (byTs.get(r.ts) || 0) + Number(r.load_mw));
                }
                points = [...byTs.entries()]
                    .sort((a, b) => a[0] < b[0] ? -1 : 1)
                    .map(([ts, y]) => ({ ts, y }));
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
    const useWeekly = range === '5y';
    const useDaily = range === 'month' || range === '6m' || range === '1y';
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
    const useWeekly = range === '5y';
    const useDaily = range === 'month' || range === '6m' || range === '1y';
    const raw = !(useWeekly || useDaily); // day, week only
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
            const byTs = new Map();
            for (const p of points) byTs.set(p.ts, (byTs.get(p.ts) || 0) + Number(p.y));
            euSeries = [...byTs.entries()].map(([ts, y]) => ({ ts, y })).sort((a, b) => new Date(a.ts) - new Date(b.ts));
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
    const useWeekly = range === '5y';
    const useDaily = range === 'month' || range === '6m' || range === '1y';
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
    const useWeekly = range === '5y';
    const raw = range === 'day' || range === 'week'; // day + week use raw snapshots for intraday
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
            const byTs = new Map();
            for (const p of points) byTs.set(p.ts, (byTs.get(p.ts) || 0) + Number(p.y));
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
            // Sum psr_type into EU total.
            const byTs = new Map();
            for (const r of euRows) {
                if (!r.ts || !Number.isFinite(Number(r.production_mwh))) continue;
                byTs.set(r.ts, (byTs.get(r.ts) || 0) + Number(r.production_mwh));
            }
            euSeries = [...byTs.entries()].map(([ts, y]) => ({ ts, y })).sort((a, b) => new Date(a.ts) - new Date(b.ts));
        }
    }
    const typeLabel = psrFilter ? (ELEC_TYPE_GROUPS.find(g => g.key === psrFilter)?.label ?? psrFilter) : 'total';
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
        const fill = t == null ? 'rgba(148,163,184,0.18)' : `rgba(${Math.round(34 + t * (239-34))},${Math.round(197 + t * (68-197))},${Math.round(94 + t * (68-94))},${0.18 + t * 0.55})`;

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
        fetchEntsoeZonesGeoJsonOnce(),
    ]);

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
        if (iso2 === 'DK' || iso2 === 'SE' || iso2 === 'NO') continue;
        // GB rendered via zone overlay below; skip base layer to avoid double-draw
        if (iso2 === 'GB' || iso2 === 'UK') continue;

        const dataKey = iso2GeoToDataKey(iso2);
        const mw = byCountry[dataKey]?.mw;
        const t = Number.isFinite(mw) && countryMax > 0 ? Math.max(0, Math.min(1, mw / countryMax)) : null;
        const fill = t == null ? 'rgba(148,163,184,0.18)' : gasBlueScale(t);

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
        const fill = t == null ? 'rgba(148,163,184,0.18)' : gasBlueScale(t);

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
let gasCountryRange = '1y';
let gasSelectedCountry = null;
let gasEuChart = null;
let gasCountryChart = null;
let gasDemandCbChart = null;
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
    const demandBtn = document.getElementById('gasDemandTabBtn');
    const overviewPane = document.getElementById('gasOverviewEmTab');
    const demandPaneEl = document.getElementById('gasDemandEmTab');
    if (!alreadyBound && overviewBtn && demandBtn && overviewPane && demandPaneEl) {
        const setActive = (which) => {
            const isDemand = which === 'demand';
            overviewBtn.classList.toggle('active', !isDemand);
            demandBtn.classList.toggle('active', isDemand);
            overviewBtn.setAttribute('aria-selected', String(!isDemand));
            demandBtn.setAttribute('aria-selected', String(isDemand));
            overviewPane.classList.toggle('active', !isDemand);
            demandPaneEl.classList.toggle('active', isDemand);

            if (isDemand) {
                // Build once when the user first lands on the tab.
                if (!gasDemandCbHasBuilt) loadGasDemandChartBuilderChart();
            }
        };

        overviewBtn.addEventListener('click', () => setActive('overview'));
        demandBtn.addEventListener('click', () => setActive('demand'));
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
}

async function loadGasDemandChartBuilderChart() {
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
}

async function loadGasEuAggregateChart(range) {
    const statusEl = document.getElementById('gasEuStatus');
    const canvas = document.getElementById('gasEuChart');
    const titleEl = document.getElementById('gasEuChartTitle');
    if (!canvas || !supabase) return;
    const setStatus = (m) => { if (statusEl) statusEl.textContent = m || ''; };

    try {
        const cachedReady = gasCacheFresh(gasEuAllRows);
        setStatus(cachedReady ? `Rendering EU27 (${range})…` : `Loading EU27 aggregate (${range})…`);
        const fromDate = gasRangeStartISO(range);
        const all = await gasFetchEuAll();
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

        if (titleEl) titleEl.textContent = `EU27 — Gas demand by sector (GWh/day) · ${days[0] || ''} → ${days.at(-1) || ''}`;

        if (gasEuChart) { try { gasEuChart.destroy(); } catch (_) {} }
        gasEuChart = new Chart(canvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: days,
                datasets: [
                    { label: 'Power', data: power, backgroundColor: GAS_SECTOR_COLORS.power + 'cc', borderColor: GAS_SECTOR_COLORS.power, fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false },
                    { label: 'Household', data: household, backgroundColor: GAS_SECTOR_COLORS.household + 'cc', borderColor: GAS_SECTOR_COLORS.household, fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false },
                    { label: 'Industry', data: industry, backgroundColor: GAS_SECTOR_COLORS.industry + 'cc', borderColor: GAS_SECTOR_COLORS.industry, fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false },
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
                                const idx = ctx.dataIndex;
                                const total = (ctx.chart?.data?.datasets || []).reduce(
                                    (s, ds) => s + (Number(ds.data?.[idx]) || 0), 0);
                                const val = Number(ctx.raw);
                                const pct = total > 0 ? (val / total * 100) : null;
                                return pct != null
                                    ? `${ctx.dataset.label}: ${val.toFixed(0)} GWh (${pct.toFixed(1)}%)`
                                    : `${ctx.dataset.label}: ${val.toFixed(0)} GWh`;
                            },
                            footer: (items) => {
                                const vals = items.filter(i => i.raw != null).map(i => Number(i.raw));
                                if (!vals.length) return 'No data for this day';
                                return `Total: ${vals.reduce((s, v) => s + v, 0).toFixed(0)} GWh`;
                            },
                        },
                    },
                },
                scales: {
                    x: { ticks: { maxTicksLimit: 10 } },
                    y: { stacked: true, title: { display: true, text: 'GWh / day' }, beginAtZero: true },
                },
            },
        });

        setStatus(`EU27: ${days.length} days`);
    } catch (err) {
        console.error('EU gas aggregate failed:', err);
        setStatus(`Failed: ${err.message || err}`);
    }
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
        const all = await gasFetchCountryAll(country);
        const rows = all.filter(r => String(r.gas_day).slice(0, 10) >= fromDate);
        const toGwh = (v) => (v == null ? null : Number(v) / 1000);
        const days = rows.map(r => String(r.gas_day).slice(0, 10));
        const power = rows.map(r => toGwh(r.power_mwh));
        const household = rows.map(r => toGwh(r.household_mwh));
        const industry = rows.map(r => toGwh(r.industry_mwh));

        if (titleEl) titleEl.textContent = `${country} — Gas demand by sector (GWh/day) · ${days[0] || ''} → ${days.at(-1) || ''}`;

        if (gasCountryChart) { try { gasCountryChart.destroy(); } catch (_) {} }
        gasCountryChart = new Chart(canvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: days,
                datasets: [
                    { label: 'Power', data: power, backgroundColor: GAS_SECTOR_COLORS.power + 'cc', borderColor: GAS_SECTOR_COLORS.power, fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false },
                    { label: 'Household', data: household, backgroundColor: GAS_SECTOR_COLORS.household + 'cc', borderColor: GAS_SECTOR_COLORS.household, fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false },
                    { label: 'Industry', data: industry, backgroundColor: GAS_SECTOR_COLORS.industry + 'cc', borderColor: GAS_SECTOR_COLORS.industry, fill: true, pointRadius: 0, tension: 0.25, borderWidth: 1, stack: 'sec', spanGaps: false },
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
                                const idx = ctx.dataIndex;
                                const total = (ctx.chart?.data?.datasets || []).reduce(
                                    (s, ds) => s + (Number(ds.data?.[idx]) || 0), 0);
                                const val = Number(ctx.raw);
                                const pct = total > 0 ? (val / total * 100) : null;
                                return pct != null
                                    ? `${ctx.dataset.label}: ${val.toFixed(1)} GWh (${pct.toFixed(1)}%)`
                                    : `${ctx.dataset.label}: ${val.toFixed(1)} GWh`;
                            },
                            footer: (items) => {
                                const vals = items.filter(i => i.raw != null).map(i => Number(i.raw));
                                if (!vals.length) return 'No data for this day';
                                return `Total: ${vals.reduce((s, v) => s + v, 0).toFixed(1)} GWh`;
                            },
                        },
                    },
                },
                scales: {
                    x: { ticks: { maxTicksLimit: 10 } },
                    y: { stacked: true, title: { display: true, text: 'GWh / day' }, beginAtZero: true },
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

})(); // End IIFE
