"""Build a standalone, hard-coded interactive map of the summer 2026 heatwave.

The page is meant to be dropped into another site, so it carries everything it
needs: the per-country figures, the country outlines, the styling and the
behaviour all end up inside one HTML file with no network calls at runtime.

    python python/make_heatwave_map_html.py

Encoding follows the same rule as the printed infographics: the map fill is a
sequential ramp because demand uplift is a magnitude, and the country card uses
a diverging pair because "what rose and what fell" is a polarity. Fuel identity
is carried by the row label, never by a hue - nine fuels cannot be given nine
distinguishable colours, and pretending otherwise is how a chart becomes
unreadable for a colourblind reader.
"""
import json
import math
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "heatwave-map.html"

env = {}
for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
KEY, URL = env["SUPABASE_ANON_KEY"], env["SUPABASE_URL"]


def get(q):
    req = urllib.request.Request(f"{URL}/rest/v1/{q}",
                                 headers={"apikey": KEY, "Authorization": f"Bearer {KEY}"})
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read())


NAMES = {"AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CH": "Switzerland",
         "CY": "Cyprus", "CZ": "Czechia", "DE": "Germany", "DK": "Denmark",
         "ES": "Spain", "FR": "France", "GR": "Greece", "HR": "Croatia",
         "HU": "Hungary", "IT": "Italy", "LU": "Luxembourg", "PL": "Poland",
         "PT": "Portugal", "RO": "Romania", "SI": "Slovenia", "SK": "Slovakia"}

# A country needs enough heatwave days for the comparison to mean anything.
# Below THIN the figure is still shown but flagged: Sweden reads -11.8% on three
# days, which is sampling noise rather than a demand collapse, and that is the
# shape of what a low count buys you.
MIN_DAYS, THIN = 8, 10
LABEL = {"imports": "Net imports", "gas": "Gas", "coal": "Coal", "solar": "Solar",
         "wind": "Wind", "hydro": "Hydro", "nuclear": "Nuclear",
         "biomass": "Biomass", "other": "Other"}

# Not on the map, and why. Kept here rather than on the page: the widget is an
# embed and carries only its own legend, but the next person to wonder where
# Malta went should not have to re-derive it.
#   Estonia, Finland  - no heatwave days at all this summer
#   Sweden 3, Latvia 4, Lithuania 6, Ireland 6 - too few days to compare against
#   Malta             - ENTSO-E publishes no load or generation for it
#   Netherlands       - a TenneT reporting break makes the series unusable

print("querying...")
uplift = {r["country_code"]: r
          for r in get(f"v_heatwave_demand_uplift?select=*&heatwave_days=gte.{MIN_DAYS}")}
delta = get("mv_heatwave_component_delta?select=country_code,component,delta_mw,"
            f"delta_gwh,change_pct&heatwave_days=gte.{MIN_DAYS}")
temp = {r["country_code"]: r for r in get("v_heatwave_baseline_quality?select=*")}

comp = {}
for r in delta:
    comp.setdefault(r["country_code"], []).append(r)


def item(p):
    pct = p.get("change_pct")
    return {"k": LABEL[p["component"]], "id": p["component"],
            "mw": round(float(p["delta_mw"])),
            "gwh": round(float(p["delta_gwh"]), 1),
            "pct": round(float(pct), 1) if pct is not None else None}


countries = {}
for cc, u in sorted(uplift.items()):
    if cc not in NAMES:
        continue
    parts = sorted(comp.get(cc, []), key=lambda r: -float(r["delta_mw"]))
    up = [item(p) for p in parts if float(p["delta_mw"]) > 0]
    dn = [item(p) for p in parts if float(p["delta_mw"]) < 0][::-1]
    t = temp.get(cc, {})
    note = ""
    if not any(x["id"] == "imports" for x in up + dn):
        note = ("Sits inside the DE-LU bidding zone, so it has no separate "
                "cross-border trade figure." if cc == "LU"
                else "An island system with no interconnector, so there is no trade to report.")
    countries[cc] = {
        "name": NAMES[cc],
        "thin": int(u["heatwave_days"]) < THIN,
        "days": int(u["heatwave_days"]),
        "normalDays": int(u["normal_days"]),
        "mean": round(float(u["mean_demand_uplift_pct"]), 1),
        "peak": round(float(u["peak_demand_uplift_pct"]), 1),
        "normalMw": round(float(u["normal_mean_mw"])),
        "hwMw": round(float(u["heatwave_mean_mw"])),
        "hwTemp": round(float(t["hw_tmax_c"]), 1) if t.get("hw_tmax_c") is not None else None,
        "refTemp": round(float(t["ref_tmax_c"]), 1) if t.get("ref_tmax_c") is not None else None,
        "note": note, "up": up, "down": dn,
    }
print(f"  {len(countries)} countries")

# ---- outlines -------------------------------------------------------------
# East has to reach 35 deg or Cyprus - the single largest demand rise on the
# map - falls off the edge entirely. North stops just past Denmark, since no
# country above it qualifies and the empty sea only shrinks everything else.
BOUNDS = dict(min_lon=-10.5, max_lon=35.2, min_lat=34.3, max_lat=58.4)
W = 780
MIDLAT = math.radians((BOUNDS["min_lat"] + BOUNDS["max_lat"]) / 2)
LONSPAN = (BOUNDS["max_lon"] - BOUNDS["min_lon"]) * math.cos(MIDLAT)
LATSPAN = BOUNDS["max_lat"] - BOUNDS["min_lat"]
# Derive the height from the bounds so the frame holds no dead margin.
H = round(W * LATSPAN / LONSPAN)
S = W / LONSPAN
OFFX = OFFY = 0


def proj(lon, lat):
    x = (lon - BOUNDS["min_lon"]) * math.cos(MIDLAT) * S + OFFX
    y = H - ((lat - BOUNDS["min_lat"]) * S) - OFFY
    return x, y


def simplify(pts, tol):
    """Douglas-Peucker: the raw rings are far denser than 760px needs."""
    if len(pts) < 3:
        return pts
    dmax, idx = 0.0, 0
    (x1, y1), (x2, y2) = pts[0], pts[-1]
    for i in range(1, len(pts) - 1):
        x0, y0 = pts[i]
        num = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1)
        den = math.hypot(y2 - y1, x2 - x1) or 1e-9
        d = num / den
        if d > dmax:
            dmax, idx = d, i
    if dmax > tol:
        return simplify(pts[:idx + 1], tol)[:-1] + simplify(pts[idx:], tol)
    return [pts[0], pts[-1]]


def simplify_ring(pts, tol):
    """Douglas-Peucker measures distance from the chord between the first and
    last point. On a closed ring those are the SAME point, so the chord is
    degenerate, every distance computes to zero, and the ring collapses to
    nothing. Split it into two arcs at the farthest vertex and simplify each.
    """
    if len(pts) > 3 and abs(pts[0][0] - pts[-1][0]) < 1e-9 \
            and abs(pts[0][1] - pts[-1][1]) < 1e-9:
        pts = pts[:-1]
    if len(pts) < 4:
        return pts
    x0, y0 = pts[0]
    far = max(range(1, len(pts)), key=lambda i: math.dist(pts[i], (x0, y0)))
    a = simplify(pts[:far + 1], tol)
    b = simplify(pts[far:] + [pts[0]], tol)
    return a[:-1] + b[:-1]


geo = json.loads((ROOT / "assets" / "geo" / "europe-countries.geojson")
                 .read_text(encoding="utf-8"))
shapes, ctx = {}, []
for f in geo["features"]:
    iso2 = (f.get("properties", {}).get("ISO2") or "").upper()
    if not iso2:
        continue
    cc = "GB" if iso2 == "UK" else iso2
    geom = f.get("geometry") or {}
    rings = ([geom["coordinates"][0]] if geom.get("type") == "Polygon"
             else [p[0] for p in geom.get("coordinates", []) if p and p[0]]
             if geom.get("type") == "MultiPolygon" else [])
    paths, kept = [], []
    for ring in rings:
        pts = [proj(lo, la) for lo, la in ring]
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        if max(xs) < -60 or min(xs) > W + 60 or max(ys) < -60 or min(ys) > H + 60:
            continue
        # Decimate before simplifying: the recursion is depth-proportional to
        # the point count and the densest rings would otherwise overflow it.
        while len(pts) > 600:
            pts = pts[::2]
        pts = simplify_ring(pts, 0.7)
        if len(pts) < 3:
            continue
        xs2 = [p[0] for p in pts]
        ys2 = [p[1] for p in pts]
        # A ring a couple of pixels across is an offshore islet: invisible, and
        # only costs payload.
        if max(xs2) - min(xs2) < 2 and max(ys2) - min(ys2) < 2:
            continue
        paths.append("M" + "L".join(f"{x:.1f} {y:.1f}" for x, y in pts) + "Z")
        kept.append(pts)
    if not paths:
        continue
    if cc in countries:
        big = max(kept, key=len)
        xs = [p[0] for p in big]
        ys = [p[1] for p in big]
        # The source carries its own label point; a vertex mean would be dragged
        # off by dense coastline (Greece, Croatia) and land offshore.
        props = f.get("properties", {})
        if props.get("LON") is not None and props.get("LAT") is not None:
            cx, cy = proj(float(props["LON"]), float(props["LAT"]))
        else:
            cx, cy = sum(xs) / len(xs), sum(ys) / len(ys)
        shapes[cc] = {"d": " ".join(paths), "cx": round(cx, 1), "cy": round(cy, 1),
                      "span": round(max(max(xs) - min(xs), max(ys) - min(ys)), 1)}
    else:
        ctx.append(" ".join(paths))

payload = {"w": W, "h": H, "countries": countries, "shapes": shapes, "context": ctx}
print(f"  {len(shapes)} shapes, {len(ctx)} context shapes")

TEMPLATE = Path(__file__).resolve().parent / "heatwave_map_template.html"
html = TEMPLATE.read_text(encoding="utf-8")
html = html.replace("/*__DATA__*/null", json.dumps(payload, separators=(",", ":")))
OUT.write_text(html, encoding="utf-8")
print(f"wrote {OUT}  ({OUT.stat().st_size / 1024:.0f} KB)")

# The three widget blocks are not contiguous in the standalone page - the styles
# sit in <head> and the rest in <body> - so pasting it into a CMS field would
# mean three separate copies and a chance to miss one. Emit them joined, ready
# to select-all and paste.
BEGIN, END = "<!-- ==================== EMBEDDABLE WIDGET", "WIDGET: SCRIPT ==================== -->"
blocks = []
for opener, closer in (("<style>", "</style>"),
                       ('<div class="hwmap"', "END EMBEDDABLE WIDGET: MARKUP ================== -->"),
                       ("<script>", "</script>")):
    i = html.index(opener, html.index(BEGIN) if opener == "<style>" else 0)
    if opener == "<script>":                       # the widget script is the last one
        i = html.rindex(opener)
    blocks.append(html[i:html.index(closer, i) + len(closer)])

EMBED = ROOT / "heatwave-map-embed.html"
EMBED.write_text(
    "<!-- Heatwave map widget. Paste all of this into one Custom HTML block.\n"
    "     Self-contained: no network requests, no external files, no API keys.\n"
    f"     Generated by python/make_heatwave_map_html.py - {len(countries)} countries. -->\n\n"
    + "\n\n".join(blocks) + "\n", encoding="utf-8")
print(f"wrote {EMBED}  ({EMBED.stat().st_size / 1024:.0f} KB)")
