"""Shareable infographic: day-ahead prices and renewable share on heatwave days
across central and south-east Europe.

The finding the map is built around is a NON-relationship, and that is the point.
Across these ten countries the renewable share of generation on heatwave days
runs from about 16% to 84% while the average day-ahead price sits inside a band
of roughly 20 EUR/MWh. The correlation between the two is r = +0.03, nothing.

The Hungary-Romania-Bulgaria-Greece corridor makes the case sharpest, which is
why it carries the headline and the only annotation on the map. Four markets
wired to each other, a 19 EUR/MWh spread end to end - and Romania and Greece,
on renewable shares of 54% and 53%, sit at opposite ends of it.

The map shades by renewable share and prints the price on top, so a reader
looking for a pattern in the shading can see for themselves that there is not
one. That is a deliberate choice rather than an accident of encoding, so the
claim is also stated in words instead of being left to be inferred from a fill.

Writes PDF and PNG to python/output/.
"""
from __future__ import annotations

import json
import math
import urllib.request
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import PathPatch, Rectangle, FancyArrowPatch
from matplotlib.path import Path as MPath

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "python" / "output"

CEE = ["PL", "CZ", "SK", "AT", "HU", "SI", "HR", "RO", "BG", "GR"]
# Four markets connected in a chain, and the ones whose cross-border data was
# most incomplete until the Balkan and non-EU borders were backfilled. Romania
# and Greece sit at opposite ends of the price range on almost identical
# renewable shares, which is the whole point of the map.
CORRIDOR = ["HU", "RO", "BG", "GR"]
NAMES = {"PL": "Poland", "CZ": "Czechia", "SK": "Slovakia", "AT": "Austria",
         "HU": "Hungary", "SI": "Slovenia", "HR": "Croatia", "RO": "Romania",
         "BG": "Bulgaria", "GR": "Greece"}

# Sequential ramp, light to dark. Every step carries text, so each was checked
# for 4.5:1 against the ink chosen for it; the top step is the only one that
# takes white. Adjacent steps are >=1.35:1 apart in luminance.
BINS = [25, 40, 55, 70]
RAMP = ["#f3f9ef", "#c2dfb0", "#8ec378", "#519f47", "#22682e"]
BIN_LABELS = ["under 25%", "25-39%", "40-54%", "55-69%", "70% and over"]
ON_FILL = ["#152a19", "#152a19", "#152a19", "#152a19", "#ffffff"]

INK, MUTED, FAINT = "#14203a", "#6b7280", "#e6e8ec"
SEAM = "#ffffff"
EURO, NDASH, MIDDOT, MINUS = "€", "–", "·", "−"

# Only the ten countries are drawn, so everything west of Austria and Croatia is
# empty canvas. Slovenia and Croatia are too small and too thin to hold three
# lines of text, so their labels go out there on a leader; ABROAD gives the
# absolute position of the label box in projected units.
ABROAD = {"SI": (92, 688), "HR": (92, 516)}
# Hand nudges for labels that would otherwise sit on a border or over a
# neighbour's text.
NUDGE = {"SK": (16, 26), "AT": (-14, 2), "CZ": (-6, 6), "HU": (8, -6),
         "GR": (14, -14), "RO": (6, 0)}


def load_env() -> dict:
    env = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


ENV = load_env()


def get(q):
    req = urllib.request.Request(
        f"{ENV['SUPABASE_URL']}/rest/v1/{q}",
        headers={"apikey": ENV["SUPABASE_ANON_KEY"],
                 "Authorization": f"Bearer {ENV['SUPABASE_ANON_KEY']}"})
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read())


# ---------------------------------------------------------------- data
print("querying...")
price = {r["country_code"]: r for r in get("v_heatwave_price?select=*")}
res = {r["country_code"]: r for r in get("v_heatwave_renewable?select=*")}

D = {}
for cc in CEE:
    p, r = price[cc], res[cc]
    D[cc] = dict(
        name=NAMES[cc],
        price=float(p["heatwave_price_eur"]),
        normal_price=float(p["normal_price_eur"]),
        rise=float(p["delta_eur"]),
        rise_pct=float(p["change_pct"]),
        resh=float(r["heatwave_renewable_pct"]),
        res_delta=float(r["delta_pp"]),
        # The two views can differ by a day where one series has a gap, so the
        # smaller count is the honest one to quote.
        days=min(int(p["heatwave_days"]), int(r["heatwave_days"])),
    )

lo_res = min(D, key=lambda c: D[c]["resh"])
hi_res = max(D, key=lambda c: D[c]["resh"])
lo_p = min(D, key=lambda c: D[c]["price"])
hi_p = max(D, key=lambda c: D[c]["price"])
n = len(D)
mx = sum(D[c]["resh"] for c in D) / n
my = sum(D[c]["price"] for c in D) / n
cov = sum((D[c]["resh"] - mx) * (D[c]["price"] - my) for c in D)
sx = math.sqrt(sum((D[c]["resh"] - mx) ** 2 for c in D))
sy = math.sqrt(sum((D[c]["price"] - my) ** 2 for c in D))
CORR = cov / (sx * sy)
print(f"  renewable share {D[lo_res]['resh']:.0f}-{D[hi_res]['resh']:.0f}%, "
      f"price {D[lo_p]['price']:.0f}-{D[hi_p]['price']:.0f} EUR/MWh, r = {CORR:+.2f}")


def bin_of(v: float) -> int:
    i = 0
    while i < len(BINS) and v >= BINS[i]:
        i += 1
    return i


# ---------------------------------------------------------------- geometry
# Crete and the far Aegean islands are dropped: they stretch the frame a long
# way south for a few pixels of land and shrink everything else.
BOUNDS = dict(min_lon=9.0, max_lon=30.4, min_lat=36.1, max_lat=55.2)
W = 1000.0
MIDLAT = math.radians((BOUNDS["min_lat"] + BOUNDS["max_lat"]) / 2)
LONSPAN = (BOUNDS["max_lon"] - BOUNDS["min_lon"]) * math.cos(MIDLAT)
LATSPAN = BOUNDS["max_lat"] - BOUNDS["min_lat"]
H = W * LATSPAN / LONSPAN
S = W / LONSPAN


def proj(lon, lat):
    return ((lon - BOUNDS["min_lon"]) * math.cos(MIDLAT) * S,
            (lat - BOUNDS["min_lat"]) * S)


def simplify(pts, tol):
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
    """On a closed ring the first and last point are the same, so the chord
    Douglas-Peucker measures against is degenerate and the whole ring collapses.
    Split it at the farthest vertex and simplify the two arcs."""
    if len(pts) > 3 and abs(pts[0][0] - pts[-1][0]) < 1e-9 \
            and abs(pts[0][1] - pts[-1][1]) < 1e-9:
        pts = pts[:-1]
    if len(pts) < 4:
        return pts
    x0, y0 = pts[0]
    far = max(range(1, len(pts)), key=lambda i: math.dist(pts[i], (x0, y0)))
    return simplify(pts[:far + 1], tol)[:-1] + simplify(pts[far:] + [pts[0]], tol)[:-1]


geo = json.loads((ROOT / "assets" / "geo" / "europe-countries.geojson")
                 .read_text(encoding="utf-8"))
SHAPES, ANCHOR = {}, {}
for f in geo["features"]:
    cc = (f.get("properties", {}).get("ISO2") or "").upper()
    if cc not in CEE:
        continue
    g = f.get("geometry") or {}
    rings = ([g["coordinates"][0]] if g.get("type") == "Polygon"
             else [p[0] for p in g.get("coordinates", []) if p and p[0]]
             if g.get("type") == "MultiPolygon" else [])
    kept = []
    for ring in rings:
        pts = [proj(lo, la) for lo, la in ring]
        xs, ys = [p[0] for p in pts], [p[1] for p in pts]
        if max(xs) < -40 or min(xs) > W + 40 or max(ys) < -40 or min(ys) > H + 40:
            continue
        while len(pts) > 600:
            pts = pts[::2]
        pts = simplify_ring(pts, 0.8)
        if len(pts) < 3:
            continue
        xs, ys = [p[0] for p in pts], [p[1] for p in pts]
        if max(xs) - min(xs) < 3 and max(ys) - min(ys) < 3:
            continue
        kept.append(pts)
    SHAPES[cc] = kept
    pr = f["properties"]
    ANCHOR[cc] = proj(float(pr["LON"]), float(pr["LAT"]))
print(f"  {len(SHAPES)} outlines")


# ---------------------------------------------------------------- draw
fig = plt.figure(figsize=(9.0, 11.0), dpi=200)
fig.patch.set_facecolor("#ffffff")

cor_p = [D[c]["price"] for c in CORRIDOR]
cor_r = [D[c]["resh"] for c in CORRIDOR]
CSPREAD = max(cor_p) - min(cor_p)

# The subtitle is three lines now; the legend has to start below where it ends.
TOP = 0.843
fig.text(0.055, 0.972, f"Romania and Greece both ran {D['RO']['resh']:.0f}% renewable.",
         fontsize=18, fontweight="bold", color=INK, va="top")
fig.text(0.055, 0.947, f"One paid {EURO}{D['RO']['price'] - D['GR']['price']:.0f}/MWh more.",
         fontsize=18, fontweight="bold", color=INK, va="top")
fig.text(0.055, 0.919,
         f"Average day-ahead price and renewable share of generation on heatwave days, "
         f"May{NDASH}September 2026.\nAlong the Hungary{NDASH}Romania{NDASH}Bulgaria"
         f"{NDASH}Greece corridor {MIDDOT} four markets wired to each other {MIDDOT} the spread\n"
         f"is {EURO}{CSPREAD:.0f}/MWh, while renewable share swings between "
         f"{min(cor_r):.0f}% and {max(cor_r):.0f}%.",
         fontsize=10.2, color=MUTED, va="top", linespacing=1.5)

# ---- map axis
# With only the legend in the left column, the map takes the width the two
# removed panels were using.
ax = fig.add_axes([0.235, 0.075, 0.755, 0.805])
ax.set_xlim(-16, W + 16)
ax.set_ylim(-16, H + 16)
ax.set_aspect("equal")
ax.axis("off")

for cc, rings in SHAPES.items():
    b = bin_of(D[cc]["resh"])
    # The corridor keeps a darker seam so the chain reads as one object without
    # having to tint the four away from their own renewable-share colour.
    on_cor = cc in CORRIDOR
    for pts in rings:
        ax.add_patch(PathPatch(MPath(pts, closed=True), facecolor=RAMP[b],
                               edgecolor="#5d6b58" if on_cor else SEAM,
                               linewidth=1.7 if on_cor else 1.3,
                               zorder=2.5 if on_cor else 2))


def label(x, y, d, ink):
    """Three lines stacked in POINTS, not in map units. Offsetting in data
    coordinates made the spacing depend on how the map happened to scale, which
    is why the name and the price were landing on top of each other."""
    common = dict(xycoords="data", textcoords="offset points", ha="center",
                  annotation_clip=False, zorder=5)
    ax.annotate(d["name"], (x, y), xytext=(0, 14), va="center", fontsize=8.6,
                color=ink, fontweight="bold", **common)
    ax.annotate(f"{EURO}{d['price']:.0f}", (x, y), xytext=(0, -1), va="center",
                fontsize=15.5, color=ink, fontweight="bold", **common)
    ax.annotate(f"RES {d['resh']:.0f}%", (x, y), xytext=(0, -15), va="center",
                fontsize=8.2, color=ink, alpha=.82, **common)


LABELPT = {}
for cc in CEE:
    d = D[cc]
    axy = ANCHOR[cc]
    nx, ny = NUDGE.get(cc, (0, 0))
    LABELPT[cc] = ABROAD[cc] if cc in ABROAD else (axy[0] + nx, axy[1] + ny)
    if cc in ABROAD:
        tx, ty = ABROAD[cc]
        ax.plot([axy[0], tx], [axy[1], ty], color="#9aa2ae", lw=0.9, zorder=4,
                solid_capstyle="round")
        ax.plot([axy[0]], [axy[1]], marker="o", ms=3.4, color="#9aa2ae", zorder=4)
        ax.add_patch(Rectangle((tx - 62, ty - 34), 124, 68, facecolor="#ffffff",
                               edgecolor=FAINT, lw=1.0, zorder=4.5))
        label(tx, ty, d, INK)
    else:
        label(axy[0] + nx, axy[1] + ny, d, ON_FILL[bin_of(d["resh"])])

# ---- the corridor: a link between each neighbouring pair, with the gap on it
def shrink(p, q, by):
    """Pull an endpoint back along the segment so the arrow starts clear of the
    label text rather than under it."""
    dx, dy = q[0] - p[0], q[1] - p[1]
    n = math.hypot(dx, dy) or 1.0
    return p[0] + dx / n * by, p[1] + dy / n * by


for a, b in zip(CORRIDOR, CORRIDOR[1:]):
    pa, pb = LABELPT[a], LABELPT[b]
    ax.annotate("", xy=shrink(pb, pa, 62), xytext=shrink(pa, pb, 62),
                arrowprops=dict(arrowstyle="<->", color="#5d6b58", lw=1.3,
                                shrinkA=0, shrinkB=0), zorder=5.5)
    mid = ((pa[0] + pb[0]) / 2, (pa[1] + pb[1]) / 2)
    ax.text(mid[0], mid[1], f"{EURO}{abs(D[a]['price'] - D[b]['price']):.0f}\nspread",
            ha="center", va="center", fontsize=8.4, color=INK, linespacing=1.3,
            zorder=6, bbox=dict(boxstyle="round,pad=0.34", facecolor="#ffffff",
                                edgecolor="#c9d2c6", linewidth=0.9))

# ---- the comparison the map is built around, called out on it
ax.annotate(
    f"Same renewable share,\n"
    f"{EURO}{D['RO']['price'] - D['GR']['price']:.0f}/MWh spread:\n"
    f"Romania {D['RO']['resh']:.0f}% at {EURO}{D['RO']['price']:.0f},\n"
    f"Greece {D['GR']['resh']:.0f}% at {EURO}{D['GR']['price']:.0f}.",
    # No leader. It names both countries, both are labelled on the map, and a
    # line to either one would have to cross the other's text to get there.
    xy=(250, 250), xytext=(250, 250),
    fontsize=9.2, color=INK, linespacing=1.5, ha="center", va="center",
    zorder=6, annotation_clip=False,
    bbox=dict(boxstyle="round,pad=0.55", facecolor="#ffffff",
              edgecolor="#c9d2c6", linewidth=1.1))

# ---- left column: legend, then the heatwave effect ranked
lx = 0.055
fig.text(lx, TOP - 0.012, "Renewable share\nof generation", fontsize=10.4,
         fontweight="bold", color=INK, va="top", linespacing=1.35)
y = TOP - 0.062
for i, lab in enumerate(BIN_LABELS):
    fig.patches.append(Rectangle((lx, y - 0.0125), 0.052, 0.0175,
                                 transform=fig.transFigure, facecolor=RAMP[i],
                                 edgecolor="#d4dad2", linewidth=0.7, zorder=3))
    fig.text(lx + 0.064, y - 0.0042, lab, fontsize=9.5, color=MUTED, va="center")
    y -= 0.0285

# ---- footer
fig.lines.append(plt.Line2D([0.055, 0.945], [0.062, 0.062],
                            transform=fig.transFigure, color=FAINT, lw=1))
fig.text(0.055, 0.052,
         f"Correlation between renewable share and price across the ten: r = {CORR:+.2f}. "
         f"A heatwave day is 3+ consecutive days above the local day-of-year 90th\n"
         f"percentile of daily maximum temperature, or at or above 30 {MIDDOT}C; each country "
         f"is measured against its own non-heatwave days in the same May{NDASH}September\n"
         f"season, weekdays matched to weekdays. Price is the unweighted daily mean of "
         f"day-ahead prices. Source: ENTSO-E Transparency Platform; ERA5 via Open-Meteo.",
         fontsize=7.2, color=MUTED, va="top", linespacing=1.6)

OUT.mkdir(parents=True, exist_ok=True)
for ext in ("pdf", "png"):
    p = OUT / f"heatwave_price_renewable_map.{ext}"
    fig.savefig(p, facecolor="#ffffff", bbox_inches=None)
    print(f"wrote {p}  ({p.stat().st_size / 1024:.0f} KB)")
plt.close(fig)
