"""Two shareable infographics of the same ten countries in central and
south-east Europe, on heatwave days.

Both carry identical labels, identical flow arrows and the same corridor
emphasis. Only the fill and its legend differ, so the pair can be read against
each other without re-learning the picture:

  heatwave_price_trade_map   shaded by net trade position
  heatwave_price_level_map   shaded by day-ahead price

Trade is the fill worth having if you only take one. Across these ten the
correlation between price and net imports is +0.67; between price and renewable
share it is +0.03. Renewable share simply does not order them - Austria at 84%
and Slovakia at 16% paid within 9 EUR/MWh of each other - so shading by it
invites a reader to hunt for a pattern that is not there.

Neither figure causes the other and the charts do not claim it: a country
imports because it is short, and being short is what costs.

Arrows are the mean net flow across every border between the ten, scaled by
volume and pointing the way the power actually went. On the price version they
make the headline visible rather than asserted - the flows run out of the pale
countries and into the dark ones. The script counts how often that held instead
of claiming it always does: 10 of the 13 borders.

Each border carries two numbers: the price spread between its two ends and
the volume that crossed it. They answer different questions - the spread is
why the power moved, the volume is how much of it did - and the euro sign
tells them apart without spending a line on units.

The trade scale is diverging, because importing and exporting are opposite
states rather than more and less of one thing, so lightness peaks at the
balanced midpoint. The price scale is sequential, because price is a magnitude.
Every step of both was checked for 4.5:1 against the ink chosen for it.

Writes PDF and PNG for each to python/output/.
"""
from __future__ import annotations

import json
import math
import urllib.request
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import patheffects as pe
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

# Diverging, because net trade has a sign: a country is either leaning on its
# neighbours or supplying them, and those are opposite states rather than more
# and less of one thing. Lightness peaks at the balanced midpoint and falls
# toward both poles. Every step carries text and was checked for 4.5:1 against
# the ink chosen for it; adjacent steps are at least 17 apart in OKLab.
BINS = [-20, -5, 5, 15]
RAMP = ["#146b7a", "#84bcc7", "#eeeeea", "#eeb078", "#b3541d"]
BIN_LABELS = ["exports 20%+ of demand", "exports 5-20%", "broadly balanced",
              "imports 5-15%", "imports 15%+"]
ON_FILL = ["#ffffff", "#152a19", "#152a19", "#152a19", "#ffffff"]

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
         "GR": (-2, -4), "RO": (6, 0)}


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
trade = {r["country_code"]: r for r in get("v_heatwave_trade_position?select=*")}
uplift = {r["country_code"]: r for r in get("v_heatwave_demand_uplift?select=*")}

D = {}
for cc in CEE:
    p, r, t, u = price[cc], res[cc], trade[cc], uplift[cc]
    # Net trade as a share of that country's own demand, so a 27 GWh/day import
    # into Hungary and a 27 GWh/day import into France are not made to look
    # like the same thing. Positive is a net importer.
    dem = float(u["heatwave_mean_mw"]) * 24 / 1000.0
    net = float(t["heatwave_net_import_gwh"])
    D[cc] = dict(
        name=NAMES[cc],
        price=float(p["heatwave_price_eur"]),
        normal_price=float(p["normal_price_eur"]),
        rise=float(p["delta_eur"]),
        rise_pct=float(p["change_pct"]),
        resh=float(r["heatwave_renewable_pct"]),
        res_delta=float(r["delta_pp"]),
        net=net, demand=dem, imp=net / dem * 100.0,
        # The views can differ by a day where one series has a gap, so the
        # smallest count is the honest one to quote.
        days=min(int(p["heatwave_days"]), int(r["heatwave_days"]),
                 int(t["heatwave_days"])),
    )

# ---- net flow on each border, on heatwave days ----------------------------
def page(q):
    out, off = [], 0
    while True:
        chunk = get(f"{q}&limit=1000&offset={off}")
        out += chunk
        if len(chunk) < 1000:
            return out
        off += 1000


hw_days = {}
for r in page("v_heatwave_days?select=country_code,date"):
    hw_days.setdefault(r["country_code"], set()).add(r["date"])

# Every border is stored in both directions, so collapse each to one canonical
# orientation before summing or the mirror row cancels it out.
_acc = {}
for r in page("mv_crossborder_net_daily?select=date,from_country,to_country,"
              "net_export_gwh&date=gte.2026-05-01&date=lte.2026-09-30"):
    a, b = r["from_country"], r["to_country"]
    if a not in NAMES or b not in NAMES:
        continue
    # A border belongs to the heat event if either end was in a heatwave.
    if r["date"] not in hw_days.get(a, ()) and r["date"] not in hw_days.get(b, ()):
        continue
    lo, hi = (a, b) if a < b else (b, a)
    v = float(r["net_export_gwh"] or 0)
    _acc.setdefault((lo, hi), {})[r["date"]] = v if a == lo else -v

FLOW = {}
for (lo, hi), days in _acc.items():
    mean = sum(days.values()) / len(days)
    src, dst = (lo, hi) if mean > 0 else (hi, lo)
    FLOW[(lo, hi)] = FLOW[(hi, lo)] = dict(src=src, dst=dst, gwh=abs(mean),
                                           days=len(days))
print(f"  {len(_acc)} borders with flow on heatwave days")

# Coupling is supposed to send power toward the dearer market. Count how often
# it actually did, rather than asserting that it did.
_pairs = [k for k in _acc if k[0] in D and k[1] in D]
NBORDERS = len(_pairs)
TOWARD = sum(1 for k in _pairs
             if D[FLOW[k]["dst"]]["price"] > D[FLOW[k]["src"]]["price"])
print(f"  flow ran toward the dearer market on {TOWARD} of {NBORDERS} borders")


def corr(key):
    n = len(D)
    xs = [D[c][key] for c in D]
    ys = [D[c]["price"] for c in D]
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    sx = math.sqrt(sum((v - mx) ** 2 for v in xs))
    sy = math.sqrt(sum((v - my) ** 2 for v in ys))
    return cov / (sx * sy)


lo_p = min(D, key=lambda c: D[c]["price"])
hi_p = max(D, key=lambda c: D[c]["price"])
R_IMP, R_RES = corr("imp"), corr("resh")
print(f"  price {D[lo_p]['price']:.0f}-{D[hi_p]['price']:.0f} EUR/MWh")
print(f"  price vs net import share  r = {R_IMP:+.2f}")
print(f"  price vs renewable share   r = {R_RES:+.2f}")


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
# Two versions of the same map. Only the fill and its legend change: the labels,
# the flow arrows and the corridor emphasis are identical, so the pair can be
# read against each other without re-learning the picture.
MODES = {
    "trade": dict(
        key="imp",
        # Diverging: importing and exporting are opposite states, not more and
        # less of one thing, so lightness peaks at the balanced midpoint.
        bins=[-20, -5, 5, 15],
        ramp=["#146b7a", "#84bcc7", "#eeeeea", "#eeb078", "#b3541d"],
        ink=["#ffffff", "#152a19", "#152a19", "#152a19", "#ffffff"],
        labels=["exports 20%+ of demand", "exports 5-20%", "broadly balanced",
                "imports 5-15%", "imports 15%+"],
        legend="Net trade on\nheatwave days",
        title=("The importers paid the most,", "the exporters paid the least"),
        file="heatwave_price_trade_map",
    ),
    "price": dict(
        key="price",
        # Sequential: price is a magnitude, so one hue, light to dark. Every
        # step was checked for 4.5:1 against the ink chosen for it and adjacent
        # steps are at least 1.3:1 apart in luminance.
        bins=[120, 126, 131, 136],
        ramp=["#fdf1dc", "#f7cf99", "#eda75d", "#d97b34", "#a8431a"],
        ink=["#152a19", "#152a19", "#152a19", "#152a19", "#ffffff"],
        labels=[f"under {EURO}120", f"{EURO}120-125", f"{EURO}126-130",
                f"{EURO}131-135", f"{EURO}136 and over"],
        legend="Day-ahead price on\nheatwave days",
        title=("Power flowed out of the cheap markets", "and into the dear ones"),
        file="heatwave_price_level_map",
    ),
}

CSPREAD = max(D[c]["price"] for c in CORRIDOR) - min(D[c]["price"] for c in CORRIDOR)
GMAX = max(f["gwh"] for f in FLOW.values())
ARROW = "#22313d"


def shrink(p, q, by):
    """Pull an endpoint back along the segment so the arrow starts clear of the
    label text rather than under it."""
    dx, dy = q[0] - p[0], q[1] - p[1]
    n = math.hypot(dx, dy) or 1.0
    return p[0] + dx / n * by, p[1] + dy / n * by


def flow_arrow(a, b, gwh, zorder):
    """One filled polygon, outlined.

    Built from a line plus a separate arrowhead first, with a second wider
    arrow drawn underneath as a white casing. That looked wrong: the casing's
    head is scaled independently of the shaft, so it protruded around the dark
    head as a ragged white wedge. A single 'simple' arrow is one closed shape,
    and a stroke path-effect outlines the whole silhouette evenly - which is
    what the casing was trying to be.
    """
    w = (gwh / GMAX) ** 0.55
    tail = 1.8 + 5.4 * w
    head_w = tail * 2.5
    head_l = tail * 2.3
    return FancyArrowPatch(
        a, b,
        arrowstyle=f"simple,head_width={head_w:.2f},head_length={head_l:.2f},"
                   f"tail_width={tail:.2f}",
        mutation_scale=1, facecolor=ARROW, edgecolor="none",
        shrinkA=0, shrinkB=0, zorder=zorder,
        path_effects=[pe.withStroke(linewidth=2.4, foreground=SEAM)])


def render(mode):
    M = MODES[mode]
    ramp, ink_for, bins = M["ramp"], M["ink"], M["bins"]

    def bin_of(v):
        i = 0
        while i < len(bins) and v >= bins[i]:
            i += 1
        return i

    fig = plt.figure(figsize=(9.0, 11.0), dpi=200)
    fig.patch.set_facecolor("#ffffff")

    TOP = 0.818
    fig.text(0.055, 0.972, M["title"][0], fontsize=18, fontweight="bold",
             color=INK, va="top")
    fig.text(0.055, 0.947, M["title"][1], fontsize=18, fontweight="bold",
             color=INK, va="top")
    fig.text(0.055, 0.919,
             f"Net trade, day-ahead price and net cross-border flow on heatwave days, "
             f"May{NDASH}September 2026.\nAcross these ten, price tracks net trade "
             f"(r = {R_IMP:+.2f}), not renewable share (r = {R_RES:+.2f}). Neither causes the\n"
             f"other: a country imports because it is short, and being short is what costs.\n"
             f"Arrows are net flow, scaled by volume; power ran to the dearer market on "
             f"{TOWARD} of {NBORDERS} borders.",
             fontsize=10.2, color=MUTED, va="top", linespacing=1.5)

    ax = fig.add_axes([0.235, 0.100, 0.755, 0.730])
    ax.set_xlim(-16, W + 16)
    ax.set_ylim(-16, H + 16)
    ax.set_aspect("equal")
    ax.axis("off")

    for cc, rings in SHAPES.items():
        b = bin_of(D[cc][M["key"]])
        on_cor = cc in CORRIDOR
        for pts in rings:
            ax.add_patch(PathPatch(MPath(pts, closed=True), facecolor=ramp[b],
                                   edgecolor="#5d6b58" if on_cor else SEAM,
                                   linewidth=1.7 if on_cor else 1.3,
                                   zorder=2.5 if on_cor else 2))

    def label(x, y, d, ink):
        """Three lines stacked in POINTS, not in map units - offsetting in data
        coordinates made the spacing depend on how the map happened to scale."""
        common = dict(xycoords="data", textcoords="offset points", ha="center",
                      annotation_clip=False, zorder=7)
        ax.annotate(d["name"], (x, y), xytext=(0, 14), va="center", fontsize=8.6,
                    color=ink, fontweight="bold", **common)
        ax.annotate(f"{EURO}{d['price']:.0f}", (x, y), xytext=(0, -1), va="center",
                    fontsize=15.5, color=ink, fontweight="bold", **common)
        v = d["imp"]
        txt = (f"imports {v:.0f}%" if v >= 5 else
               f"exports {-v:.0f}%" if v <= -5 else "balanced")
        ax.annotate(txt, (x, y), xytext=(0, -15), va="center",
                    fontsize=8.2, color=ink, alpha=.82, **common)

    LABELPT = {}
    for cc in CEE:
        d = D[cc]
        axy = ANCHOR[cc]
        nx, ny = NUDGE.get(cc, (0, 0))
        LABELPT[cc] = ABROAD[cc] if cc in ABROAD else (axy[0] + nx, axy[1] + ny)
        if cc in ABROAD:
            tx, ty = ABROAD[cc]
            ax.plot([axy[0], tx], [axy[1], ty], color="#9aa2ae", lw=0.9, zorder=6,
                    solid_capstyle="round")
            ax.plot([axy[0]], [axy[1]], marker="o", ms=3.4, color="#9aa2ae", zorder=6)
            ax.add_patch(Rectangle((tx - 62, ty - 34), 124, 68, facecolor="#ffffff",
                                   edgecolor=FAINT, lw=1.0, zorder=6.5))
            label(tx, ty, d, INK)
        else:
            label(axy[0] + nx, axy[1] + ny, d, ink_for[bin_of(d[M["key"]])])

    # ---- net flow on every border, sized by volume ------------------------
    # Drawn between the two countries' own anchors so the arrow sits across the
    # border it describes, and centred there so it does not reach into either
    # country's label.
    placed = []
    # Biggest first, so the flows that matter most get the clearest spot and the
    # small ones move aside for them rather than the other way round.
    for (lo, hi) in sorted(_acc, key=lambda k: -FLOW[k]["gwh"]):
        fl = FLOW[(lo, hi)]
        pa, pb = ANCHOR[fl["src"]], ANCHOR[fl["dst"]]
        dist = math.hypot(pb[0] - pa[0], pb[1] - pa[1])
        # Kept short and hugging the border. Hungary has five of these and at
        # full length they all converged on its own label.
        half = min(37.0, dist * 0.17)
        mid = ((pa[0] + pb[0]) / 2, (pa[1] + pb[1]) / 2)
        a = shrink(mid, pa, half)
        b = shrink(mid, pb, half)
        ax.add_patch(flow_arrow(a, b, fl["gwh"], zorder=5.4))
        # Number beside the shaft, not on it, so it never breaks the arrow in two.
        ux, uy = (b[0] - a[0]) / (2 * half or 1), (b[1] - a[1]) / (2 * half or 1)
        px, py = -uy, ux
        # Clear of the shaft, not on it: the offset has to grow with the arrow,
        # since the widest heads are about as across as the badge is tall.
        off = 28 + 17 * (fl["gwh"] / GMAX) ** 0.55
        # Four borders meet within a few pixels around Slovenia, so the first
        # choice of side is not always free. Try the other side, then further
        # out, and take the first spot clear of the badges already placed.
        for sx, mult in ((1, 1.0), (-1, 1.0), (1, 1.7), (-1, 1.7), (1, 2.4)):
            cand = (mid[0] + px * off * sx * mult, mid[1] + py * off * sx * mult)
            if all(math.dist(cand, q) > 40 for q in placed):
                break
        placed.append(cand)
        # Both numbers, because they answer different questions: the spread is
        # why the power moved, the volume is how much of it did. The euro sign
        # tells them apart without spending a line on units.
        ax.text(cand[0], cand[1],
                f"{EURO}{abs(D[lo]['price'] - D[hi]['price']):.0f}\n{fl['gwh']:.1f}",
                ha="center", va="center", fontsize=7.2, color=ARROW,
                fontweight="bold", zorder=5.6, linespacing=1.25,
                bbox=dict(boxstyle="round,pad=0.24", facecolor="#ffffff",
                          edgecolor="#c9d2c6", linewidth=0.8))

    ax.annotate(
        f"Romania and Greece both ran about\n"
        f"{D['RO']['resh']:.0f}% renewable, {EURO}{D['RO']['price'] - D['GR']['price']:.0f}/MWh apart.\n"
        f"Romania imported {D['RO']['imp']:.0f}% of its power\n"
        f"that week; Greece exported {-D['GR']['imp']:.0f}%.",
        xy=(250, 250), xytext=(250, 250),
        fontsize=9.2, color=INK, linespacing=1.5, ha="center", va="center",
        zorder=8, annotation_clip=False,
        bbox=dict(boxstyle="round,pad=0.55", facecolor="#ffffff",
                  edgecolor="#c9d2c6", linewidth=1.1))

    # ---- legend ----------------------------------------------------------
    lx = 0.055
    fig.text(lx, TOP - 0.012, M["legend"], fontsize=10.4, fontweight="bold",
             color=INK, va="top", linespacing=1.35)
    y = TOP - 0.062
    for i, lab in enumerate(M["labels"]):
        fig.patches.append(Rectangle((lx, y - 0.0125), 0.052, 0.0175,
                                     transform=fig.transFigure, facecolor=ramp[i],
                                     edgecolor="#d4dad2", linewidth=0.7, zorder=3))
        fig.text(lx + 0.064, y - 0.0042, lab, fontsize=9.5, color=MUTED, va="center")
        y -= 0.0285

    y -= 0.022
    fig.lines.append(plt.Line2D([lx, lx + 0.175], [y, y], transform=fig.transFigure,
                                color=FAINT, lw=1))
    y -= 0.030
    fig.text(lx, y, "On each border", fontsize=10.4, fontweight="bold",
             color=INK, va="top")
    y -= 0.026
    # Kept narrow: the left column is only as wide as the legend swatches, and
    # a longer line reaches across into Slovenia's label box.
    fig.text(lx, y, f"{EURO}  price spread\n    net flow, GWh/day\n"
                    f"Arrows point downstream\nand widen with the volume.",
             fontsize=8.6, color=MUTED, va="top", linespacing=1.45)
    y -= 0.028
    # Two sample arrows at the extremes of the scale, so the widths mean something.
    y -= 0.052
    for gv in (min(f["gwh"] for f in FLOW.values()), GMAX):
        arr = flow_arrow((lx + 0.004, y), (lx + 0.062, y), gv, zorder=4)
        arr.set_transform(fig.transFigure)
        fig.patches.append(arr)
        fig.text(lx + 0.074, y, f"{gv:.1f}", fontsize=8.8, color=MUTED, va="center")
        y -= 0.030

    # ---- footer ----------------------------------------------------------
    fig.lines.append(plt.Line2D([0.055, 0.945], [0.088, 0.088],
                                transform=fig.transFigure, color=FAINT, lw=1))
    fig.text(0.055, 0.078,
             f"Net trade is the balance of physical cross-border flows over the country's "
             f"own demand. Arrows are the mean net flow across each border on days when\n"
             f"either end was in a heatwave; every border is collapsed to one direction "
             f"first, since ENTSO-E reports each one twice. All of each country's\n"
             f"interconnectors are included, the Balkan and non-EU borders among them. A "
             f"heatwave day is 3+ consecutive days above the local day-of-year 90th\n"
             f"percentile of daily maximum temperature, or at or above 30 {MIDDOT}C, against the "
             f"same country's non-heatwave days in the same season, weekday to weekday.\n"
             f"Price is the unweighted daily mean of day-ahead prices. "
             f"Source: ENTSO-E Transparency Platform; ERA5 via Open-Meteo.",
             fontsize=7.2, color=MUTED, va="top", linespacing=1.6)

    OUT.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        p = OUT / f"{M['file']}.{ext}"
        fig.savefig(p, facecolor="#ffffff")
        print(f"wrote {p}  ({p.stat().st_size / 1024:.0f} KB)")
    plt.close(fig)


for _m in MODES:
    render(_m)
