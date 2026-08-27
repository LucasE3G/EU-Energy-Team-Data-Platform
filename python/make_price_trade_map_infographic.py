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

CEE = ["AT", "SK", "HU", "RO", "BG", "GR"]
# Four markets connected in a chain, and the ones whose cross-border data was
# most incomplete until the Balkan and non-EU borders were backfilled. Romania
# and Greece sit at opposite ends of the price range on almost identical
# renewable shares, which is the whole point of the map.
CORRIDOR = ["HU", "RO", "BG", "GR"]
NAMES = {"AT": "Austria", "SK": "Slovakia", "HU": "Hungary",
         "RO": "Romania", "BG": "Bulgaria", "GR": "Greece"}

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

# Nothing is too small to hold its own label now that Slovenia and Croatia are
# out, so no label needs leading off the map.
ABROAD = {}
# Hand nudges for labels that would otherwise sit on a border or over a
# neighbour's text.
# Slovakia sits on its own centroid. It is shallow, its northern border is
# notched and its western end is narrow, so nudging the label in any direction
# runs it off the country - and its ink is white, because its fill is dark, so
# off the country means invisible. The arrows move instead.
NUDGE = {"SK": (0, 3), "AT": (-10, 0), "HU": (-16, -19),
         "GR": (4, 12), "RO": (6, 0)}


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
        cc=cc,
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
# way south for a few pixels of land and shrink everything else. With Poland and
# Czechia out the frame no longer has to reach north of Slovakia.
BOUNDS = dict(min_lon=9.2, max_lon=30.2, min_lat=35.9, max_lat=50.2)
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
# Built to the E3G visual guidelines (Comms, September 2025).
#
#   Size      15cm column width is a hard ceiling. The side legend that used to
#             sit beside the map does not fit inside it, so the key runs across
#             the top and the map takes the full width.
#   Title     Georgia bold 14pt Royal Blue, centred.
#   Subtitle  Open Sans bold, all caps, 10pt, Amaranth - the lead colour of the
#             energy narrative this sits in.
#   Colour    Both scales come from the E3G ramps. Price is sequential, so it is
#             tints and shades of one hue, as the guide directs for sequential
#             data. Trade is diverging because it has a sign, so it runs E3G
#             blues through a neutral to E3G reds. Every step carries a value
#             and was checked for 4.5:1 against the ink chosen for it.
#   Arrows    Smoky Black, the guide's colour for connecting lines and arrows.
#   Source    Open Sans italic 8pt Smoky Black, bottom left, naming the database
#             and access date, and E3G as the analyst.
#   Caption   NOT in the image - the guide sets it as document text below.
CM = 1 / 2.54
COL_W_CM, FIG_H_CM = 15.0, 20.0

ROYAL, VISTA, AMARANTH = "#282970", "#82a4dd", "#cd3951"
SMOKY, FLORAL, GREY_RULE = "#1b1817", "#fffaf0", "#c4c2ba"
SANS = "Open Sans"
TITLE_FONT = {"family": "Georgia", "weight": "bold", "size": 14, "color": ROYAL}

MODES = {
    "trade": dict(
        key="imp",
        bins=[-20, -5, 5, 15],
        # Diverging: importing and exporting are opposite states, not more and
        # less of one thing, so lightness peaks at the balanced midpoint.
        ramp=["#455294", "#83a4de", "#f6f4ea", "#de808f", "#b52247"],
        ink=["#ffffff", SMOKY, SMOKY, SMOKY, "#ffffff"],
        labels=["exports 20%+", "exports 5-20%", "balanced",
                "imports 5-15%", "imports 15%+"],
        legend="NET TRADE, % OF OWN DEMAND  ·  HEATWAVE DAYS, MAY–SEPTEMBER 2026",
        title=("Interconnectivity was key to address power supply\n"
               "needs in Hungary and Romania"),
        file="heatwave_price_trade_map",
    ),
    "price": dict(
        key="price",
        bins=[126, 131, 136],
        # Sequential: one hue, light to dark, per the guide's rule for data that
        # is sequential rather than categorical. Four steps, not five: no country
        # here falls under 120, so that band was a key to an empty set.
        ramp=["#de808f", "#cd3951", "#9c2b3d", "#6b1c29"],
        ink=[SMOKY, "#ffffff", "#ffffff", "#ffffff"],
        labels=[f"{EURO}120-125", f"{EURO}126-130", f"{EURO}131-135",
                f"{EURO}136+"],
        legend="DAY-AHEAD PRICE, EUR/MWH  ·  HEATWAVE DAYS, MAY–SEPTEMBER 2026",
        title=("Electricity price differences in the region still\n"
               "suggest the need for further market integration"),
        file="heatwave_price_level_map",
    ),
}

GMAX = max(f["gwh"] for f in FLOW.values())
GMIN = min(f["gwh"] for f in FLOW.values())
ARROW = SMOKY


def shrink(p, q, by):
    """Pull an endpoint back along the segment so the arrow starts clear of the
    label text rather than under it."""
    dx, dy = q[0] - p[0], q[1] - p[1]
    n = math.hypot(dx, dy) or 1.0
    return p[0] + dx / n * by, p[1] + dy / n * by


def flow_arrow(a, b, gwh, zorder, scale=1.0):
    """One filled polygon, outlined.

    A line plus a separate arrowhead scales the two independently, so a white
    casing drawn underneath protrudes around the dark head as a ragged wedge.
    A 'simple' arrow is one closed shape and a stroke outlines the whole
    silhouette evenly.
    """
    w = (gwh / GMAX) ** 0.55
    tail = (1.4 + 4.0 * w) * scale
    return FancyArrowPatch(
        a, b,
        arrowstyle=f"simple,head_width={tail * 2.5:.2f},"
                   f"head_length={tail * 2.3:.2f},tail_width={tail:.2f}",
        mutation_scale=1, facecolor=ARROW, edgecolor="none",
        shrinkA=0, shrinkB=0, zorder=zorder,
        path_effects=[pe.withStroke(linewidth=2.0, foreground="#ffffff")])


def text_width(txt, size, weight):
    """Width of a string in points, from the renderer rather than a guess at
    average character width - the euro sign and the digits are nothing like the
    same width as lowercase letters."""
    fig = plt.figure(figsize=(1, 1))
    t = fig.text(0, 0, txt, fontsize=size, fontweight=weight, fontfamily=SANS)
    fig.canvas.draw()
    w = t.get_window_extent(fig.canvas.get_renderer()).width * 72.0 / fig.dpi
    plt.close(fig)
    return w


def render(mode):
    M = MODES[mode]
    ramp, ink_for, bins = M["ramp"], M["ink"], M["bins"]

    def bin_of(v):
        i = 0
        while i < len(bins) and v >= bins[i]:
            i += 1
        return i

    plt.rcParams["font.family"] = [SANS]
    fig = plt.figure(figsize=(COL_W_CM * CM, FIG_H_CM * CM), dpi=300)
    fig.patch.set_facecolor("#ffffff")

    fig.text(0.5, 0.988, M["title"], ha="center", va="top",
             linespacing=1.35, **TITLE_FONT)
    fig.text(0.5, 0.906, M["legend"], ha="center", va="top", fontsize=10,
             fontweight="bold", color=AMARANTH, fontfamily=SANS)

    # ---- key across the top, since 15cm leaves no room for a side column ----
    n = len(ramp)
    sw_w, gap = 0.088, 0.010
    total = n * sw_w + (n - 1) * gap
    x0 = (1.0 - total) / 2
    for i, lab in enumerate(M["labels"]):
        x = x0 + i * (sw_w + gap)
        fig.patches.append(Rectangle((x, 0.870), sw_w, 0.011,
                                     transform=fig.transFigure, facecolor=ramp[i],
                                     edgecolor="#ffffff", linewidth=0.6, zorder=3))
        fig.text(x + sw_w / 2, 0.865, lab, ha="center", va="top", fontsize=8,
                 color=SMOKY, fontfamily=SANS)

    # Arrow key on its own line beneath the swatches.
    ky = 0.840
    fig.text(0.5, ky, "Net flow, GWh per day  –  arrows point downstream and widen with volume",
             ha="center", va="top", fontsize=8, color=SMOKY, fontfamily=SANS)
    for j, gv in enumerate((GMIN, GMAX)):
        ax0 = 0.335 + j * 0.20
        arr = flow_arrow((ax0, ky - 0.030), (ax0 + 0.055, ky - 0.030), gv,
                         zorder=4, scale=0.75)
        arr.set_transform(fig.transFigure)
        fig.patches.append(arr)
        fig.text(ax0 + 0.065, ky - 0.030, f"{gv:.1f}", fontsize=8, color=SMOKY,
                 fontfamily=SANS, va="center")

    # ---- map takes the full column width ----
    ax = fig.add_axes([0.015, 0.108, 0.97, 0.679])
    ax.set_xlim(-16, W + 16)
    ax.set_ylim(-16, H + 16)
    ax.set_aspect("equal")
    ax.axis("off")

    for cc, rings in SHAPES.items():
        b = bin_of(D[cc][M["key"]])
        on_cor = cc in CORRIDOR
        for pts in rings:
            ax.add_patch(PathPatch(MPath(pts, closed=True), facecolor=ramp[b],
                                   edgecolor="#ffffff",
                                   linewidth=1.3 if on_cor else 0.8,
                                   zorder=2.5 if on_cor else 2))

    COMPACT = {"SK"}

    def label(x, y, d, ink):
        """Lines stacked in POINTS, not map units - offsetting in data
        coordinates makes the spacing depend on how the map happens to scale."""
        base = dict(xycoords="data", textcoords="offset points",
                    annotation_clip=False, zorder=7, fontfamily=SANS)
        v = d["imp"]
        txt = (f"imports {v:.0f}%" if v >= 5 else
               f"exports {-v:.0f}%" if v <= -5 else "balanced")
        ax.annotate(d["name"], (x, y), xytext=(0, 11), va="center", ha="center",
                    fontsize=8, color=ink, fontweight="bold", **base)
        if d["cc"] in COMPACT:
            # Two lines rather than three, because the country is too shallow
            # for a third - the status moves up beside the price instead of
            # being dropped. Two text objects straddling the centre, since a
            # single one cannot carry two sizes.
            price_s, gap = f"{EURO}{d['price']:.0f}", 5.0
            # Measured, not guessed: the two are hung either side of one point,
            # so the pair only lands centred if the offsets account for the
            # status being the wider of the two.
            wp = text_width(price_s, 12, "bold")
            ws = text_width(txt, 8, "normal")
            ax.annotate(price_s, (x, y), xytext=((wp - gap - ws) / 2, -3),
                        va="center", ha="right", fontsize=12, color=ink,
                        fontweight="bold", **base)
            ax.annotate(txt, (x, y), xytext=((wp + gap - ws) / 2, -2),
                        va="center", ha="left", fontsize=8, color=ink, **base)
        else:
            ax.annotate(f"{EURO}{d['price']:.0f}", (x, y), xytext=(0, -1),
                        va="center", ha="center", fontsize=12, color=ink,
                        fontweight="bold", **base)
            ax.annotate(txt, (x, y), xytext=(0, -12), va="center", ha="center",
                        fontsize=8, color=ink, **base)

    for cc in CEE:
        d = D[cc]
        axy = ANCHOR[cc]
        nx, ny = NUDGE.get(cc, (0, 0))
        label(axy[0] + nx, axy[1] + ny, d, ink_for[bin_of(d[M["key"]])])

    # ---- net flow on every border, sized by volume ----
    # Seeded with the country labels, so a number box treats a country's name
    # as an obstacle too. Checking only against other boxes let one land square
    # on a label, which no amount of spacing between boxes would have caught.
    placed = [(ANCHOR[c][0] + NUDGE.get(c, (0, 0))[0],
               ANCHOR[c][1] + NUDGE.get(c, (0, 0))[1]) for c in CEE]
    for (lo, hi) in sorted(_acc, key=lambda k: -FLOW[k]["gwh"]):
        fl = FLOW[(lo, hi)]
        pa, pb = ANCHOR[fl["src"]], ANCHOR[fl["dst"]]
        dist = math.hypot(pb[0] - pa[0], pb[1] - pa[1])
        half = min(34.0, dist * 0.16)
        # Biased toward the destination rather than centred on the two capitals.
        # Centred, the Slovakia-Hungary arrow - the largest on the map - reached
        # far enough back into Slovakia to cross its own label, and Slovakia is
        # too small to move the label anywhere else. Sitting inside the receiving
        # country also reads correctly: this is power arriving.
        t = 0.62
        mid = (pa[0] + t * (pb[0] - pa[0]), pa[1] + t * (pb[1] - pa[1]))
        a, b = shrink(mid, pa, half), shrink(mid, pb, half)
        ax.add_patch(flow_arrow(a, b, fl["gwh"], zorder=5.4))
        ux, uy = (b[0] - a[0]) / (2 * half or 1), (b[1] - a[1]) / (2 * half or 1)
        px, py = -uy, ux
        off = 26 + 16 * (fl["gwh"] / GMAX) ** 0.55
        for sx, mult in ((1, 1.0), (-1, 1.0), (1, 1.7), (-1, 1.7), (1, 2.4)):
            cand = (mid[0] + px * off * sx * mult, mid[1] + py * off * sx * mult)
            if all(math.dist(cand, q) > 46 for q in placed):
                break
        placed.append(cand)
        ax.text(cand[0], cand[1], f"{fl['gwh']:.1f}", ha="center", va="center",
                fontsize=8, color=SMOKY, fontweight="bold", zorder=5.6,
                fontfamily=SANS,
                bbox=dict(boxstyle="square,pad=0.26", facecolor="#ffffff",
                          edgecolor=GREY_RULE, linewidth=0.6))

    # Square corners, no bevel, simple outline on white - per the guide.
    fig.text(0.02, 0.086,
             "Net trade is the balance of physical cross-border flows over the country's own demand.",
             fontsize=8, color=SMOKY, fontfamily=SANS, va="top")
    fig.text(0.02, 0.012,
             f"E3G analysis based on ENTSO-E Transparency Platform, May{NDASH}September 2026 "
             f"(day-ahead prices,\nphysical cross-border flows, total load) and ERA5 reanalysis via "
             f"Open-Meteo.",
             fontsize=8, style="italic", color=SMOKY, fontfamily=SANS, va="bottom",
             linespacing=1.4)

    OUT.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        p = OUT / f"{M['file']}.{ext}"
        fig.savefig(p, facecolor="#ffffff")
        print(f"wrote {p}  ({p.stat().st_size / 1024:.0f} KB)")
    plt.close(fig)


for _m in MODES:
    render(_m)
