"""Shareable infographic: hourly demand, generation and price during the late-July
2026 heatwave, built to the E3G visual guidelines (Comms, September 2025).

Writes PDF (vector, for the report) and PNG to python/output/.

Data comes from v_hw_event_profile and v_hw_event_price - each country's own
heatwave days inside 28 July - 12 August 2026, averaged by local hour.

Solar and storage sit at the TOP of the stack, with fossil directly beneath
them. The chart has one job - to show the handover from solar in the middle of
the day to gas in the evening - and putting the two against each other makes it
a single shape: the solar band swells and collapses, and the fossil band
underneath grows into the space it leaves.

Where the guidelines drove the design:

  Size      15cm column width is a hard ceiling, so the four countries are a
            2x2 grid rather than a row. At four across each panel would have
            been 3.5cm wide and the hourly shape unreadable.
  Title     Georgia bold 14pt Royal Blue, centred.
  Subtitle  Open Sans bold, all caps, 10pt, Amaranth - this is an energy
            narrative visual, and Amaranth is that narrative's lead colour.
  Colour    Every fill is from the E3G palette. Amaranth leads because fossil
            is the story; Royal Blue carries demand as the hero colour. The
            guide warns that Amaranth and Avocado converge under deuteranopia,
            so the six fills were checked in OKLab under simulated protanopia
            and deuteranopia before use, and separated until they passed.
  Stacks    White lines between segments, as the guide requires.
  Legend    The guide prefers direct labels and allows a legend where there is
            no room for them, as here: six categories across four small panels.
  Source    Open Sans italic 8pt Smoky Black, bottom left, naming the database
            and the access date rather than just the organisation.
  Caption   NOT in the image. The guide sets the caption as document text below
            the figure, so the takeaway line belongs in the report, not here.
"""
from __future__ import annotations

import json
import urllib.request
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "python" / "output"

CM = 1 / 2.54
COL_W_CM = 15.0          # E3G report column width; figures may be smaller, never wider
# The guide caps width, not height. At 8pt - its own size for secondary text -
# the labels take a fixed share of a 15cm-wide figure, so the only way to give
# eight small panels room to breathe is to make it taller.
FIG_H_CM = 20.5

COUNTRIES = [("FR", "France"), ("GR", "Greece"), ("HU", "Hungary"), ("RO", "Romania")]

# Bottom to top. Must-run and traded volume underneath; then fossil, then the
# two that define the day's shape - storage and solar - on top.
STACK = ["nuclear", "other_renewables", "net_import", "fossil", "storage", "solar"]
LABEL = {"nuclear": "Nuclear", "fossil": "Fossil", "other_renewables": "Other renewables",
         "solar": "Solar", "storage": "Storage", "net_import": "Net import"}

# --- E3G palette ---------------------------------------------------------
ROYAL, VISTA, AMARANTH, GOLD, SAGE = "#282970", "#82a4dd", "#cd3951", "#a69700", "#d1c688"
SMOKY, FLORAL, EMINENCE, GOLDENROD, AVOCADO = "#1b1817", "#fffaf0", "#6d1a81", "#deae2f", "#507514"
GOLD_TINT, RED_DEEP, GREY_RULE = "#ede8d1", "#852433", "#c4c2ba"

COLOR = {"solar": GOLDENROD, "storage": GOLD_TINT, "nuclear": EMINENCE,
         "other_renewables": AVOCADO, "fossil": AMARANTH, "net_import": VISTA}
DEMAND = ROYAL
PRICE = RED_DEEP

NDASH, MIDDOT = "–", "·"

TITLE_FONT = {"family": "Georgia", "weight": "bold", "size": 14, "color": ROYAL}
SANS = "Open Sans"


def load_env() -> dict:
    env = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def get(path: str):
    env = load_env()
    key = env["SUPABASE_ANON_KEY"]
    req = urllib.request.Request(f"{env['SUPABASE_URL']}/rest/v1/{path}", headers={
        "apikey": key, "Authorization": f"Bearer {key}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def fetch() -> tuple[dict, dict]:
    prof: dict = {}
    for x in get("v_hw_event_profile?select=*"):
        (prof.setdefault(x["country_code"], {})
             .setdefault(int(x["hour"]), {}))[x["category"]] = float(x["gwh"])
    price: dict = {}
    for x in get("v_hw_event_price?select=*"):
        price.setdefault(x["country_code"], {})[int(x["hour"])] = float(x["eur"])
    return prof, price


def draw_dispatch(ax, xs, series, title, note, show_ylabel):
    pos = [0.0] * len(xs)
    neg = [0.0] * len(xs)
    for key in STACK:
        vals = [series[h].get(key, 0.0) for h in xs]
        if all(abs(v) < 1e-9 for v in vals):
            continue
        # Positive and negative parts stack on separate piles, so a band that
        # changes sign tapers to zero at the crossing instead of leaping.
        upper = [max(v, 0.0) for v in vals]
        lower = [min(v, 0.0) for v in vals]
        # White edge between segments, per the guide - it also does the work of
        # separating Amaranth from Avocado, which converge under deuteranopia.
        edge = dict(edgecolor="#ffffff", linewidth=0.5)
        if any(v > 0 for v in upper):
            hi = [pos[i] + upper[i] for i in range(len(xs))]
            ax.fill_between(xs, pos, hi, facecolor=COLOR[key], zorder=2, **edge)
            pos = hi
        if any(v < 0 for v in lower):
            lo = [neg[i] + lower[i] for i in range(len(xs))]
            ax.fill_between(xs, lo, neg, facecolor=COLOR[key], zorder=2, **edge)
            neg = lo

    ax.plot(xs, [series[h].get("demand", 0.0) for h in xs],
            color=DEMAND, linewidth=1.5, zorder=4, solid_capstyle="round")
    ax.axhline(0, color=GREY_RULE, linewidth=0.7, zorder=3)

    ax.set_title(title, loc="left", fontsize=9, fontweight="bold", color=SMOKY,
                 fontfamily=SANS, pad=11)
    ax.annotate(note, xy=(0, 1.0), xycoords="axes fraction", xytext=(0, 2),
                textcoords="offset points", fontsize=8, color=SMOKY,
                fontfamily=SANS, va="bottom")
    if show_ylabel:
        ax.set_ylabel("GW", fontsize=8, color=SMOKY, fontfamily=SANS, labelpad=2)
    ax.set_xlim(0, 23)
    ax.set_xticks([0, 6, 12, 18])
    ax.set_xticklabels([])
    ax.grid(axis="y", color=GREY_RULE, linewidth=0.5, alpha=0.55, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right", "bottom", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8, colors=SMOKY, length=0, pad=2)
    for lb in ax.get_yticklabels():
        lb.set_fontfamily(SANS)


def draw_price(ax, xs, px, show_ylabel):
    hs = [h for h in xs if h in px]
    ax.plot(hs, [px[h] for h in hs], color=PRICE, linewidth=1.4,
            zorder=3, solid_capstyle="round")
    ax.fill_between(hs, 0, [px[h] for h in hs], color=PRICE, alpha=0.10, zorder=2)
    ax.set_xlim(0, 23)
    ax.set_xticks([0, 6, 12, 18])
    ax.set_xticklabels(["00", "06", "12", "18"], fontfamily=SANS)
    ax.set_ylim(bottom=0)
    if show_ylabel:
        ax.set_ylabel("€/MWh", fontsize=8, color=SMOKY, fontfamily=SANS, labelpad=2)
    ax.grid(axis="y", color=GREY_RULE, linewidth=0.5, alpha=0.55, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right", "bottom", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8, colors=SMOKY, length=0, pad=2)
    for lb in ax.get_yticklabels():
        lb.set_fontfamily(SANS)
    if hs:
        lo_h = min(hs, key=lambda h: px[h])
        hi_h = max(hs, key=lambda h: px[h])
        ax.annotate(f"{px[hi_h]:.0f}", xy=(hi_h, px[hi_h]), xytext=(0, 2),
                    textcoords="offset points", ha="center", fontsize=8,
                    fontweight="bold", color=PRICE, fontfamily=SANS)
        ax.annotate(f"{px[lo_h]:.0f}", xy=(lo_h, px[lo_h]), xytext=(0, -3),
                    textcoords="offset points", ha="center", va="top",
                    fontsize=8, color=PRICE, fontfamily=SANS)


def build(prof: dict, price: dict):
    plt.rcParams["font.family"] = [SANS]
    fig = plt.figure(figsize=(COL_W_CM * CM, FIG_H_CM * CM), dpi=300)
    fig.patch.set_facecolor("#ffffff")

    # Title: Georgia bold 14pt Royal Blue, centred.
    fig.text(0.5, 0.988, "Hourly generation and day-ahead price",
             ha="center", va="top", **TITLE_FONT)
    # Subtitle: Open Sans bold, all caps, 10pt, Amaranth for the energy narrative.
    fig.text(0.5, 0.958,
             f"DURING THE 28 JULY {NDASH} 12 AUGUST 2026 HEATWAVES",
             ha="center", va="top", fontsize=10, fontweight="bold",
             color=AMARANTH, fontfamily=SANS)

    present = {k for cc, _ in COUNTRIES for h in prof.get(cc, {}).values()
               for k in h if abs(h.get(k, 0.0)) > 1e-9}
    handles = [Line2D([], [], color=DEMAND, linewidth=1.5, label="Demand")]
    handles += [Patch(facecolor=COLOR[k], edgecolor="#ffffff", linewidth=0.5,
                      label=LABEL[k]) for k in reversed(STACK) if k in present]
    handles += [Line2D([], [], color=PRICE, linewidth=1.4, label="Day-ahead price")]
    # Eight entries will not fit one row inside 15cm at 8pt, so it is two rows
    # of four and the panels start below them.
    leg = fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 0.938),
                     ncol=4, frameon=False, fontsize=8, handlelength=1.4,
                     handleheight=0.85, columnspacing=1.0, labelspacing=0.5,
                     labelcolor=SMOKY)
    for t in leg.get_texts():
        t.set_fontfamily(SANS)

    # Two countries per row; a spacer row keeps the pairs visually grouped.
    gs = fig.add_gridspec(5, 2, left=0.115, right=0.985, top=0.828, bottom=0.130,
                          hspace=0.16, wspace=0.26,
                          height_ratios=[3.0, 1.15, 0.55, 3.0, 1.15])

    for i, (cc, name) in enumerate(COUNTRIES):
        if cc not in prof:
            continue
        series = prof[cc]
        xs = sorted(series)
        peak = max(series[h].get("demand", 0.0) for h in xs)
        row, col = (0 if i < 2 else 3), i % 2
        ax = fig.add_subplot(gs[row, col])
        # GW, not GWh: the series is an average over the hour, which is a rate.
        draw_dispatch(ax, xs, series, name, f"peak demand {peak:.1f} GW",
                      show_ylabel=(col == 0))
        axp = fig.add_subplot(gs[row + 1, col])
        draw_price(axp, xs, price.get(cc, {}), show_ylabel=(col == 0))

    # Wrapped by hand to the 15cm column: at 8pt roughly 100 characters fit, and
    # matplotlib will not wrap for us - it would simply run off the edge.
    notes = [
        "Hours are local; each panel has its own scale. Generation stacks above zero, net exports below it.",
        "Greece's exports are understated: ENTSO-E reports few of its Albanian and Italian flow hours.",
    ]
    y = 0.098
    for txt in notes:
        fig.text(0.02, y, txt, fontsize=8, color=SMOKY, fontfamily=SANS, va="top")
        y -= 0.019

    # Source: Open Sans italic, 8pt, Smoky Black, bottom left, aligned to the
    # left edge. Names the database and when it was read, not just the body.
    fig.text(0.02, 0.012,
             "Source: ENTSO-E Transparency Platform, accessed August 2026 (generation per production "
             "type, total load,\nphysical cross-border flows, day-ahead prices); ERA5 via Open-Meteo, "
             "August 2026. E3G analysis.",
             fontsize=8, style="italic", color=SMOKY, fontfamily=SANS, va="bottom",
             linespacing=1.4)
    return fig


def main() -> int:
    prof, price = fetch()
    for cc, name in COUNTRIES:
        px = price.get(cc, {})
        if px:
            lo = min(px, key=lambda h: px[h])
            hi = max(px, key=lambda h: px[h])
            print(f"{name}: price {px[lo]:.0f} at {lo}h -> {px[hi]:.0f} at {hi}h "
                  f"({px[hi] / max(px[lo], 1):.1f}x)")
    fig = build(prof, price)
    OUT.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        p = OUT / f"heatwave_july_august_2026.{ext}"
        fig.savefig(p, facecolor="#ffffff")
        print(f"{p}  ({p.stat().st_size / 1024:.0f} KB)")
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
