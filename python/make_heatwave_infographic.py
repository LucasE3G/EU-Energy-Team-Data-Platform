"""Shareable infographic: hourly demand and generation during the late-July 2026
heatwave, in the Ember small-multiples format.

Writes PDF (vector, for decks and print) and PNG (for chat and slides) to
python/output/.

Data comes from v_hw_event_profile - each country's own heatwave days inside
28 July - 12 August 2026, averaged by local hour.
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

# Panel order: biggest system first, so the eye starts where the megawatts are.
COUNTRIES = [("IT", "Italy"), ("FR", "France"), ("GR", "Greece"),
             ("HU", "Hungary"), ("RO", "Romania")]
HW_DAYS = {"IT": 16, "GR": 16, "HU": 14, "RO": 13, "FR": 8}

# Stack order, bottom to top. Nuclear and fossil form the always-on base; solar
# rides above them where its midday bulge is legible.
STACK = ["nuclear", "fossil", "other_renewables", "solar", "storage", "net_import"]
LABEL = {"nuclear": "Nuclear", "fossil": "Fossil", "other_renewables": "Other renewables",
         "solar": "Solar", "storage": "Storage", "net_import": "Net import"}
COLOR = {"solar": "#22c55e", "storage": "#f5c542", "nuclear": "#6b74a8",
         "other_renewables": "#a5e8e0", "fossil": "#c4c4c4", "net_import": "#7cc3ea"}
DEMAND = "#12315e"
INK, MUTED, GRID = "#14203a", "#6b7280", "#e6e8ec"

DASH = "—"      # em dash
NDASH = "–"     # en dash
DEG = "°"
MIDDOT = "·"
RSQUO = "’"


def load_env() -> dict:
    env = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def fetch() -> dict:
    env = load_env()
    url = f"{env['SUPABASE_URL']}/rest/v1/v_hw_event_profile?select=*"
    key = env["SUPABASE_ANON_KEY"]
    req = urllib.request.Request(url, headers={"apikey": key,
                                               "Authorization": f"Bearer {key}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        rows = json.loads(r.read())
    out: dict = {}
    for x in rows:
        (out.setdefault(x["country_code"], {})
            .setdefault(int(x["hour"]), {}))[x["category"]] = float(x["gwh"])
    return out


def draw_panel(ax, xs, series, title, note):
    pos = [0.0] * len(xs)
    neg = [0.0] * len(xs)

    for key in STACK:
        vals = [series[h].get(key, 0.0) for h in xs]
        if all(abs(v) < 1e-9 for v in vals):
            continue
        # Split each series into positive and negative parts and stack them on
        # separate piles. Assigning the whole value to one side makes a band
        # that changes sign leap from the top of the stack to below zero in a
        # single step - Romania's net imports dip negative at midday and drew a
        # vertical sliver through the panel. Split, both parts taper to zero at
        # the crossing and the transition is continuous.
        upper = [max(v, 0.0) for v in vals]
        lower = [min(v, 0.0) for v in vals]
        if any(v > 0 for v in upper):
            hi = [pos[i] + upper[i] for i in range(len(xs))]
            ax.fill_between(xs, pos, hi, facecolor=COLOR[key], linewidth=0, zorder=2)
            pos = hi
        if any(v < 0 for v in lower):
            lo = [neg[i] + lower[i] for i in range(len(xs))]
            ax.fill_between(xs, lo, neg, facecolor=COLOR[key], linewidth=0, zorder=2)
            neg = lo

    ax.plot(xs, [series[h].get("demand", 0.0) for h in xs],
            color=DEMAND, linewidth=2.1, zorder=4, solid_capstyle="round")
    ax.axhline(0, color="#9aa3b2", linewidth=0.9, zorder=3)

    # Title sits clear of the note beneath it; at a small pad the two collided.
    ax.set_title(title, loc="left", fontsize=12, fontweight="bold", color=INK, pad=21)
    ax.annotate(note, xy=(0, 1.0), xycoords="axes fraction",
                xytext=(0, 5), textcoords="offset points",
                fontsize=8.3, color=MUTED, va="bottom")

    ax.set_xlim(0, 23)
    ax.set_xticks([0, 6, 12, 18])
    ax.set_xticklabels(["00", "06", "12", "18"])
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right", "bottom", "left"):
        ax.spines[side].set_visible(False)
    ax.tick_params(labelsize=8.6, colors=MUTED, length=0, pad=4)


def build(data: dict):
    plt.rcParams["font.family"] = ["DejaVu Sans"]
    fig = plt.figure(figsize=(11.2, 8.6), dpi=200)
    fig.patch.set_facecolor("#ffffff")

    fig.text(0.055, 0.958, "Solar carried the middle of the day, but gas and imports",
             fontsize=19, fontweight="bold", color=INK, va="top")
    fig.text(0.055, 0.916, "carried the evening peak",
             fontsize=19, fontweight="bold", color=INK, va="top")
    fig.text(0.055, 0.879,
             f"Average hourly demand and generation by source during the 28 July {NDASH} 12 August 2026 heatwave (GWh)",
             fontsize=10.4, color=MUTED, va="top")

    handles = [Line2D([], [], color=DEMAND, linewidth=2.2, label="Demand")]
    handles += [Patch(facecolor=COLOR[k], label=LABEL[k]) for k in reversed(STACK)]
    fig.legend(handles=handles, loc="upper left", bbox_to_anchor=(0.052, 0.852),
               ncol=7, frameon=False, fontsize=9.2, handlelength=1.5,
               handleheight=0.9, columnspacing=1.5, labelcolor=INK)

    gs = fig.add_gridspec(2, 3, left=0.055, right=0.975, top=0.735, bottom=0.190,
                          hspace=0.58, wspace=0.24)

    for i, (cc, name) in enumerate(COUNTRIES):
        if cc not in data:
            continue
        ax = fig.add_subplot(gs[i // 3, i % 3])
        series = data[cc]
        xs = sorted(series)
        peak = max(series[h].get("demand", 0.0) for h in xs)
        draw_panel(ax, xs, series, name, f"peak {peak:.1f} GWh")

    # Sixth cell carries the reading notes rather than sitting empty. Kept
    # short: the long-form caveats live in the footer, where there is width.
    ax = fig.add_subplot(gs[1, 2])
    ax.axis("off")
    ax.text(0, 1.04, "How to read", fontsize=11, fontweight="bold", color=INK, va="top")
    # Seven lines is what the cell holds at this size; ten overran into the
    # footer. The detail that was cut lives in the footnotes below.
    ax.text(0, 0.88,
            "Generation stacks above zero,\n"
            "net exports below it. The dark\n"
            "line is demand.\n\n"
            "Hours are local (EEST in Greece\n"
            "and Romania, CEST elsewhere).\n\n"
            "Panels have independent scales.",
            fontsize=8.9, color=MUTED, va="top", linespacing=1.65)

    # Each line is kept under ~155 characters so none runs past the right edge.
    foot = [
        (0.150, f"Heatwave days: 3 or more consecutive days above the local 90th-percentile daily maximum (2014{NDASH}2025 baseline), or above 30 {DEG}C."),
        # The per-country day counts moved here off the panel headings, so the
        # uneven sample is still disclosed without cluttering the charts.
        (0.128, f"Days in this window: Italy 16, Greece 16, Hungary 14, Romania 13, France 8."),
        (0.102, f"Storage is discharge only (ENTSO-E publishes pumping separately), so the band never dips below zero; Hungary and Romania report no pumped storage."),
        (0.080, f"Demand and generation do not close exactly: distributed solar counts as generation but never crosses the load meter; Greece also trades outside ENTSO-E."),
        (0.046, f"Source: ENTSO-E Transparency Platform (generation, load, cross-border flows) {MIDDOT} ERA5 via Open-Meteo, population-weighted (temperature)"),
    ]
    for y, txt in foot:
        fig.text(0.055, y, txt, fontsize=7.7, color=MUTED, va="top")

    fig.add_artist(Line2D([0.055, 0.30], [0.984, 0.984], color="#22c55e",
                          linewidth=3.4, solid_capstyle="butt"))
    return fig


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    data = fetch()
    fig = build(data)
    pdf = OUT / "heatwave_july_august_2026.pdf"
    png = OUT / "heatwave_july_august_2026.png"
    fig.savefig(pdf, format="pdf", facecolor=fig.get_facecolor())
    fig.savefig(png, format="png", dpi=200, facecolor=fig.get_facecolor())
    plt.close(fig)
    for p in (pdf, png):
        print(f"{p}  ({p.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
