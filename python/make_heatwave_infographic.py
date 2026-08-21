"""Shareable infographic: hourly demand, generation and price during the late-July
2026 heatwave, in the Ember small-multiples format.

Writes PDF (vector, for decks and print) and PNG (for chat and slides) to
python/output/.

Data comes from v_hw_event_profile and v_hw_event_price - each country's own
heatwave days inside 28 July - 12 August 2026, averaged by local hour.

Fossil sits at the TOP of the stack, not the bottom. It is the marginal unit:
what is dispatched last, sets the price, and fills whatever the rest cannot.
Stacking it underneath buried the one band whose shape explains the price panel.
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

# Italy dropped at request; these four keep the panels legible at this width.
COUNTRIES = [("FR", "France"), ("GR", "Greece"), ("HU", "Hungary"), ("RO", "Romania")]
HW_DAYS = {"GR": 16, "HU": 14, "RO": 13, "FR": 8}

# Bottom to top, in merit order: must-run first, the marginal unit last.
STACK = ["nuclear", "other_renewables", "solar", "storage", "net_import", "fossil"]
LABEL = {"nuclear": "Nuclear", "fossil": "Fossil", "other_renewables": "Other renewables",
         "solar": "Solar", "storage": "Storage", "net_import": "Net import"}
COLOR = {"solar": "#22c55e", "storage": "#f5c542", "nuclear": "#6b74a8",
         "other_renewables": "#a5e8e0", "fossil": "#d94f3d", "net_import": "#7cc3ea"}
DEMAND = "#12315e"
PRICE = "#c0392b"
INK, MUTED, GRID = "#14203a", "#6b7280", "#e6e8ec"

NDASH, MIDDOT = "–", "·"


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


def draw_dispatch(ax, xs, series, title, note):
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
        if any(v > 0 for v in upper):
            hi = [pos[i] + upper[i] for i in range(len(xs))]
            ax.fill_between(xs, pos, hi, facecolor=COLOR[key], linewidth=0, zorder=2)
            pos = hi
        if any(v < 0 for v in lower):
            lo = [neg[i] + lower[i] for i in range(len(xs))]
            ax.fill_between(xs, lo, neg, facecolor=COLOR[key], linewidth=0, zorder=2)
            neg = lo

    ax.plot(xs, [series[h].get("demand", 0.0) for h in xs],
            color=DEMAND, linewidth=2.2, zorder=4, solid_capstyle="round")
    ax.axhline(0, color="#9aa3b2", linewidth=0.9, zorder=3)

    ax.set_title(title, loc="left", fontsize=12, fontweight="bold", color=INK, pad=19)
    ax.annotate(note, xy=(0, 1.0), xycoords="axes fraction", xytext=(0, 4),
                textcoords="offset points", fontsize=8.2, color=MUTED, va="bottom")
    ax.set_xlim(0, 23)
    ax.set_xticks([0, 6, 12, 18])
    ax.set_xticklabels([])
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right", "bottom", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8.4, colors=MUTED, length=0, pad=3)


def draw_price(ax, xs, px):
    hs = [h for h in xs if h in px]
    ax.plot(hs, [px[h] for h in hs], color=PRICE, linewidth=2.0,
            zorder=3, solid_capstyle="round")
    ax.fill_between(hs, 0, [px[h] for h in hs], color=PRICE, alpha=0.10, zorder=2)
    ax.set_xlim(0, 23)
    ax.set_xticks([0, 6, 12, 18])
    ax.set_xticklabels(["00", "06", "12", "18"])
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right", "bottom", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8.2, colors=MUTED, length=0, pad=3)
    if hs:
        lo_h = min(hs, key=lambda h: px[h])
        hi_h = max(hs, key=lambda h: px[h])
        ax.annotate(f"{px[hi_h]:.0f}", xy=(hi_h, px[hi_h]), xytext=(0, 3),
                    textcoords="offset points", ha="center", fontsize=8.4,
                    fontweight="bold", color=PRICE)
        ax.annotate(f"{px[lo_h]:.0f}", xy=(lo_h, px[lo_h]), xytext=(0, -4),
                    textcoords="offset points", ha="center", va="top",
                    fontsize=8.4, color=PRICE)


def build(prof: dict, price: dict):
    plt.rcParams["font.family"] = ["DejaVu Sans"]
    fig = plt.figure(figsize=(13.2, 8.0), dpi=200)
    fig.patch.set_facecolor("#ffffff")

    fig.text(0.045, 0.962, "Solar carried the middle of the day; gas carried the evening,",
             fontsize=18, fontweight="bold", color=INK, va="top")
    fig.text(0.045, 0.918, "and the price followed it",
             fontsize=18, fontweight="bold", color=INK, va="top")
    fig.text(0.045, 0.878,
             f"Average hourly generation and day-ahead price during the 28 July {NDASH} 12 August 2026 heatwave",
             fontsize=10.2, color=MUTED, va="top")

    present = {k for cc, _ in COUNTRIES for h in prof.get(cc, {}).values()
               for k in h if abs(h.get(k, 0.0)) > 1e-9}
    handles = [Line2D([], [], color=DEMAND, linewidth=2.2, label="Demand")]
    handles += [Patch(facecolor=COLOR[k], label=LABEL[k])
                for k in reversed(STACK) if k in present]
    handles += [Line2D([], [], color=PRICE, linewidth=2.0, label="Day-ahead price")]
    fig.legend(handles=handles, loc="upper left", bbox_to_anchor=(0.042, 0.845),
               ncol=8, frameon=False, fontsize=9, handlelength=1.5,
               handleheight=0.9, columnspacing=1.3, labelcolor=INK)

    gs = fig.add_gridspec(2, 4, left=0.045, right=0.985, top=0.745, bottom=0.205,
                          hspace=0.10, wspace=0.19, height_ratios=[3.0, 1.15])

    for i, (cc, name) in enumerate(COUNTRIES):
        if cc not in prof:
            continue
        series = prof[cc]
        xs = sorted(series)
        peak = max(series[h].get("demand", 0.0) for h in xs)
        ax = fig.add_subplot(gs[0, i])
        draw_dispatch(ax, xs, series, name,
                      f"peak {peak:.1f} GWh  {MIDDOT}  {HW_DAYS.get(cc, 0)} heatwave days")
        if i == 0:
            ax.set_ylabel("GWh per hour", fontsize=8.6, color=MUTED, labelpad=4)
        axp = fig.add_subplot(gs[1, i])
        draw_price(axp, xs, price.get(cc, {}))
        if i == 0:
            axp.set_ylabel("€/MWh", fontsize=8.6, color=MUTED, labelpad=4)

    foot = [
        (0.166, f"Fossil is drawn at the top of the stack because it is the marginal unit: dispatched last, and the one that sets the price. The price panel is the same hours and the same days."),
        (0.140, f"Hours are local, so Greece and Romania (EEST) line up with France and Hungary (CEST). Each panel has its own scale {MIDDOT} France peaks near 47 GWh, Greece near 8."),
        (0.114, f"Generation stacks above zero and net exports fall below it, which is why France, an exporter throughout, carries a band beneath the axis."),
        (0.088, f"Storage is discharge only: ENTSO-E publishes pumping separately, and Hungary and Romania report no pumped storage at all. Demand and generation do not close exactly {MIDDOT}"),
        (0.062, f"distributed solar counts as generation but never crosses the transmission load meter, and Greece also trades with Albania, North Macedonia and Turkey, outside ENTSO-E's data."),
        (0.030, f"Source: ENTSO-E Transparency Platform (generation, load, cross-border flows, day-ahead prices) {MIDDOT} ERA5 via Open-Meteo (temperature)"),
    ]
    for y, txt in foot:
        fig.text(0.045, y, txt, fontsize=7.5, color=MUTED, va="top")

    fig.add_artist(Line2D([0.045, 0.26], [0.985, 0.985], color="#22c55e",
                          linewidth=3.4, solid_capstyle="butt"))
    return fig


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    prof, price = fetch()
    for cc, name in COUNTRIES:
        px = price.get(cc, {})
        if px:
            lo = min(px, key=lambda h: px[h])
            hi = max(px, key=lambda h: px[h])
            print(f"{name}: price {px[lo]:.0f} at {lo:02d}h -> {px[hi]:.0f} at {hi:02d}h "
                  f"({px[hi]/max(px[lo], 0.01):.1f}x)")
    fig = build(prof, price)
    pdf = OUT / "heatwave_july_august_2026.pdf"
    png = OUT / "heatwave_july_august_2026.png"
    fig.savefig(pdf, format="pdf", facecolor=fig.get_facecolor())
    fig.savefig(png, format="png", dpi=200, facecolor=fig.get_facecolor())
    plt.close(fig)
    for p in (pdf, png):
        print(f"{p}  ({p.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
