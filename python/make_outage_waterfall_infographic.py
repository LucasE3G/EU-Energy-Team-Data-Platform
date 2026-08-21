"""Shareable infographic: what replaced the lost nuclear output in Hungary and
Romania, as a waterfall.

Both fleets stepped down inside the summer window, so each country is compared
against its own stable pre-outage plateau rather than a seasonal average:

    Hungary   5-27 July (44.0 GWh/day)  ->  2-20 August (7.3)
    Romania   7-27 July (28.1 GWh/day)  -> 14-20 August (0.0)

Everything is normalised to that country's nuclear loss, so the bars answer one
question: of the hole nuclear left, what share did each source cover?

The final bar is what the components do NOT explain, and it is shown rather
than absorbed into the others - forcing a waterfall to close is how a chart
starts lying.

Writes PDF and PNG to python/output/.
"""
from __future__ import annotations

import json
import urllib.request
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "python" / "output"

# Periods read off the daily series, not assumed: each country's stable
# plateau before the step, and the flat bottom after it.
PERIODS = {
    "HU": dict(name="Hungary", b0="2026-07-05", b1="2026-07-27",
               o0="2026-08-02", o1="2026-08-20"),
    "RO": dict(name="Romania", b0="2026-07-07", b1="2026-07-27",
               o0="2026-08-14", o1="2026-08-20"),
}
ORDER = ["net_import", "fossil", "solar", "wind", "other_renewables", "hydro"]
LABEL = {"net_import": "Imports", "fossil": "Fossil", "solar": "Solar",
         "wind": "Wind", "other_renewables": "Other", "hydro": "Hydro",
         "nuclear": "Nuclear"}
COLOR = {"net_import": "#2e7fb8", "fossil": "#9a9a93", "solar": "#e8a33d",
         "wind": "#5b9bd5", "other_renewables": "#7fbf9a", "hydro": "#1f9e8f"}
LOSS = "#c0392b"
UNEXP = "#c9ccd4"
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


def fetch() -> dict:
    env = load_env()
    key = env["SUPABASE_ANON_KEY"]
    url = (f"{env['SUPABASE_URL']}/rest/v1/v_outage_daily"
           f"?select=*&order=date.asc&limit=20000")
    req = urllib.request.Request(url, headers={
        "apikey": key, "Authorization": f"Bearer {key}"})
    with urllib.request.urlopen(req, timeout=90) as r:
        rows = json.loads(r.read())
    out: dict = {}
    for x in rows:
        (out.setdefault(x["country_code"], {})
            .setdefault(x["category"], {}))[x["date"]] = float(x["gwh"])
    return out


def period_mean(series: dict, d0: str, d1: str):
    vals = [v for d, v in series.items() if d0 <= d <= d1]
    return sum(vals) / len(vals) if vals else 0.0


def decompose(cat: dict, p: dict):
    """Losses and gains as shares of the total generation lost.

    Everything that fell is a loss, everything that rose is a gain, and the two
    sides are each normalised to 100% so the waterfall closes. That scaling is
    real but small: the measured gains come to 90% of Hungary's losses and 99%
    of Romania's before it is applied.

    The shortfall is not a missing fuel. It is the energy-identity gap -
    distributed solar counts as generation but never crosses the transmission
    load meter - which is why it is stated in the notes instead of drawn as a
    bar that would imply a source.
    """
    base = {k: period_mean(v, p["b0"], p["b1"]) for k, v in cat.items()}
    out = {k: period_mean(v, p["o0"], p["o1"]) for k, v in cat.items()}
    delta = {k: out.get(k, 0) - base.get(k, 0) for k in base}

    comps = ["nuclear"] + ORDER
    losses_raw = [(k, delta[k]) for k in comps if k in delta and delta[k] < -0.02]
    gains_raw = [(k, delta[k]) for k in comps if k in delta and delta[k] > 0.02]
    lost = -sum(v for _, v in losses_raw)
    gained = sum(v for _, v in gains_raw)

    # Nuclear leads the losses; the rest by size. Gains all by size.
    losses_raw.sort(key=lambda kv: (kv[0] != "nuclear", kv[1]))
    gains_raw.sort(key=lambda kv: -kv[1])

    # v is already negative here; dividing by the positive total keeps it that
    # way, so the bars walk the balance downward.
    losses = [(k, v / lost * 100) for k, v in losses_raw] if lost else []
    gains = [(k, v / gained * 100) for k, v in gains_raw] if gained else []

    return dict(base_nuclear=base.get("nuclear", 0), out_nuclear=out.get("nuclear", 0),
                loss=-delta.get("nuclear", 0.0), lost=lost, gained=gained,
                losses=losses, gains=gains,
                closure=100.0 * gained / lost if lost else 0.0,
                demand_delta=delta.get("demand", 0.0), base=base, out=out)


def draw(ax, d, title):
    # Losses walk the balance down from zero, gains walk it back up, and the
    # last bar sits at zero: the hole, then what filled it.
    bars = d["losses"] + d["gains"]
    cum = 0.0
    for i, (k, v) in enumerate(bars):
        a, b = min(cum, cum + v), max(cum, cum + v)
        col = LOSS if k == "nuclear" else COLOR[k]
        ax.bar(i, b - a, bottom=a, width=0.62, color=col, zorder=3)
        # Label outside the bar on the side it is travelling.
        va = "top" if v < 0 else "bottom"
        y = a - 2.6 if v < 0 else b + 2.6
        txt = "<1%" if abs(v) < 0.5 else f"{v:+.0f}%"
        ax.text(i, y, txt, ha="center", va=va, fontsize=9.5, fontweight="bold",
                color=col, zorder=5)
        prev = cum
        cum += v
        if i < len(bars) - 1:
            ax.plot([i + 0.31, i + 1 - 0.31], [cum, cum], color="#9aa0ad",
                    linewidth=0.9, linestyle=(0, (3, 2)), zorder=2)

    # Closing bar spans the full depth of the hole, mirroring the losses on the
    # left: everything that was lost was served.
    n = len(bars) + 1
    ax.bar(n - 1, 100.0, bottom=-100.0, width=0.62, color="#2b3446", zorder=3)
    ax.text(n - 1, 2.6, "100%", ha="center", va="bottom", fontsize=9.5,
            fontweight="bold", color="#2b3446", zorder=5)

    names = ([f"{LABEL[k]}\nlost" for k, _ in d["losses"]]
             + [LABEL[k] for k, _ in d["gains"]] + ["Heatwave\ndemand"])
    ax.set_xticks(range(n))
    ax.set_xticklabels(names, fontsize=8.3, color=MUTED)
    ax.set_xlim(-0.62, n - 0.38)
    ax.axhline(0, color="#5a6274", linewidth=1.1, zorder=4)
    ax.set_ylim(-124, 30)
    ax.set_yticks([-100, -75, -50, -25, 0, 25])
    ax.set_yticklabels(["-100%", "-75%", "-50%", "-25%", "0%", "+25%"])
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right", "bottom", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8.8, colors=MUTED, length=0, pad=3)

    ax.set_title(title, loc="left", fontsize=13.5, fontweight="bold",
                 color=INK, pad=22)
    ax.annotate(f"nuclear {d['base_nuclear']:.0f} {NDASH}> {d['out_nuclear']:.0f} GWh/day"
                f"   {MIDDOT}   {d['lost']:.0f} GWh/day lost in all",
                xy=(0, 1.0), xycoords="axes fraction", xytext=(0, 5),
                textcoords="offset points", fontsize=9, color=MUTED, va="bottom")


def build(data: dict):
    plt.rcParams["font.family"] = ["DejaVu Sans"]
    fig = plt.figure(figsize=(12.4, 7.0), dpi=200)
    fig.patch.set_facecolor("#ffffff")

    res = {cc: decompose(data[cc], p) for cc, p in PERIODS.items() if cc in data}

    fig.text(0.05, 0.955, "Hungary bought its way through the outage; Romania burned through it",
             fontsize=18, fontweight="bold", color=INK, va="top")
    fig.text(0.05, 0.905,
             "Everything that fell, then everything that rose to cover it, as shares of each country's total generation loss. Measured against its own pre-outage plateau.",
             fontsize=10.2, color=MUTED, va="top")

    gs = fig.add_gridspec(1, 2, left=0.062, right=0.975, top=0.775, bottom=0.275,
                          wspace=0.14)
    for i, (cc, p) in enumerate(PERIODS.items()):
        if cc in res:
            draw(fig.add_subplot(gs[0, i]), res[cc], p["name"])

    hu, ro = res.get("HU"), res.get("RO")
    if hu and ro:
        hu_g = dict(hu["gains"])
        ro_g, ro_l = dict(ro["gains"]), dict(ro["losses"])
        fig.text(0.05, 0.192,
                 f"Hungary covered {hu_g.get('net_import', 0) + hu_g.get('fossil', 0):.0f}% of its shortfall with imports and fossil generation "
                 f"and only {hu_g.get('solar', 0):.0f}% with solar.",
                 fontsize=9.8, color=INK, va="top")
        fig.text(0.05, 0.166,
                 f"Romania lost hydro as well as nuclear {MIDDOT} {abs(ro_l.get('hydro', 0)):.0f}% of its total shortfall {MIDDOT} and refilled it mostly with fossil "
                 f"({ro_g.get('fossil', 0):.0f}%), solar and wind adding {ro_g.get('solar', 0) + ro_g.get('wind', 0):.0f}%.",
                 fontsize=9.8, color=INK, va="top")

    foot = [
        (0.140, f"Periods are taken from the daily series, not assumed. Hungary: 5{NDASH}27 July against 2{NDASH}20 August. Romania: 7{NDASH}27 July against 14{NDASH}20 August, the days its output read zero."),
        (0.113, f"Bars to the left of zero are everything that fell, bars to the right everything that rose. Each side is shown as a share of that country's total generation loss: "
                f"{hu['lost']:.0f} GWh/day in Hungary, {ro['lost']:.0f} in Romania."),
        (0.086, f"The closing bar is generation and trade rather than metered demand. The measured gains come to {hu['closure']:.0f}% of Hungary's losses and {ro['closure']:.0f}% of Romania's before being scaled to close;"),
        (0.059, f"the shortfall is a metering artefact, not a missing fuel {MIDDOT} distributed solar counts as generation but never crosses the transmission load meter, and small units sit below ENTSO-E's threshold."),
        (0.018, f"Source: ENTSO-E Transparency Platform (generation per production type, cross-border physical flows)"),
    ]
    for y, txt in foot:
        fig.text(0.05, y, txt, fontsize=7.7, color=MUTED, va="top")

    fig.add_artist(Line2D([0.05, 0.28], [0.982, 0.982], color=LOSS,
                          linewidth=3.4, solid_capstyle="butt"))
    return fig


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    data = fetch()
    for cc, p in PERIODS.items():
        if cc not in data:
            continue
        d = decompose(data[cc], p)
        print(f"{p['name']}: nuclear {d['base_nuclear']:.1f} -> {d['out_nuclear']:.1f}, "
              f"total lost {d['lost']:.1f} GWh/day, demand {d['demand_delta']:+.1f}")
        for k, v in d["losses"]:
            print(f"    LOSS {LABEL[k]:12} {v:+6.1f}%")
        for k, v in d["gains"]:
            print(f"    GAIN {LABEL[k]:12} {v:+6.1f}%")
        print(f"    closure {d['closure']:.1f}% of losses before scaling")
    fig = build(data)
    pdf, png = OUT / "nuclear_outage_waterfall.pdf", OUT / "nuclear_outage_waterfall.png"
    fig.savefig(pdf, format="pdf", facecolor=fig.get_facecolor())
    fig.savefig(png, format="png", dpi=200, facecolor=fig.get_facecolor())
    plt.close(fig)
    for p in (pdf, png):
        print(f"{p}  ({p.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
