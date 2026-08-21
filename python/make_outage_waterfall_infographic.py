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
         "wind": "Wind", "other_renewables": "Other", "hydro": "Hydro"}
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
    """Shares of the measured replacement, scaled to the nuclear loss.

    The raw components do not sum to the loss, and the difference is not a
    missing fuel: it is the energy-identity gap. Total measured supply fell
    3.8 GWh/day in Hungary while metered demand rose 2.7, because distributed
    solar counts as generation but never crosses the transmission load meter.
    Carrying that as its own bar mixed a metering artefact in with real sources,
    so the components are scaled instead to sum to the loss - each bar is that
    source's share of the replacement actually observed.
    """
    base = {k: period_mean(v, p["b0"], p["b1"]) for k, v in cat.items()}
    out = {k: period_mean(v, p["o0"], p["o1"]) for k, v in cat.items()}
    delta = {k: out.get(k, 0) - base.get(k, 0) for k in base}
    loss = -delta.get("nuclear", 0.0)          # positive size of the hole
    raw = [(k, delta.get(k, 0.0)) for k in ORDER if k in delta]
    net = sum(v for _, v in raw)
    parts = [(k, v / net * 100) for k, v in raw] if net else []
    return dict(base_nuclear=base.get("nuclear", 0), out_nuclear=out.get("nuclear", 0),
                loss=loss, parts=parts, raw=raw, net=net,
                # How far the measured components fell short before scaling,
                # reported in the notes rather than drawn as a fuel.
                closure=100.0 * net / loss if loss else 0.0,
                demand_delta=delta.get("demand", 0.0), base=base, out=out)


def draw(ax, d, title):
    # Biggest contributor first, anything negative last, so the bar that pulls
    # the total back down is the one the eye finishes on.
    parts = sorted(d["parts"], key=lambda kv: (kv[1] < 0, -kv[1]))

    # Bracketed by two full-height reference bars: what nuclear was providing,
    # and what the other sources put back. Between them the contributors build
    # from zero, so the chart reads as a rebuild rather than a deficit.
    cum = 0.0
    n = len(parts) + 2
    ax.bar(0, 100, bottom=0, width=0.62, color=LOSS, zorder=3)
    ax.text(0, -3.0, "-100%", ha="center", va="top", fontsize=9.6,
            fontweight="bold", color=LOSS, zorder=5)

    for i, (k, v) in enumerate(parts, start=1):
        a, b = min(cum, cum + v), max(cum, cum + v)
        ax.bar(i, b - a, bottom=a, width=0.62, color=COLOR[k], zorder=3)
        va = "bottom" if v >= 0 else "top"
        y = b + 2.6 if v >= 0 else a - 2.6
        # A 0.4% contribution formatted as "+0%" reads as an error.
        txt = "<1%" if abs(v) < 0.5 else f"{v:+.0f}%"
        ax.text(i, y, txt, ha="center", va=va, fontsize=9.6, fontweight="bold",
                color=COLOR[k], zorder=5)
        prev = cum
        cum += v
        ax.plot([i - 0.31, i + 0.31], [prev, prev], color="#9aa0ad",
                linewidth=0.9, linestyle=(0, (3, 2)), zorder=2) if i > 1 else None
        ax.plot([i + 0.31, i + 1 - 0.31], [cum, cum], color="#9aa0ad",
                linewidth=0.9, linestyle=(0, (3, 2)), zorder=2)

    ax.bar(n - 1, 100, bottom=0, width=0.62, color="#2b3446", zorder=3)
    ax.text(n - 1, 103.5, "100%", ha="center", va="bottom", fontsize=9.6,
            fontweight="bold", color="#2b3446", zorder=5)

    names = (["Nuclear\nlost"] + [LABEL[k] for k, _ in parts]
             + ["Replacement\ngenerated"])
    ax.set_xticks(range(n))
    ax.set_xticklabels(names, fontsize=8.4, color=MUTED)
    ax.set_xlim(-0.62, n - 0.38)
    ax.axhline(0, color="#5a6274", linewidth=1.1, zorder=4)
    ax.set_ylim(-14, 132)
    ax.set_yticks([0, 25, 50, 75, 100, 125])
    ax.set_yticklabels(["0%", "25%", "50%", "75%", "100%", "125%"])
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ("top", "right", "bottom", "left"):
        ax.spines[s].set_visible(False)
    ax.tick_params(labelsize=8.8, colors=MUTED, length=0, pad=3)

    ax.set_title(title, loc="left", fontsize=13.5, fontweight="bold",
                 color=INK, pad=22)
    ax.annotate(f"{d['base_nuclear']:.0f} {NDASH}> {d['out_nuclear']:.0f} GWh/day"
                f"   {MIDDOT}   a hole of {d['loss']:.0f} GWh/day",
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
             "What replaced the lost nuclear output, as a share of the reduction. Each country is measured against its own pre-outage plateau.",
             fontsize=10.2, color=MUTED, va="top")

    gs = fig.add_gridspec(1, 2, left=0.062, right=0.975, top=0.775, bottom=0.275,
                          wspace=0.14)
    for i, (cc, p) in enumerate(PERIODS.items()):
        if cc in res:
            draw(fig.add_subplot(gs[0, i]), res[cc], p["name"])

    hu, ro = res.get("HU"), res.get("RO")
    if hu and ro:
        hu_imp = dict(hu["parts"]).get("net_import", 0)
        hu_fos = dict(hu["parts"]).get("fossil", 0)
        ro_fos = dict(ro["parts"]).get("fossil", 0)
        ro_sol = dict(ro["parts"]).get("solar", 0)
        ro_wind = dict(ro["parts"]).get("wind", 0)
        fig.text(0.05, 0.192,
                 f"Hungary covered {hu_imp + hu_fos:.0f}% of its shortfall with imports and fossil generation and only "
                 f"{dict(hu['parts']).get('solar', 0):.0f}% with solar.",
                 fontsize=9.8, color=INK, va="top")
        fig.text(0.05, 0.166,
                 f"Romania leaned hardest on fossil ({ro_fos:.0f}%), with solar and wind together adding {ro_sol + ro_wind:.0f}% "
                 f"and hydro falling a further {abs(dict(ro['parts']).get('hydro', 0)):.0f}%.",
                 fontsize=9.8, color=INK, va="top")

    foot = [
        (0.140, f"Periods are taken from the daily series, not assumed. Hungary: 5{NDASH}27 July against 2{NDASH}20 August. Romania: 7{NDASH}27 July against 14{NDASH}20 August, the days its output read zero."),
        (0.113, f"Bars are shares of each country's own nuclear reduction, so the two panels are comparable despite Hungary losing 37 GWh/day and Romania 28."),
        (0.086, f"The closing bar is generation and trade, not metered demand: the measured components come to {hu['closure']:.0f}% of Hungary's nuclear loss and {ro['closure']:.0f}% of Romania's before scaling, and are scaled to 100%."),
        (0.059, f"The shortfall is a metering artefact rather than a missing fuel {MIDDOT} distributed solar counts as generation but never crosses the transmission load meter, and small units sit below ENTSO-E's reporting threshold."),
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
        print(f"{p['name']}: nuclear {d['base_nuclear']:.1f} -> {d['out_nuclear']:.1f} "
              f"(-{d['loss']:.1f} GWh/day), demand {d['demand_delta']:+.1f}")
        for k, v in d["parts"]:
            print(f"    {LABEL[k]:14} {v:+6.1f}%")
        print(f"    {'closure':14} {d['closure']:6.1f}% of the loss before scaling")
    fig = build(data)
    pdf, png = OUT / "nuclear_outage_waterfall.pdf", OUT / "nuclear_outage_waterfall.png"
    fig.savefig(pdf, format="pdf", facecolor=fig.get_facecolor())
    fig.savefig(png, format="png", dpi=200, facecolor=fig.get_facecolor())
    plt.close(fig)
    for p in (pdf, png):
        print(f"{p}  ({p.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
