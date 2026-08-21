"""Shareable infographic: average hourly demand and generation by source in
Hungary and Romania, 1 July - 15 August 2026.

Deliberately the same window as make_nuclear_summer_infographic.py, so the two
can be read together: that chart shows both fleets collapsing inside this
window, and this one shows what covered the hours they used to cover.

Every day in the window is included, not only heatwave days, so the dates match
the nuclear charts exactly.

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
from matplotlib.patches import Patch

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "python" / "output"

COUNTRIES = [("HU", "Hungary"), ("RO", "Romania")]

STACK = ["nuclear", "fossil", "other_renewables", "solar", "storage", "net_import"]
LABEL = {"nuclear": "Nuclear", "fossil": "Fossil", "other_renewables": "Other renewables",
         "solar": "Solar", "storage": "Storage", "net_import": "Net import"}
COLOR = {"solar": "#22c55e", "storage": "#f5c542", "nuclear": "#6b74a8",
         "other_renewables": "#a5e8e0", "fossil": "#c4c4c4", "net_import": "#7cc3ea"}
DEMAND = "#12315e"
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
    codes = ",".join(cc for cc, _ in COUNTRIES)
    url = (f"{env['SUPABASE_URL']}/rest/v1/v_hw_window_profile"
           f"?select=*&country_code=in.({codes})&order=hour.asc")
    req = urllib.request.Request(url, headers={
        "apikey": key, "Authorization": f"Bearer {key}"})
    with urllib.request.urlopen(req, timeout=90) as r:
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
        # Positive and negative parts stack on separate piles: a band that
        # changes sign would otherwise leap from the top of the stack to below
        # zero in one step and draw a vertical sliver through the panel.
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
            color=DEMAND, linewidth=2.4, zorder=4, solid_capstyle="round")
    ax.axhline(0, color="#9aa3b2", linewidth=0.9, zorder=3)

    ax.set_title(title, loc="left", fontsize=13, fontweight="bold", color=INK, pad=21)
    ax.annotate(note, xy=(0, 1.0), xycoords="axes fraction",
                xytext=(0, 5), textcoords="offset points",
                fontsize=8.8, color=MUTED, va="bottom")

    ax.set_xlim(0, 23)
    ax.set_xticks([0, 3, 6, 9, 12, 15, 18, 21])
    ax.set_xticklabels([f"{h:02d}" for h in (0, 3, 6, 9, 12, 15, 18, 21)])
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right", "bottom", "left"):
        ax.spines[side].set_visible(False)
    ax.tick_params(labelsize=9, colors=MUTED, length=0, pad=4)
    ax.set_ylabel("GWh per hour", fontsize=9, color=MUTED, labelpad=6)


def build(data: dict):
    plt.rcParams["font.family"] = ["DejaVu Sans"]
    fig = plt.figure(figsize=(11.0, 6.4), dpi=200)
    fig.patch.set_facecolor("#ffffff")

    fig.text(0.055, 0.955, "With their reactors down, both countries leaned on imports",
             fontsize=17.5, fontweight="bold", color=INK, va="top")
    fig.text(0.055, 0.898, "after dark and on solar in the middle of the day",
             fontsize=17.5, fontweight="bold", color=INK, va="top")
    fig.text(0.055, 0.842,
             f"Average hourly demand and generation by source, 1 July {NDASH} 15 August 2026 (GWh)",
             fontsize=10.4, color=MUTED, va="top")

    # Only categories that actually appear: neither country reports pumped
    # storage, and a legend entry with no band on the chart is a puzzle.
    present = {k for cc, _ in COUNTRIES for h in data.get(cc, {}).values()
               for k in h if abs(h.get(k, 0.0)) > 1e-9}
    handles = [Line2D([], [], color=DEMAND, linewidth=2.4, label="Demand")]
    handles += [Patch(facecolor=COLOR[k], label=LABEL[k])
                for k in reversed(STACK) if k in present]
    fig.legend(handles=handles, loc="upper left", bbox_to_anchor=(0.052, 0.806),
               ncol=7, frameon=False, fontsize=9.2, handlelength=1.5,
               handleheight=0.9, columnspacing=1.4, labelcolor=INK)

    gs = fig.add_gridspec(1, 2, left=0.065, right=0.975, top=0.675, bottom=0.250,
                          wspace=0.20)
    for i, (cc, name) in enumerate(COUNTRIES):
        if cc not in data:
            continue
        ax = fig.add_subplot(gs[0, i])
        series = data[cc]
        xs = sorted(series)
        peak = max(series[h].get("demand", 0.0) for h in xs)
        nuc = sum(series[h].get("nuclear", 0.0) for h in xs)
        draw_panel(ax, xs, series, name,
                   f"peak {peak:.1f} GWh  {MIDDOT}  nuclear {nuc:.0f} GWh/day across the window")

    foot = [
        (0.185, f"Every day in the window is included, not only heatwave days, so these dates match the nuclear charts exactly. Hours are local: Romania keeps EEST, Hungary CEST."),
        (0.158, f"Both fleets were partly out inside this window: Hungary fell from 37 to about 4 GWh/day between 28 July and 2 August, and Romania from 15 to zero on 13{NDASH}14 August."),
        (0.131, f"Neither country reports pumped storage, so there is no storage band. Demand and generation do not close exactly: distributed solar counts as generation but never"),
        (0.104, f"crosses the transmission load meter, opening a gap that grows and shrinks with the sun; at night the shortfall is losses and units below ENTSO-E's reporting threshold."),
        (0.062, f"Source: ENTSO-E Transparency Platform (generation, load, cross-border flows)"),
    ]
    for y, txt in foot:
        fig.text(0.055, y, txt, fontsize=7.6, color=MUTED, va="top")

    fig.add_artist(Line2D([0.055, 0.30], [0.982, 0.982], color="#22c55e",
                          linewidth=3.4, solid_capstyle="butt"))
    return fig


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    data = fetch()
    for cc, name in COUNTRIES:
        s = data.get(cc, {})
        if not s:
            continue
        xs = sorted(s)
        sup = {h: sum(v for k, v in s[h].items() if k != "demand") for h in xs}
        print(f"{name}: peak demand {max(s[h].get('demand',0) for h in xs):.2f} GWh | "
              f"balance gap {min(sup[h]-s[h].get('demand',0) for h in xs):+.2f} to "
              f"{max(sup[h]-s[h].get('demand',0) for h in xs):+.2f} GWh")
    fig = build(data)
    pdf = OUT / "hu_ro_profile_summer_2026.pdf"
    png = OUT / "hu_ro_profile_summer_2026.png"
    fig.savefig(pdf, format="pdf", facecolor=fig.get_facecolor())
    fig.savefig(png, format="png", dpi=200, facecolor=fig.get_facecolor())
    plt.close(fig)
    for p in (pdf, png):
        print(f"{p}  ({p.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
