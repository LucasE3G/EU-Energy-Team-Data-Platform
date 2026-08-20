"""Shareable infographic: monthly nuclear generation in France, Hungary and
Romania, 2026 against the 2021-2025 range.

Writes PDF and PNG to python/output/.

Monthly energy is built from daily means (avg MW that day x 24 h), so a day with
partial reporting contributes its average rather than a short total. Months with
fewer than 25 days of data are dropped from both the line and the baseline -
that removes the in-progress current month and the partial start of the record
in April 2021.
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

COUNTRIES = [("FR", "France"), ("HU", "Hungary"), ("RO", "Romania")]
BASE_YEARS = (2021, 2025)
CUR_YEAR = 2026
MIN_DAYS = 25

LINE = "#1f4fd8"
BAND = "#d8dae0"
AVG = "#8b8f99"
INK, MUTED, GRID = "#14203a", "#6b7280", "#e6e8ec"

NDASH = "–"
MIDDOT = "·"
RSQUO = "’"

MONTHS = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"]


def load_env() -> dict:
    env = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def fetch() -> dict:
    """country -> year -> month -> TWh, for complete months only."""
    env = load_env()
    key = env["SUPABASE_ANON_KEY"]
    codes = ",".join(cc for cc, _ in COUNTRIES)
    url = (f"{env['SUPABASE_URL']}/rest/v1/v_nuclear_monthly"
           f"?select=*&country_code=in.({codes})&order=year.asc,month.asc")
    req = urllib.request.Request(url, headers={
        "apikey": key, "Authorization": f"Bearer {key}"})
    with urllib.request.urlopen(req, timeout=90) as r:
        rows = json.loads(r.read())

    out: dict = {}
    for x in rows:
        if int(x["days"]) < MIN_DAYS:
            continue
        (out.setdefault(x["country_code"], {})
            .setdefault(int(x["year"]), {}))[int(x["month"])] = float(x["twh"])
    return out


def draw_panel(ax, name, years: dict):
    lo, hi = BASE_YEARS
    base = {m: [years[y][m] for y in range(lo, hi + 1)
                if y in years and m in years[y]] for m in range(1, 13)}
    base = {m: v for m, v in base.items() if v}

    ms = sorted(base)
    ax.fill_between(ms, [min(base[m]) for m in ms], [max(base[m]) for m in ms],
                    facecolor=BAND, linewidth=0, zorder=1)
    ax.plot(ms, [sum(base[m]) / len(base[m]) for m in ms],
            color=AVG, linewidth=1.6, linestyle=(0, (4, 2.5)), zorder=2)

    cur = years.get(CUR_YEAR, {})
    cms = sorted(cur)
    if cms:
        ax.plot(cms, [cur[m] for m in cms], color=LINE, linewidth=2.6,
                zorder=3, solid_capstyle="round")

    # Pad clears the note beneath it; at 8 the two overlapped.
    ax.set_title(name, loc="left", fontsize=11.5, fontweight="bold",
                 color=INK, pad=20)
    ax.set_xlim(1, 12)
    ax.set_xticks([1, 4, 7, 10])
    ax.set_xticklabels(["Jan", "Apr", "Jul", "Oct"])
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right", "bottom", "left"):
        ax.spines[side].set_visible(False)
    ax.tick_params(labelsize=8.6, colors=MUTED, length=0, pad=4)

    # Latest complete month against its own five-year average, stated in words
    # so the reader does not have to measure the gap by eye.
    if cms:
        m = max(cms)
        if m in base:
            avg = sum(base[m]) / len(base[m])
            pct = 100 * (cur[m] / avg - 1) if avg else 0
            word = "above" if pct >= 0 else "below"
            ax.annotate(f"{['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][m-1]}: "
                        f"{abs(pct):.0f}% {word} average",
                        xy=(0, 1.0), xycoords="axes fraction",
                        xytext=(0, 4), textcoords="offset points",
                        fontsize=8.3, color=MUTED, va="bottom")


def build(data: dict):
    plt.rcParams["font.family"] = ["DejaVu Sans"]
    fig = plt.figure(figsize=(11.0, 5.6), dpi=200)
    fig.patch.set_facecolor("#ffffff")

    # January sits inside the range for both, so "all year" would overstate it:
    # the clean claim is that they have been outside it, in opposite directions,
    # every month from February onward.
    fig.text(0.055, 0.945, "Since February, France has run above its five-year range",
             fontsize=17.5, fontweight="bold", color=INK, va="top")
    fig.text(0.055, 0.878, "every month and Romania below it every month",
             fontsize=17.5, fontweight="bold", color=INK, va="top")
    fig.text(0.055, 0.812, "Nuclear generation by month (TWh)",
             fontsize=10.4, color=MUTED, va="top")

    handles = [
        Line2D([], [], color=LINE, linewidth=2.6, label="2026"),
        Line2D([], [], color=AVG, linewidth=1.6, linestyle=(0, (4, 2.5)),
               label=f"Five-year average ({BASE_YEARS[0]}{NDASH}{BASE_YEARS[1]})"),
        Patch(facecolor=BAND, label=f"Five-year monthly range"),
    ]
    fig.legend(handles=handles, loc="upper left", bbox_to_anchor=(0.052, 0.775),
               ncol=3, frameon=False, fontsize=9.2, handlelength=2.0,
               columnspacing=1.8, labelcolor=INK)

    gs = fig.add_gridspec(1, 3, left=0.055, right=0.975, top=0.635, bottom=0.235,
                          wspace=0.22)
    for i, (cc, name) in enumerate(COUNTRIES):
        if cc not in data:
            continue
        draw_panel(fig.add_subplot(gs[0, i]), name, data[cc])

    foot = [
        (0.170, f"Each panel has its own scale: France runs about 30 TWh a month against roughly 1 TWh in Hungary and Romania, so a shared axis would flatten the smaller two."),
        (0.140, f"Shaded band is the monthly minimum to maximum across {BASE_YEARS[0]}{NDASH}{BASE_YEARS[1]}. Months with fewer than {MIN_DAYS} days of reported data are excluded, which drops the"),
        (0.112, f"month in progress and the start of the record in April 2021 {MIDDOT} January to March rest on four years rather than five."),
        (0.062, f"Source: ENTSO-E Transparency Platform, actual generation per production type (nuclear)"),
    ]
    for y, txt in foot:
        fig.text(0.055, y, txt, fontsize=7.7, color=MUTED, va="top")

    fig.add_artist(Line2D([0.055, 0.30], [0.975, 0.975], color=LINE,
                          linewidth=3.4, solid_capstyle="butt"))
    return fig


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    data = fetch()
    for cc, name in COUNTRIES:
        yrs = data.get(cc, {})
        cur = yrs.get(CUR_YEAR, {})
        print(f"{name}: {sorted(yrs)} | 2026 months {sorted(cur)}")
    fig = build(data)
    pdf = OUT / "nuclear_2026_vs_range.pdf"
    png = OUT / "nuclear_2026_vs_range.png"
    fig.savefig(pdf, format="pdf", facecolor=fig.get_facecolor())
    fig.savefig(png, format="png", dpi=200, facecolor=fig.get_facecolor())
    plt.close(fig)
    for p in (pdf, png):
        print(f"{p}  ({p.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
