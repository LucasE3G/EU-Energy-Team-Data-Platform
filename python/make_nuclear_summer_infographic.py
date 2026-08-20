"""Shareable infographic: daily nuclear generation, 1 July - 15 August, for
France, Hungary and Romania, 2026 against the 2021-2025 range.

The zoomed companion to make_nuclear_infographic.py, which covers whole months.
Writes PDF and PNG to python/output/.

Daily energy is the day's mean output x 24 h, so a day with partial reporting
contributes its average level rather than a short total. Days with fewer than
18 readings are dropped - Romania has a handful in 2021 and 2023 with only one
or ten.
"""
from __future__ import annotations

import json
import urllib.request
from datetime import date, timedelta
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
WIN_START = (7, 1)
WIN_END = (8, 15)
MIN_SAMPLES = 18

LINE = "#1f4fd8"
BAND = "#d8dae0"
AVG = "#8b8f99"
INK, MUTED, GRID = "#14203a", "#6b7280", "#e6e8ec"

NDASH = "–"
MIDDOT = "·"


def window_days() -> list[tuple[int, int]]:
    d, end, out = date(2001, *WIN_START), date(2001, *WIN_END), []
    while d <= end:
        out.append((d.month, d.day))
        d += timedelta(days=1)
    return out


def load_env() -> dict:
    env = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def fetch() -> dict:
    """country -> (month, day) -> year -> GWh."""
    env = load_env()
    key = env["SUPABASE_ANON_KEY"]
    codes = ",".join(cc for cc, _ in COUNTRIES)
    out: dict = {}
    offset, page = 0, 1000
    while True:
        url = (f"{env['SUPABASE_URL']}/rest/v1/v_nuclear_daily"
               f"?select=country_code,date,gwh,samples&country_code=in.({codes})"
               f"&order=date.asc&limit={page}&offset={offset}")
        req = urllib.request.Request(url, headers={
            "apikey": key, "Authorization": f"Bearer {key}"})
        with urllib.request.urlopen(req, timeout=90) as r:
            rows = json.loads(r.read())
        for x in rows:
            if int(x["samples"]) < MIN_SAMPLES:
                continue
            d = date.fromisoformat(x["date"])
            (out.setdefault(x["country_code"], {})
                .setdefault((d.month, d.day), {}))[d.year] = float(x["gwh"])
        if len(rows) < page:
            return out
        offset += page


def draw_panel(ax, name, per_day: dict, keys: list):
    lo, hi = BASE_YEARS
    xs, band_lo, band_hi, band_avg = [], [], [], []
    cur_x, cur_y = [], []
    for i, k in enumerate(keys):
        vals = [per_day[k][y] for y in range(lo, hi + 1)
                if k in per_day and y in per_day[k]]
        if vals:
            xs.append(i)
            band_lo.append(min(vals))
            band_hi.append(max(vals))
            band_avg.append(sum(vals) / len(vals))
        if k in per_day and CUR_YEAR in per_day[k]:
            cur_x.append(i)
            cur_y.append(per_day[k][CUR_YEAR])

    ax.fill_between(xs, band_lo, band_hi, facecolor=BAND, linewidth=0, zorder=1)
    ax.plot(xs, band_avg, color=AVG, linewidth=1.5, linestyle=(0, (4, 2.5)), zorder=2)
    if cur_x:
        ax.plot(cur_x, cur_y, color=LINE, linewidth=2.4, zorder=3,
                solid_capstyle="round")

    ax.set_title(name, loc="left", fontsize=11.5, fontweight="bold",
                 color=INK, pad=20)

    # Window mean against the window mean of the baseline, stated in words.
    if cur_y and band_avg:
        cur_mean = sum(cur_y) / len(cur_y)
        base_mean = sum(band_avg) / len(band_avg)
        pct = 100 * (cur_mean / base_mean - 1) if base_mean else 0
        word = "above" if pct >= 0 else "below"
        ax.annotate(f"{cur_mean:,.0f} GWh/day, {abs(pct):.0f}% {word} the five-year mean",
                    xy=(0, 1.0), xycoords="axes fraction",
                    xytext=(0, 4), textcoords="offset points",
                    fontsize=8.3, color=MUTED, va="bottom")

    ticks = [i for i, k in enumerate(keys) if k[1] == 1 or k[1] == 15]
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{k[1]} {['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][k[0]]}"
                        for i, k in enumerate(keys) if i in ticks])
    ax.set_xlim(0, len(keys) - 1)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right", "bottom", "left"):
        ax.spines[side].set_visible(False)
    ax.tick_params(labelsize=8.4, colors=MUTED, length=0, pad=4)


def build(data: dict):
    plt.rcParams["font.family"] = ["DejaVu Sans"]
    fig = plt.figure(figsize=(11.0, 5.8), dpi=200)
    fig.patch.set_facecolor("#ffffff")

    fig.text(0.055, 0.948, "Hungary and Romania lost most of their nuclear output in early",
             fontsize=17, fontweight="bold", color=INK, va="top")
    fig.text(0.055, 0.884, "August, while France ran near the top of its five-year range",
             fontsize=17, fontweight="bold", color=INK, va="top")
    fig.text(0.055, 0.820,
             f"Daily nuclear generation, 1 July {NDASH} 15 August (GWh per day)",
             fontsize=10.4, color=MUTED, va="top")

    handles = [
        Line2D([], [], color=LINE, linewidth=2.4, label="2026"),
        Line2D([], [], color=AVG, linewidth=1.5, linestyle=(0, (4, 2.5)),
               label=f"Five-year average ({BASE_YEARS[0]}{NDASH}{BASE_YEARS[1]})"),
        Patch(facecolor=BAND, label="Five-year daily range"),
    ]
    fig.legend(handles=handles, loc="upper left", bbox_to_anchor=(0.052, 0.785),
               ncol=3, frameon=False, fontsize=9.2, handlelength=2.0,
               columnspacing=1.8, labelcolor=INK)

    keys = window_days()
    gs = fig.add_gridspec(1, 3, left=0.055, right=0.975, top=0.640, bottom=0.230,
                          wspace=0.22)
    for i, (cc, name) in enumerate(COUNTRIES):
        if cc not in data:
            continue
        draw_panel(fig.add_subplot(gs[0, i]), name, data[cc], keys)

    foot = [
        # A line falling to zero reads as missing data, so say plainly that it
        # is not: Romania reported around fifty readings on every one of those
        # days, and each one was zero.
        (0.165, "The Romanian and Hungarian troughs are reported output, not gaps: both countries filed a full day of readings throughout, and Romania's were zero from 14 August."),
        (0.136, "Each panel has its own scale: France runs about 900 GWh a day against roughly 30 in Hungary and Romania, so a shared axis would flatten the smaller two."),
        (0.108, f"Shaded band is the daily minimum to maximum across the same calendar days in {BASE_YEARS[0]}{NDASH}{BASE_YEARS[1]}; days with fewer than {MIN_SAMPLES} readings are excluded. France's band spans the 2022"),
        (0.080, "corrosion outages at its floor and 2025 at its ceiling, so it is wide by construction."),
        (0.038, "Source: ENTSO-E Transparency Platform, actual generation per production type (nuclear)"),
    ]
    for y, txt in foot:
        fig.text(0.055, y, txt, fontsize=7.7, color=MUTED, va="top")

    fig.add_artist(Line2D([0.055, 0.30], [0.978, 0.978], color=LINE,
                          linewidth=3.4, solid_capstyle="butt"))
    return fig


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    data = fetch()
    keys = window_days()
    for cc, name in COUNTRIES:
        per = data.get(cc, {})
        cur = [per[k][CUR_YEAR] for k in keys if k in per and CUR_YEAR in per[k]]
        base = [v for k in keys if k in per
                for y, v in per[k].items() if BASE_YEARS[0] <= y <= BASE_YEARS[1]]
        if cur and base:
            cm, bm = sum(cur) / len(cur), sum(base) / len(base)
            print(f"{name}: 2026 {cm:,.0f} GWh/day over {len(cur)} days | "
                  f"baseline {bm:,.0f} | {100*(cm/bm-1):+.0f}%")
    fig = build(data)
    pdf = OUT / "nuclear_summer_2026.pdf"
    png = OUT / "nuclear_summer_2026.png"
    fig.savefig(pdf, format="pdf", facecolor=fig.get_facecolor())
    fig.savefig(png, format="png", dpi=200, facecolor=fig.get_facecolor())
    plt.close(fig)
    for p in (pdf, png):
        print(f"{p}  ({p.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
