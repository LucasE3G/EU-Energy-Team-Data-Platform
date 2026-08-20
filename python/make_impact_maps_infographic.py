"""Shareable infographic: what fell and what rose most in each country during
its 2026 heatwaves, as two maps.

Writes PDF and PNG to python/output/.

Values are POWER (GW/MW), not percentages: the trade component crosses zero, so
Bulgaria's 4.9 GWh/day fall on a negative base computes as +36%, which is
arithmetically true and useless. Ranking is by absolute change within each
country, so it reads as "what moved most here", not "who moved most in Europe".
"""
from __future__ import annotations

import json
import urllib.request
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import FancyBboxPatch, Polygon as MplPolygon

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "python" / "output"
GEO = ROOT / "assets" / "geo" / "europe-countries.geojson"

BOUNDS = dict(min_lon=-16.0, max_lon=36.0, min_lat=33.0, max_lat=63.0)
MIN_DAYS = 10

COLOR = {"gas": "#c2562f", "coal": "#6b6a62", "other": "#a05fb4",
         "biomass": "#2f8f4e", "nuclear": "#4a3aa7", "hydro": "#1baf7a",
         "solar": "#e8a33d", "wind": "#2a78d6", "imports": "#0f9d6e"}
TINT = {"gas": "#f6d9cc", "coal": "#ded9d3", "other": "#e8d9ea",
        "biomass": "#d5e8d5", "nuclear": "#dcdcef", "hydro": "#d3e6e4",
        "solar": "#f7ecc9", "wind": "#d5e2f5", "imports": "#d2ece0"}
NO_DATA = "#eceef1"
INK, MUTED = "#14203a", "#6b7280"

NAMES = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CH": "Switzerland",
    "CY": "Cyprus", "CZ": "Czechia", "DE": "Germany", "DK": "Denmark",
    "EE": "Estonia", "ES": "Spain", "FI": "Finland", "FR": "France",
    "GB": "United Kingdom", "GR": "Greece", "HR": "Croatia", "HU": "Hungary",
    "IE": "Ireland", "IT": "Italy", "LT": "Lithuania", "LU": "Luxembourg",
    "LV": "Latvia", "NL": "Netherlands", "NO": "Norway", "PL": "Poland",
    "PT": "Portugal", "RO": "Romania", "SE": "Sweden", "SI": "Slovenia",
    "SK": "Slovakia",
}
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
    url = (f"{env['SUPABASE_URL']}/rest/v1/mv_heatwave_component_delta"
           f"?select=country_code,component,delta_gwh,delta_mw"
           f"&heatwave_days=gte.{MIN_DAYS}")
    req = urllib.request.Request(url, headers={
        "apikey": key, "Authorization": f"Bearer {key}"})
    with urllib.request.urlopen(req, timeout=90) as r:
        rows = json.loads(r.read())
    out: dict = {}
    for x in rows:
        out.setdefault(x["country_code"], []).append(x)
    return out


def power(mw) -> str:
    v = abs(float(mw or 0))
    return f"{v/1000:.1f} GW" if v >= 1000 else f"{v:.0f} MW"


def project(lon, lat):
    """Equirectangular, cos-corrected at the mid-latitude - the same shape the
    site's maps use, so print and screen agree."""
    import math
    mid = math.radians((BOUNDS["min_lat"] + BOUNDS["max_lat"]) / 2)
    x = (lon - BOUNDS["min_lon"]) * math.cos(mid)
    y = lat - BOUNDS["min_lat"]
    return x, y


def rings(geom):
    if geom["type"] == "Polygon":
        return [geom["coordinates"][0]]
    if geom["type"] == "MultiPolygon":
        return [p[0] for p in geom["coordinates"] if p and p[0]]
    return []


def layout(items, xlim, ylim, passes=140):
    """Push overlapping badges apart, biggest mover first, then clamp."""
    boxes = sorted(items, key=lambda b: -b["weight"])
    for b in boxes:
        b["x"], b["y"] = b["ax"], b["ay"]
    for _ in range(passes):
        moved = False
        for i in range(len(boxes)):
            for j in range(i + 1, len(boxes)):
                a, b = boxes[i], boxes[j]
                dx = (a["w"] + b["w"]) / 2 + 0.15 - abs(a["x"] - b["x"])
                dy = (a["h"] + b["h"]) / 2 + 0.15 - abs(a["y"] - b["y"])
                if dx <= 0 or dy <= 0:
                    continue
                moved = True
                if dx < dy:
                    s = (-dx if a["x"] <= b["x"] else dx)
                    a["x"] += s * 0.35
                    b["x"] -= s * 0.65
                else:
                    s = (-dy if a["y"] <= b["y"] else dy)
                    a["y"] += s * 0.35
                    b["y"] -= s * 0.65
        for b in boxes:
            b["x"] = min(xlim[1] - b["w"] / 2, max(xlim[0] + b["w"] / 2, b["x"]))
            b["y"] = min(ylim[1] - b["h"] / 2, max(ylim[0] + b["h"] / 2, b["y"]))
        if not moved:
            break
    return boxes


def draw_map(ax, feats, data, direction):
    down = direction == "down"
    arrow = "▼" if down else "▲"
    used, pending = set(), []

    def top2(cc):
        lst = sorted(data.get(cc, []), key=lambda r: float(r["delta_gwh"]),
                     reverse=not down)
        return [r for r in lst
                if (float(r["delta_gwh"]) < -0.05 if down else float(r["delta_gwh"]) > 0.05)][:2]

    for f in feats:
        iso2 = (f.get("properties", {}).get("ISO2") or "").upper()
        if not iso2:
            continue
        cc = "GB" if iso2 == "UK" else iso2
        top = top2(cc)
        rs = rings(f.get("geometry") or {})
        if not rs:
            continue
        fill = TINT.get(top[0]["component"], NO_DATA) if top else NO_DATA
        for ring in rs:
            pts = [project(lon, lat) for lon, lat in ring]
            ax.add_patch(MplPolygon(pts, closed=True, facecolor=fill,
                                    edgecolor="#ffffff", linewidth=0.5, zorder=1))
        if not top:
            continue
        used.add(top[0]["component"])
        big = max(rs, key=len)
        pts = [project(lon, lat) for lon, lat in big]
        cx = sum(p[0] for p in pts) / len(pts)
        cy = sum(p[1] for p in pts) / len(pts)
        lines = [(("Trade" if r["component"] == "imports" else r["component"].capitalize()),
                  power(r["delta_mw"]), COLOR.get(r["component"], "#8a8f98"))
                 for r in top]
        widest = max([len(NAMES.get(cc, cc))] + [len(a) + len(b) + 4 for a, b, _ in lines])
        pending.append(dict(cc=cc, lines=lines, ax=cx, ay=cy,
                            w=widest * 0.36 + 0.9, h=1.05 + 0.72 * len(lines),
                            weight=abs(float(top[0]["delta_mw"] or 0))))

    xs = [project(BOUNDS["min_lon"], 0)[0], project(BOUNDS["max_lon"], 0)[0]]
    ys = [0, BOUNDS["max_lat"] - BOUNDS["min_lat"]]
    for b in layout(pending, xs, ys):
        # Connect whenever the badge has left its own footprint. A generous
        # threshold left half of them floating with no tie to their country.
        if (b["x"] - b["ax"]) ** 2 + (b["y"] - b["ay"]) ** 2 > (b["h"] / 2 + 0.2) ** 2:
            ax.plot([b["ax"], b["x"]], [b["ay"], b["y"]], color="#8b93a3",
                    linewidth=0.6, zorder=4)
            ax.plot([b["ax"]], [b["ay"]], marker="o", markersize=2.0,
                    color="#8b93a3", zorder=4)
        ax.add_patch(FancyBboxPatch(
            (b["x"] - b["w"] / 2, b["y"] - b["h"] / 2), b["w"], b["h"],
            boxstyle="round,pad=0,rounding_size=0.18",
            facecolor="#ffffff", edgecolor="#c9cedb", linewidth=0.6, zorder=5))
        left = b["x"] - b["w"] / 2 + 0.28
        ax.text(left, b["y"] + b["h"] / 2 - 0.42, NAMES.get(b["cc"], b["cc"]),
                fontsize=6.4, fontweight="bold", color=INK, va="center", zorder=6)
        for i, (fuel, val, col) in enumerate(b["lines"]):
            y = b["y"] + b["h"] / 2 - 1.06 - i * 0.72
            ax.text(left, y, arrow, fontsize=5.2, color=col, va="center", zorder=6)
            ax.text(left + 0.42, y, f"{fuel} {val}", fontsize=6.2,
                    color=INK if i == 0 else "#6b7488", va="center", zorder=6)

    ax.set_xlim(xs[0] - 0.4, xs[1] + 0.4)
    ax.set_ylim(ys[0] - 0.4, ys[1] + 0.4)
    ax.set_aspect("equal")
    ax.axis("off")
    return used


def build(data: dict, feats: list):
    plt.rcParams["font.family"] = ["DejaVu Sans"]
    fig = plt.figure(figsize=(15.6, 8.4), dpi=200)
    fig.patch.set_facecolor("#ffffff")

    fig.text(0.035, 0.962, "Wind and hydro gave way; gas, solar and the interconnectors took over",
             fontsize=18, fontweight="bold", color=INK, va="top")
    fig.text(0.035, 0.918,
             f"The two components that moved most in each country on its 2026 heatwave days, against all non-heatwave days of the same season and day type",
             fontsize=10.2, color=MUTED, va="top")

    # Legend sits between the subtitle and the maps; at the same height as the
    # panel titles the two ran straight through each other.
    gs = fig.add_gridspec(1, 2, left=0.025, right=0.985, top=0.815, bottom=0.115,
                          wspace=0.02)
    used = set()
    axes = []
    for i, d in enumerate(["down", "up"]):
        ax = fig.add_subplot(gs[0, i])
        used |= draw_map(ax, feats, data, d)
        axes.append(ax)

    for ax, title in zip(axes, ["What fell the most", "What rose to replace it"]):
        ax.set_title(title, loc="left", fontsize=13, fontweight="bold",
                     color=INK, pad=8)

    order = ["wind", "hydro", "nuclear", "coal", "biomass", "other",
             "solar", "gas", "imports"]
    handles = [Line2D([], [], marker="s", linestyle="none", markersize=8,
                      markerfacecolor=TINT[k], markeredgecolor=COLOR[k],
                      label="Net trade" if k == "imports" else k.capitalize())
               for k in order if k in used]
    fig.legend(handles=handles, loc="upper left", bbox_to_anchor=(0.033, 0.886),
               ncol=len(handles), frameon=False, fontsize=9, handletextpad=0.5,
               columnspacing=1.4, labelcolor=INK)

    foot = [
        (0.082, "Figures are power, in gigawatts or megawatts, not percentages: the trade component crosses zero, so Bulgaria's net imports falling 4.9 GWh/day on a negative base"),
        (0.058, "would compute as +36%. Ranking is within each country, so Germany's 4.1 GW of lost wind and Slovenia's 95 MW of lost hydro are each their own country's largest move."),
        (0.026, f"Heatwave days: 3 or more consecutive days above the local 90th-percentile daily maximum (2014{NDASH}2025 baseline), or above 30 °C {MIDDOT} "
                f"Source: ENTSO-E Transparency Platform {MIDDOT} ERA5 via Open-Meteo"),
    ]
    for y, txt in foot:
        fig.text(0.035, y, txt, fontsize=7.6, color=MUTED, va="top")

    fig.add_artist(Line2D([0.035, 0.20], [0.985, 0.985], color="#2a78d6",
                          linewidth=3.4, solid_capstyle="butt"))
    return fig


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    feats = json.loads(GEO.read_text(encoding="utf-8"))["features"]
    data = fetch()
    print(f"{len(data)} countries with data, {len(feats)} outlines")
    fig = build(data, feats)
    pdf, png = OUT / "heatwave_impact_maps.pdf", OUT / "heatwave_impact_maps.png"
    fig.savefig(pdf, format="pdf", facecolor=fig.get_facecolor())
    fig.savefig(png, format="png", dpi=200, facecolor=fig.get_facecolor())
    plt.close(fig)
    for p in (pdf, png):
        print(f"{p}  ({p.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
