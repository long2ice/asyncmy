# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib==3.11.2"]
# ///
"""Render the recorded benchmark JSON as reproducible README charts.

    uv run scripts/plot_benchmarks.py
    uv run scripts/plot_benchmarks.py --check
    uv run scripts/plot_benchmarks.py --format png --output-dir /tmp/asyncmy-charts

Plotting dependencies are isolated by uv; the driver's dependencies stay unchanged.
"""

from __future__ import annotations

import argparse
import io
import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.ticker import MaxNLocator, PercentFormatter

ROOT = Path(__file__).resolve().parent.parent
TEXT = "#172b3a"
MUTED = "#526579"
GRID = "#e4eaf0"
COLORS = {
    "asyncmy": "#087f70",
    "asyncmy-binary": "#2a9b8d",
    "mysqlclient": "#6b7e94",
    "pymysql": "#8998aa",
    "aiomysql": "#526579",
    "asyncmy_asyncio": "#3274bc",
    "asyncmy_uvloop": "#087f70",
    "go": "#bc791a",
    "rust": "#865ac3",
}
LABELS = {
    "asyncmy": "asyncmy",
    "asyncmy-binary": "asyncmy · binary",
    "mysqlclient": "mysqlclient",
    "pymysql": "PyMySQL",
    "aiomysql": "aiomysql",
    "asyncmy_asyncio": "asyncmy · asyncio",
    "asyncmy_uvloop": "asyncmy · uvloop",
    "go": "Go",
    "rust": "Rust",
}


def configure_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 12,
            "text.color": TEXT,
            "axes.labelcolor": MUTED,
            "axes.edgecolor": GRID,
            "xtick.color": MUTED,
            "ytick.color": TEXT,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "svg.fonttype": "none",
            "svg.hashsalt": "asyncmy-benchmarks",
        }
    )


def heading(fig: Figure, title: str, subtitle: str) -> None:
    fig.text(0.055, 0.965, title, fontsize=25, weight="bold", va="top")
    subtitle_y = 0.965 - 42 / (fig.get_figheight() * 72)
    fig.text(0.055, subtitle_y, subtitle, fontsize=12, color=MUTED, va="top")


def footer(fig: Figure, environment: dict, notes: str) -> None:
    fig.text(0.055, 0.067, notes, fontsize=10.5, color=MUTED, va="top")
    fig.text(
        0.055,
        0.033,
        f"{environment['date']}  ·  {environment['cpu']}  ·  "
        f"MySQL {environment['mysql']} / TCP  ·  CPython {environment['python']}",
        fontsize=10.5,
        color=MUTED,
        va="top",
    )


def time_panel(
    ax: Axes, title: str, detail: str, series: list[tuple[str, float]], decimals: int
) -> None:
    values = [value for _, value in series]
    if not values or any(not math.isfinite(value) or value <= 0 for value in values):
        raise ValueError(f"{title}: timings must be finite and positive")
    ax.barh(
        range(len(series)),
        values,
        height=0.53,
        color=[COLORS[key] for key, _ in series],
        zorder=3,
    )
    ax.set_yticks(range(len(series)), [LABELS[key] for key, _ in series])
    ax.set_ylim(len(series) - 0.4, -0.65)
    ax.set_xlim(0, max(values) * 1.26)
    ax.xaxis.set_major_locator(MaxNLocator(nbins=4, integer=decimals == 0))
    ax.set_xlabel("Elapsed time (ms)", fontsize=10.5, labelpad=8)
    ax.tick_params(axis="y", length=0, pad=9, labelsize=11.5)
    ax.tick_params(axis="x", length=0, labelsize=10.5)
    ax.grid(axis="x", color=GRID, linewidth=0.8, zorder=0)
    for name in ["top", "right", "left"]:
        ax.spines[name].set_visible(False)
    ax.set_title(title, loc="left", fontsize=16, weight="bold", pad=32)
    ax.text(0, 1.055, detail, transform=ax.transAxes, fontsize=10.5, color=MUTED)
    for y, value in enumerate(values):
        ax.text(
            value + max(values) * 0.025,
            y,
            f"{value:.{decimals}f}",
            va="center",
            fontsize=12,
            weight="bold" if value == min(values) else "normal",
        )


def python_chart(data: dict) -> Figure:
    cases = [
        ("large_resultset", "Large result set", "33,334 mixed-type rows"),
        ("concurrent", "Concurrent queries", "50 queries · one connection per query"),
        ("pool", "Connection pool", "2,000 queries · 5–20 connections"),
        ("batch_insert", "Batch insert", "10,000 rows · executemany()"),
    ]
    drivers = ["asyncmy", "asyncmy-binary", "mysqlclient", "pymysql", "aiomysql"]
    values = {(row["case"], row["driver"]): row["reported_ms"] for row in data["results"]}
    fig, axes = plt.subplots(2, 2, figsize=(15, 9.5))
    fig.subplots_adjust(left=0.155, right=0.94, top=0.79, bottom=0.17, wspace=0.8, hspace=0.9)
    heading(
        fig,
        "Python MySQL drivers",
        "Elapsed time · lower is better · best of 3 after 1 warmup · standard asyncio",
    )
    for ax, (case, title, detail) in zip(axes.flat, cases):
        series = [(key, values[case, key]) for key in drivers if (case, key) in values]
        time_panel(ax, title, detail, series, decimals=0)
    footer(
        fig,
        data["environment"],
        "Independent zero-based axes · only measured combinations shown · "
        "rounded to 1 ms; batch insert is effectively tied",
    )
    return fig


def cross_language_chart(data: dict) -> Figure:
    cases = [
        ("scan_integer_text", "Integer scan · text", "50,000 rows × 8 integer columns"),
        ("scan_integer_binary", "Integer scan · binary", "50,000 rows × 8 integer columns"),
        ("scan_mixed_text", "Mixed scan · text", "50,000 rows × 10 mixed-type columns"),
        ("scan_mixed_binary", "Mixed scan · binary", "50,000 rows × 10 mixed-type columns"),
        (
            "point_text",
            "Pooled point queries · text",
            "4,000 queries · 20 workers · 10 connections",
        ),
        (
            "point_binary",
            "Pooled point queries · binary",
            "4,000 queries · 20 workers · 10 connections",
        ),
    ]
    drivers = ["asyncmy_asyncio", "asyncmy_uvloop", "go", "rust"]
    values = {(row["case"], row["runtime"]): row["median_ms"] for row in data["results"]}
    fig, axes = plt.subplots(3, 2, figsize=(15, 12))
    fig.subplots_adjust(left=0.155, right=0.94, top=0.82, bottom=0.135, wspace=0.8, hspace=0.9)
    heading(
        fig,
        "asyncmy vs Go & Rust",
        "Median elapsed time · lower is better · 504 samples per case · 72 balanced rounds",
    )
    for ax, (case, title, detail) in zip(axes.flat, cases):
        time_panel(ax, title, detail, [(key, values[case, key]) for key in drivers], decimals=2)
    footer(
        fig,
        data["environment"],
        "Independent zero-based axes · scans retain all rows · "
        "DECIMAL: Python Decimal, Go/Rust strings",
    )
    return fig


def point_advantage_chart(data: dict) -> Figure:
    comparisons = data["point_binary_statistics"]["comparisons"]
    fig, ax = plt.subplots(figsize=(15, 5))
    fig.subplots_adjust(left=0.16, right=0.68, top=0.66, bottom=0.3)
    heading(
        fig,
        "Pooled binary queries: asyncmy + uvloop",
        "Mean per-round throughput advantage · higher is better · 95% block-bootstrap intervals",
    )
    for y, key in enumerate(["go", "rust"]):
        stats = comparisons[key]
        mean = stats["mean_throughput_advantage_pct"]
        low, high = stats["confidence_interval_95_pct"]
        ax.errorbar(
            mean,
            y,
            xerr=[[mean - low], [high - mean]],
            fmt="o",
            markersize=10,
            color=COLORS["asyncmy_uvloop"],
            elinewidth=4,
            capsize=7,
            capthick=2,
            zorder=3,
        )
        ax.text(
            1.045,
            y,
            f"+{mean:.2f}%  [{low:.2f}%, {high:.2f}%]",
            transform=ax.get_yaxis_transform(),
            fontsize=13,
            weight="bold",
            va="center",
        )
    maximum = max(row["confidence_interval_95_pct"][1] for row in comparisons.values())
    ax.set_xlim(0, maximum * 1.12)
    ax.set_ylim(1.65, -0.65)
    ax.set_yticks([0, 1], ["vs Go", "vs Rust"])
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
    ax.xaxis.set_major_locator(MaxNLocator(nbins=6))
    ax.set_xlabel("Throughput advantage over the native driver", labelpad=10)
    ax.tick_params(axis="both", length=0, pad=9)
    ax.grid(axis="x", color=GRID, zorder=0)
    ax.axvline(0, color=MUTED, linewidth=1.2)
    for name in ["top", "right", "left"]:
        ax.spines[name].set_visible(False)
    fig.text(
        0.055,
        0.14,
        "20 × 200 queries · 10 connections · autocommit=True · stmt_cache_size=32 · "
        "ordinary Cursor API",
        fontsize=11,
        color=MUTED,
    )
    footer(
        fig,
        data["environment"],
        "72 rounds · 12 balanced blocks · 100,000 bootstrap draws · "
        "ratios computed per round, separately from aggregate medians",
    )
    return fig


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "images")
    parser.add_argument("--format", choices=["svg", "png", "both"], default="svg")
    parser.add_argument("--check", action="store_true", help="Fail if a generated chart differs")
    args = parser.parse_args()
    configure_style()
    python = json.loads((ROOT / "benchmark/python-results.json").read_text())
    cross = json.loads((ROOT / "benchmark/crosslang/results.json").read_text())
    charts = [
        ("benchmark-python", python_chart(python)),
        ("benchmark-crosslang", cross_language_chart(cross)),
        ("benchmark-point-binary", point_advantage_chart(cross)),
    ]
    formats = ["svg", "png"] if args.format == "both" else [args.format]
    if not args.check:
        args.output_dir.mkdir(parents=True, exist_ok=True)
    stale = False
    for name, fig in charts:
        for fmt in formats:
            output = io.BytesIO()
            metadata = {"Date": None, "Creator": "asyncmy benchmark charts"} if fmt == "svg" else {}
            fig.savefig(output, format=fmt, dpi=144, metadata=metadata)
            path = args.output_dir / f"{name}.{fmt}"
            content = output.getvalue()
            if fmt == "svg":
                content = b"\n".join(line.rstrip() for line in content.splitlines()) + b"\n"
            if args.check:
                if not path.exists() or path.read_bytes() != content:
                    print(f"Stale or missing chart: {path}")
                    stale = True
            else:
                path.write_bytes(content)
                print(path)
        plt.close(fig)
    return int(stale)


if __name__ == "__main__":
    raise SystemExit(main())
