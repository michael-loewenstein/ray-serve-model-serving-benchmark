#!/usr/bin/env python3
"""Generate the Phase 3 GPU rerun charts as hand-rolled SVG (stdlib only).

Charts:
  1. Throughput vs concurrency per arm  -> docs/images/phase3_gpu_rerun_throughput_scaling.svg
  2. p95 and p99 latency vs concurrency  -> docs/images/phase3_gpu_rerun_latency_tail.svg

Each chart draws, per arm and concurrency level: the per-trial scatter points
(5 per arm per level), the mean line, and a 95% CI band (Student's t, df = n-1)
as a series-colored wash.  Scatter x-positions are jittered deterministically
(seed 42) inside each concurrency slot, on the arm's side of the slot, so
near-identical trials stay legible without misrepresenting any value.

Colors are the dataviz reference categorical palette slots 1-2 (blue, orange),
validated for all-pairs use in both light and dark modes; the SVG ships both
steps and swaps them via prefers-color-scheme, so the charts read in both
GitHub light and dark themes.

Usage:
  python3 benchmarks/phase3-gpu-rerun/make_charts.py \
    --data results/phase3-gpu-rerun-2026-11-19/phase3-raw.json
"""

import argparse
import json
import math
import random
import re
from statistics import mean, stdev

DEFAULT_OUTPUTS = {
    "throughput": "docs/images/phase3_gpu_rerun_throughput_scaling.svg",
    "latency": "docs/images/phase3_gpu_rerun_latency_tail.svg",
}

LEVELS = [1, 4, 8, 16, 32, 64]
# (target_key, display name, css series class, jitter window sign)
ARMS = [
    ("direct", "Direct Serving (GPU)", "s1", -1),
    ("ray_ha", "Ray Serve (HAProxy :8000)", "s2", +1),
]
# Student's t two-sided 95% critical value for df=4 (n=5 trials per cell).
T_DF4 = 2.776445

FONT_STACK = "system-ui, -apple-system, 'Segoe UI', sans-serif"

# Light-mode palette steps (dataviz reference palette slots 1-2 + ink tokens).
LIGHT = {
    "surface": "#fcfcfb", "ink": "#0b0b0b", "ink2": "#52514e",
    "muted": "#898781", "grid": "#e1e0d9", "axis": "#c3c2b7",
    "s1": "#2a78d6", "s2": "#eb6834",
}
# Dark-mode steps: the same two hues stepped for the dark surface.
DARK = {
    "surface": "#1a1a19", "ink": "#f0efec", "ink2": "#c3c2b7",
    "muted": "#898781", "grid": "#2c2c2a", "axis": "#383835",
    "s1": "#3987e5", "s2": "#d95926",
}


# ---------------------------------------------------------------- data

def load_trials(path):
    with open(path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    trials = doc["trials"] if isinstance(doc, dict) else doc
    cells = {}
    for arm_key, _, _, _ in ARMS:
        for level in LEVELS:
            rows = sorted(
                (r for r in trials
                 if r["target_key"] == arm_key and r["concurrency"] == level),
                key=lambda r: r["trial_number"])
            if len(rows) != 5:
                raise SystemExit(
                    "expected 5 trials for arm=%s C=%d, found %d"
                    % (arm_key, level, len(rows)))
            cells[(arm_key, level)] = rows
    if len(cells) * 5 != 60:
        raise SystemExit("expected 60 trials total, found %d" % (len(cells) * 5))
    return cells


def cell_stats(rows, field):
    """Per-cell values, mean, and Student-t 95% CI of the mean."""
    values = [r[field] for r in rows]
    mu = mean(values)
    half = T_DF4 * stdev(values) / math.sqrt(len(values))
    return values, mu, (mu - half, mu + half)


def arm_stats(cells, field):
    return {(arm, level): cell_stats(cells[(arm, level)], field)
            for arm, _, _, _ in ARMS for level in LEVELS}


# ---------------------------------------------------------------- scales

def nice_ticks(data_max, target):
    step0 = data_max / target
    mag = 10 ** math.floor(math.log10(step0))
    step = 10 * mag
    for mult in (1, 2, 2.5, 5, 10):
        step = mult * mag
        if step >= step0:
            break
    top = math.ceil(data_max / step) * step
    count = int(round(top / step))
    return top, [step * k for k in range(count + 1)]


def fmt_thousands(value):
    return "{:,}".format(int(round(value)))


# ---------------------------------------------------------------- drawing

class Panel:
    """One plotting frame on a log2 x-scale (concurrency 1..64)."""

    def __init__(self, x0, x1, top, bottom, ymax, ticks, tick_fmt,
                 x_title, y_title, y_title_gap):
        self.x0, self.x1 = x0, x1
        self.top, self.bottom = top, bottom
        self.ymax = ymax
        self.ticks = ticks
        self.tick_fmt = tick_fmt
        self.x_title = x_title
        self.y_title = y_title
        self.y_title_gap = y_title_gap

    # Inner padding so jittered dots and end labels stay inside the frame.
    PAD_LEFT = 30
    PAD_RIGHT = 72

    def px(self, level):
        frac = math.log2(level) / math.log2(LEVELS[-1])
        lo = self.x0 + self.PAD_LEFT
        hi = self.x1 - self.PAD_RIGHT
        return lo + frac * (hi - lo)

    def py(self, value):
        return self.bottom - (value / self.ymax) * (self.bottom - self.top)

    def frame(self):
        parts = []
        for tick in self.ticks:
            y = self.py(tick)
            if tick != 0:
                parts.append(
                    '<line class="grid" x1="%.2f" y1="%.2f" x2="%.2f" y2="%.2f"/>'
                    % (self.x0, y, self.x1, y))
            parts.append(
                '<text class="tick" x="%.2f" y="%.2f" text-anchor="end">%s</text>'
                % (self.x0 - 8, y + 4, self.tick_fmt(tick)))
        parts.append(
            '<line class="axis" x1="%.2f" y1="%.2f" x2="%.2f" y2="%.2f"/>'
            % (self.x0, self.bottom, self.x1, self.bottom))
        for level in LEVELS:
            parts.append(
                '<text class="tick" x="%.2f" y="%.2f" text-anchor="middle">%d</text>'
                % (self.px(level), self.bottom + 18, level))
        parts.append(
            '<text class="axtitle" x="%.2f" y="%.2f" text-anchor="middle">%s</text>'
            % ((self.x0 + self.x1) / 2.0, self.bottom + 40, self.x_title))
        if self.y_title:
            cx = self.x0 - self.y_title_gap
            cy = (self.top + self.bottom) / 2.0
            parts.append(
                '<text class="axtitle" transform="rotate(-90 %.2f %.2f)" '
                'x="%.2f" y="%.2f" text-anchor="middle">%s</text>'
                % (cx, cy, cx, cy, self.y_title))
        return "".join(parts)

    def scatter(self, stats, arm_index):
        """Per-trial dots, jittered on the arm's side of each level slot."""
        arm_key, _, css, sign = ARMS[arm_index]
        parts = []
        for level in LEVELS:
            rand = random.Random(42 + int(math.log2(level)) * 7 + arm_index)
            values = stats[(arm_key, level)][0]
            for value in values:
                x = self.px(level) + sign * rand.uniform(6.0, 22.0)
                parts.append(
                    '<circle class="pt %s" cx="%.2f" cy="%.2f" r="4"/>'
                    % (css, x, self.py(value)))
        return "".join(parts)

    def mean_line(self, stats, arm_index):
        arm_key, _, css, _ = ARMS[arm_index]
        pts = " ".join(
            "%.2f,%.2f" % (self.px(level), self.py(stats[(arm_key, level)][1]))
            for level in LEVELS)
        return ('<polyline class="line %s" points="%s"/>' % (css, pts))

    def ci_band(self, stats, arm_index):
        arm_key, _, css, _ = ARMS[arm_index]
        upper = " ".join(
            "%.2f,%.2f" % (self.px(level), self.py(stats[(arm_key, level)][2][1]))
            for level in LEVELS)
        lower = " ".join(
            "%.2f,%.2f" % (self.px(level), self.py(stats[(arm_key, level)][2][0]))
            for level in reversed(LEVELS))
        return ('<polygon class="band %s" points="%s %s"/>' % (css, upper, lower))

    def value_labels(self, points):
        """points: list of (level, value, offset_y, text, x_shift)."""
        parts = []
        for level, value, dy, text, dx in points:
            parts.append(
                '<text class="endlabel" x="%.2f" y="%.2f">%s</text>'
                % (self.px(level) + dx, self.py(value) + dy, text))
        return "".join(parts)

    def arm_layers(self, stats):
        """Bands, then mean lines, then dots, for both arms."""
        return ("".join(self.ci_band(stats, i) for i in range(len(ARMS)))
                + "".join(self.mean_line(stats, i) for i in range(len(ARMS)))
                + "".join(self.scatter(stats, i) for i in range(len(ARMS))))


def legend_row(items):
    parts = []
    lx = 26
    for label, css in items:
        parts.append(
            '<line class="line %s" x1="%d" y1="73" x2="%d" y2="73"/>'
            % (css, lx, lx + 18))
        parts.append(
            '<circle class="pt %s" cx="%.2f" cy="73" r="4"/>' % (css, lx + 9))
        parts.append(
            '<text class="legend" x="%d" y="77">%s</text>' % (lx + 24, label))
        lx += 24 + int(len(label) * 6.4) + 34
    return "".join(parts)


# Per-role styling.  Colors name a palette token (resolved per mode); other
# properties are mode-independent.  Light colors are written as presentation
# attributes (rendered by every SVG consumer); the dark steps are applied by a
# prefers-color-scheme CSS block, which overrides presentation attributes in
# browsers (GitHub renders README SVGs in the viewer's browser).
ROLE_COLORS = {
    "surface": {"fill": "surface"},
    "grid": {"stroke": "grid"},
    "axis": {"stroke": "axis"},
    "tick": {"fill": "muted"},
    "axtitle": {"fill": "ink2"},
    "ptitle": {"fill": "ink"},
    "title": {"fill": "ink"},
    "subtitle": {"fill": "ink2"},
    "legend": {"fill": "ink2"},
    "endlabel": {"fill": "ink2"},
}
SERIES_COLORS = {
    "line": lambda s: {"stroke": s},
    "band": lambda s: {"fill": s},
    "pt": lambda s: {"fill": s, "stroke": "surface"},
}
ROLE_STATIC = {
    "grid": {"stroke-width": "1", "fill": "none"},
    "axis": {"stroke-width": "1", "fill": "none"},
    "tick": {"font-size": "11.5"},
    "axtitle": {"font-size": "12.5"},
    "ptitle": {"font-size": "13", "font-weight": "600"},
    "title": {"font-size": "15", "font-weight": "600"},
    "subtitle": {"font-size": "12"},
    "legend": {"font-size": "12"},
    "endlabel": {"font-size": "11.5"},
    "line": {"fill": "none", "stroke-width": "2", "stroke-linecap": "round",
             "stroke-linejoin": "round"},
    "band": {"fill-opacity": "0.12", "stroke": "none"},
    "pt": {"stroke-width": "2"},
}


def role_colors(classes):
    """Token map {css property: palette token} for an element's class list."""
    out = {}
    series = next((c for c in classes if c in ("s1", "s2")), None)
    for cls in classes:
        out.update(ROLE_COLORS.get(cls, {}))
        if series and cls in SERIES_COLORS:
            out.update(SERIES_COLORS[cls](series))
    return out


def paint(body):
    """Inject light-mode presentation attributes; collect class combos used."""
    combos = set()

    def repl(match):
        classes = match.group(1).split()
        combos.add(tuple(classes))
        attrs = {}
        for cls in classes:
            attrs.update(ROLE_STATIC.get(cls, {}))
        attrs.update({prop: LIGHT[tok]
                      for prop, tok in role_colors(classes).items()})
        rendered = " ".join('%s="%s"' % (k, v) for k, v in sorted(attrs.items()))
        return 'class="%s" %s' % (match.group(1), rendered)

    return re.sub(r'class="([^"]+)"', repl, body), combos


def dark_rules(combos):
    rules = []
    for classes in sorted(combos):
        colors = role_colors(classes)
        if not colors:
            continue
        selector = "." + ".".join(classes)
        decl = " ".join("%s: %s;" % (p, DARK[t]) for p, t in sorted(colors.items()))
        rules.append("      %s { %s }" % (selector, decl))
    return rules


def svg_shell(width, height, title, subtitle, body):
    body = ('<rect class="surface" x="0" y="0" width="%d" height="%d"/>'
            % (width, height)
            + '<text class="title" x="26" y="30">%s</text>' % title
            + '<text class="subtitle" x="26" y="50">%s</text>' % subtitle
            + body)
    body, combos = paint(body)
    parts = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append(
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
        'viewBox="0 0 %d %d" role="img" aria-label="%s" font-family="%s">'
        % (width, height, width, height, title, FONT_STACK))
    parts.append('  <title>%s</title>' % title)
    parts.append('  <desc>%s</desc>' % subtitle)
    parts.append('  <style>')
    parts.append('    @media (prefers-color-scheme: dark) {')
    parts.extend(dark_rules(combos))
    parts.append('    }')
    parts.append('  </style>')
    parts.append('  ' + body.replace("><", ">\n  <"))
    parts.append('</svg>')
    return "\n".join(parts) + "\n"


# ---------------------------------------------------------------- charts

def build_throughput_chart(cells):
    stats = arm_stats(cells, "throughput_rps")
    data_max = max(s[2][1] for s in stats.values())
    ymax, ticks = nice_ticks(data_max, target=7)

    width, height = 760, 470
    panel = Panel(72, width - 24, 100, height - 56, ymax, ticks,
                  fmt_thousands, "Concurrency (virtual users)",
                  "Throughput (requests/s)", 56)

    direct_c64 = stats[("direct", 64)][1]
    ray_c64 = stats[("ray_ha", 64)][1]
    direct_c32 = stats[("direct", 32)][1]
    labels = panel.value_labels([
        (64, direct_c64, 4, fmt_thousands(direct_c64), 28),
        (64, ray_c64, 4, fmt_thousands(ray_c64), 28),
        (32, direct_c32, -12, fmt_thousands(direct_c32), -14),
    ])
    body = legend_row([(name, css) for _, name, css, _ in ARMS])
    body += panel.frame() + panel.arm_layers(stats) + labels
    return svg_shell(width, height,
                     "Throughput vs concurrency (GPU rerun, gblock1)",
                     "Mean line with 95% CI band; points are individual trials"
                     " (n=5 per arm per level)",
                     body)


def build_latency_chart(cells):
    p95 = arm_stats(cells, "p95_ms")
    p99 = arm_stats(cells, "p99_ms")
    data_max = max(s[2][1] for s in list(p95.values()) + list(p99.values()))
    ymax, ticks = nice_ticks(data_max, target=7)

    width, height = 1184, 490
    panel_w = 500
    gap = 96
    top = 132
    p1 = Panel(64, 64 + panel_w, top, height - 56, ymax, ticks,
               lambda v: "%d" % v, "Concurrency (virtual users)",
               "p95 latency (ms)", 46)
    p2 = Panel(64 + panel_w + gap, 64 + 2 * panel_w + gap, top, height - 56,
               ymax, ticks, lambda v: "%d" % v,
               "Concurrency (virtual users)", "p99 latency (ms)", 46)

    # Selective value labels: the C=64 endpoints per arm per panel (right of
    # the dots), plus Direct's p99 spike at C=32.
    def labels_for(panel, stats, spike=False):
        points = [(64, stats[("direct", 64)][1], 4,
                   "%.1f" % stats[("direct", 64)][1], 28),
                  (64, stats[("ray_ha", 64)][1], 4,
                   "%.1f" % stats[("ray_ha", 64)][1], 28)]
        if spike:
            points.append((32, stats[("direct", 32)][1], -12,
                           "%.0f" % stats[("direct", 32)][1], -12))
        return panel.value_labels(points)

    body = legend_row([(name, css) for _, name, css, _ in ARMS])
    body += ('<text class="ptitle" x="%.2f" y="116" text-anchor="middle">p95</text>'
             % ((p1.x0 + p1.x1) / 2.0))
    body += ('<text class="ptitle" x="%.2f" y="116" text-anchor="middle">p99</text>'
             % ((p2.x0 + p2.x1) / 2.0))
    body += (p1.frame() + p1.arm_layers(p95) + labels_for(p1, p95))
    body += (p2.frame() + p2.arm_layers(p99) + labels_for(p2, p99, spike=True))

    return svg_shell(width, height,
                     "Tail latency vs concurrency (p95 and p99, GPU rerun, gblock1)",
                     "Mean lines with 95% CI bands; points are individual trials"
                     " (n=5 per arm per level)",
                     body)


def write_file(path, svg):
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(svg)
    print("[OK] wrote %s" % path)


def main():
    ap = argparse.ArgumentParser(
        description="Generate the Phase 3 GPU rerun SVG charts.")
    ap.add_argument("--data", required=True,
                    help="path to phase3-raw.json (60 trial records)")
    ap.add_argument("--output-throughput",
                    default=DEFAULT_OUTPUTS["throughput"])
    ap.add_argument("--output-latency", default=DEFAULT_OUTPUTS["latency"])
    args = ap.parse_args()

    cells = load_trials(args.data)
    write_file(args.output_throughput, build_throughput_chart(cells))
    write_file(args.output_latency, build_latency_chart(cells))


if __name__ == "__main__":
    main()
