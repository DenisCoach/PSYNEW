import matplotlib
matplotlib.use("Agg")  # non-interactive backend, must be before pyplot import

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from io import BytesIO
from datetime import date, timedelta
from typing import Optional

from database import get_activities_for_period, get_weekly_dynamics

# ── Colour map: emoji → hex ───────────────────────────────────────────────────
EMOJI_TO_HEX = {
    "🟥": "#E74C3C",
    "🟧": "#E67E22",
    "🟨": "#F4D03F",
    "🟩": "#27AE60",
    "🟦": "#2980B9",
    "🟪": "#8E44AD",
    "🟫": "#7D6608",
    "⬛": "#2C3E50",
    "🔴": "#C0392B",
    "🔵": "#1A5276",
    "🟤": "#A04000",
    "⚪": "#BDC3C7",
}

EMPTY_COLOR = "#ECECEC"
BG_COLOR    = "#FAFAFA"
TEXT_COLOR  = "#2C3E50"
WEEKDAYS_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


async def generate_grid(user_id: int, start: date, end: date, title: str) -> BytesIO:
    activities = await get_activities_for_period(
        user_id, start.isoformat(), end.isoformat()
    )

    # {date_str: {hour: {ctx_name: total_minutes}}}
    data: dict = {}
    ctx_colors: dict = {}

    for act_date, hour, ctx_name, emoji, _desc, dur in activities:
        color = EMOJI_TO_HEX.get(emoji, "#95A5A6")
        ctx_colors[ctx_name] = color
        data.setdefault(act_date, {}).setdefault(hour, {})
        data[act_date][hour][ctx_name] = data[act_date][hour].get(ctx_name, 0) + dur

    # Build ordered day list
    days = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)

    n_days  = len(days)
    n_hours = 24
    cw = 0.72   # cell width
    ch = 0.52   # cell height

    legend_rows = max(1, (len(ctx_colors) + 3) // 4)
    legend_h    = legend_rows * 0.45 + 0.3

    fig_w = 2.4 + n_hours * cw
    fig_h = 1.0 + n_days * ch + legend_h

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    for di, day in enumerate(days):
        y       = (n_days - 1 - di) * ch
        day_str = day.isoformat()
        wd      = WEEKDAYS_RU[day.weekday()]
        label   = f"{wd} {day.strftime('%d.%m')}"

        # Day label on the left
        ax.text(
            -0.15, y + ch / 2, label,
            ha="right", va="center", fontsize=7.5,
            color=TEXT_COLOR, fontfamily="monospace",
        )

        for hour in range(n_hours):
            x = hour * cw
            if day_str in data and hour in data[day_str]:
                dominant = max(data[day_str][hour], key=data[day_str][hour].get)
                color    = ctx_colors[dominant]
                alpha    = 0.88
            else:
                color = EMPTY_COLOR
                alpha = 1.0

            ax.add_patch(plt.Rectangle(
                (x + 0.03, y + 0.03), cw - 0.06, ch - 0.06,
                facecolor=color, edgecolor="none", alpha=alpha,
            ))

        # Daily total hours on the right
        if day_str in data:
            day_total_m = sum(
                sum(data[day_str][h].values()) for h in data[day_str]
            )
            if day_total_m > 0:
                day_h = day_total_m / 60
                ax.text(
                    n_hours * cw + 0.18, y + ch / 2,
                    f"{day_h:.1f}ч",
                    ha="left", va="center", fontsize=7,
                    color=TEXT_COLOR, fontfamily="monospace",
                )

    # Hour labels (every 2 h)
    for hour in range(n_hours):
        if hour % 2 == 0:
            ax.text(
                hour * cw + cw / 2, n_days * ch + 0.08,
                f"{hour:02d}",
                ha="center", va="bottom", fontsize=7, color=TEXT_COLOR,
            )

    # Title
    ax.text(
        n_hours * cw / 2, n_days * ch + 0.42,
        title,
        ha="center", va="bottom", fontsize=11,
        fontweight="bold", color=TEXT_COLOR,
    )

    # Legend
    if ctx_colors:
        patches = [
            mpatches.Patch(facecolor=c, label=name, edgecolor="none")
            for name, c in sorted(ctx_colors.items())
        ]
        ax.legend(
            handles=patches,
            loc="upper center",
            bbox_to_anchor=(n_hours * cw / 2, -0.12),
            ncol=4,
            fontsize=8.5,
            frameon=False,
            handlelength=1.4,
            handleheight=0.9,
        )

    ax.set_xlim(-0.1, n_hours * cw + 1.2)
    ax.set_ylim(-legend_h, n_days * ch + 0.65)
    ax.axis("off")

    plt.tight_layout(pad=0.2)

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, facecolor=BG_COLOR, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return buf


async def generate_dynamics(user_id: int, weeks: int = 8) -> Optional[BytesIO]:
    """Line chart of weekly hours per context over last N weeks."""
    rows = await get_weekly_dynamics(user_id, weeks)
    if not rows:
        return None

    # Organise: ctx_name → week_key → hours
    ctx_colors: dict = {}
    ctx_data: dict = {}
    week_labels: dict = {}

    for week_key, ctx_name, emoji, total_m, week_start_date in rows:
        color = EMOJI_TO_HEX.get(emoji, "#95A5A6")
        ctx_colors[ctx_name] = color
        ctx_data.setdefault(ctx_name, {})[week_key] = total_m / 60
        if week_key not in week_labels:
            week_labels[week_key] = week_start_date[5:].replace("-", ".")  # MM.DD

    all_weeks = sorted(week_labels)
    x_pos     = list(range(len(all_weeks)))
    x_labels  = [week_labels[w] for w in all_weeks]

    fig_w = max(7, len(all_weeks) * 1.1)
    fig, ax = plt.subplots(figsize=(fig_w, 4.5))
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    for ctx_name, week_data in sorted(ctx_data.items(), key=lambda kv: -sum(kv[1].values())):
        color  = ctx_colors[ctx_name]
        y_vals = [week_data.get(w, 0) for w in all_weeks]
        ax.plot(x_pos, y_vals, marker="o", color=color, linewidth=2,
                label=ctx_name, markersize=5)
        # Label last point
        last_y = y_vals[-1]
        if last_y > 0:
            ax.annotate(f"{last_y:.1f}ч", (x_pos[-1], last_y),
                        textcoords="offset points", xytext=(5, 2),
                        fontsize=7, color=color)

    ax.set_xticks(x_pos)
    ax.set_xticklabels(x_labels, fontsize=8, color=TEXT_COLOR)
    ax.set_ylabel("Часов / неделя", fontsize=9, color=TEXT_COLOR)
    ax.set_title("📈 Динамика по неделям", fontsize=11, fontweight="bold", color=TEXT_COLOR, pad=10)
    ax.legend(fontsize=8, frameon=False, loc="upper left")
    ax.grid(True, axis="y", alpha=0.25, color=TEXT_COLOR)
    ax.tick_params(colors=TEXT_COLOR)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    for spine in ["bottom", "left"]:
        ax.spines[spine].set_color("#CCCCCC")

    plt.tight_layout(pad=0.5)

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, facecolor=BG_COLOR, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return buf
