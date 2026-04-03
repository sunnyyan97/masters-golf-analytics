import os
from datetime import date

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PARQUET_PATH = os.path.join(os.path.dirname(__file__), "../../data/rankings_cache.parquet")

# DataGolf 3-letter country code → 2-letter ISO for flag emoji conversion
COUNTRY_TO_ISO2 = {
    "USA": "US", "ENG": "GB", "SCO": "GB", "WAL": "GB", "NIR": "GB",
    "AUS": "AU", "ESP": "ES", "JPN": "JP", "KOR": "KR", "RSA": "ZA",
    "CAN": "CA", "IRL": "IE", "GER": "DE", "FRA": "FR", "ITA": "IT",
    "SWE": "SE", "NOR": "NO", "DEN": "DK", "FIN": "FI", "ARG": "AR",
    "COL": "CO", "MEX": "MX", "CHN": "CN", "NZL": "NZ", "AUT": "AT",
    "BEL": "BE", "NED": "NL", "CZE": "CZ", "THA": "TH", "FIJ": "FJ",
}

# Grade badge colors: (background, text)
GRADE_BADGE = {
    "A+": ("#1B5E20", "#A5D6A7"),
    "A":  ("#2E7D32", "#C8E6C9"),
    "B+": ("#004D40", "#80CBC4"),
    "B":  ("#0D47A1", "#90CAF9"),
    "C+": ("#E65100", "#FFCC80"),
    "C":  ("#B71C1C", "#EF9A9A"),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def assign_fit_grades(series: pd.Series) -> pd.Series:
    """Percentile-rank based grades per DEVLOG Phase 5 spec.

    Uses PERCENT_RANK() DESC logic: top scorer → pct_rank near 0.
    A+ ≤ 10%, A ≤ 25%, B+ ≤ 45%, B ≤ 65%, C+ ≤ 80%, C = rest.
    """
    # rank(pct=True, ascending=False): rank 1 → highest score → pct ≈ 1/n ≈ 0
    pct = series.rank(pct=True, ascending=False)

    def _grade(p: float) -> str:
        if p <= 0.10: return "A+"
        if p <= 0.25: return "A"
        if p <= 0.45: return "B+"
        if p <= 0.65: return "B"
        if p <= 0.80: return "C+"
        return "C"

    return pct.apply(_grade)


def iso2_flag(code3: str) -> str:
    iso2 = COUNTRY_TO_ISO2.get((code3 or "").upper(), "")
    if len(iso2) != 2:
        return ""
    return (
        chr(ord(iso2[0]) - ord("A") + 0x1F1E6)
        + chr(ord(iso2[1]) - ord("A") + 0x1F1E6)
    )


def format_name(name: str) -> str:
    """Convert 'Last, First' → 'First Last'."""
    parts = name.split(", ", 1)
    return f"{parts[1]} {parts[0]}" if len(parts) == 2 else name


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_rankings(model_type: str) -> pd.DataFrame:
    df = pd.read_parquet(PARQUET_PATH)
    return (
        df[df["model_type"] == model_type]
        .sort_values("win_pct", ascending=False)
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# HTML table builder
# ---------------------------------------------------------------------------

def _prob_bar(value: float, max_val: float, color: str) -> str:
    """Inline probability bar: colored fill + numeric label."""
    width = (value / max_val * 100) if max_val > 0 else 0
    return (
        f'<div style="display:flex;align-items:center;gap:6px;min-width:90px">'
        f'  <div style="flex:0 0 60px;background:#2a2d35;height:7px;border-radius:4px;overflow:hidden">'
        f'    <div style="width:{width:.1f}%;background:{color};height:7px;border-radius:4px"></div>'
        f'  </div>'
        f'  <span style="font-size:12px;color:#e0e0e0;white-space:nowrap">{value:.1f}%</span>'
        f'</div>'
    )


def _grade_badge(grade: str) -> str:
    bg, fg = GRADE_BADGE.get(grade, ("#555", "#eee"))
    return (
        f'<span style="background:{bg};color:{fg};padding:2px 8px;'
        f'border-radius:4px;font-weight:700;font-size:12px;'
        f'letter-spacing:0.03em;white-space:nowrap">{grade}</span>'
    )


def _vs_dg_cell(dg_win_pct, model_win_pct: float) -> str:
    """Return colored delta HTML, or a muted placeholder if DG data unavailable."""
    if pd.isna(dg_win_pct):
        return '<span style="color:#555;font-size:12px">—</span>'
    delta = model_win_pct - (dg_win_pct * 100)
    if delta > 0:
        return f'<span style="color:#4CAF50;font-weight:600;font-size:12px">+{delta:.1f}%</span>'
    elif delta < 0:
        return f'<span style="color:#E53935;font-weight:600;font-size:12px">{delta:.1f}%</span>'
    else:
        return f'<span style="color:#888;font-size:12px">0.0%</span>'


def build_html_table(display_df: pd.DataFrame) -> str:
    max_win  = display_df["win_pct"].max()
    max_top5 = display_df["top5_pct"].max()
    max_top10 = display_df["top10_pct"].max()

    rows = []
    for _, row in display_df.iterrows():
        flag   = iso2_flag(row.get("country_code", ""))
        cc     = row.get("country_code") or ""
        name   = format_name(row.get("player_name", ""))
        dg_rank_val = int(row["dg_rank"]) if pd.notna(row["dg_rank"]) else "—"

        player_cell = (
            f'<div style="font-weight:600;font-size:13px;color:#f0f0f0">{name}</div>'
            f'<div style="font-size:11px;color:#888;margin-top:1px">{flag}&nbsp;{cc}</div>'
        )

        rows.append(
            f"<tr>"
            f'<td style="color:#888;font-size:12px;width:36px">{row["rank"]}</td>'
            f'<td style="padding:8px 12px">{player_cell}</td>'
            f'<td style="color:#aaa;font-size:12px;text-align:center">{dg_rank_val}</td>'
            f'<td>{_prob_bar(row["win_pct"],  max_win,  "#2E7D32")}</td>'
            f'<td>{_prob_bar(row["top5_pct"], max_top5, "#1565C0")}</td>'
            f'<td>{_prob_bar(row["top10_pct"],max_top10,"#E65100")}</td>'
            f'<td style="color:#ccc;font-size:12px;text-align:center">{row["mc_pct"]:.0f}%</td>'
            f'<td style="text-align:center">{_grade_badge(row["fit_grade"])}</td>'
            f'<td style="text-align:center">{_vs_dg_cell(row.get("dg_win_pct"), row["win_pct"])}</td>'
            f"</tr>"
        )

    rows_html = "\n".join(rows)

    return f"""
<style>
  .rk-wrap {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    overflow-x: auto;
  }}
  .rk-table {{
    width: 100%;
    border-collapse: collapse;
    table-layout: auto;
  }}
  .rk-table th {{
    background: #1A1D23;
    color: #9aa0ad;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 8px 10px;
    border-bottom: 1px solid #2a2d35;
    white-space: nowrap;
  }}
  .rk-table td {{
    padding: 7px 10px;
    border-bottom: 1px solid #1e2028;
    vertical-align: middle;
    background: transparent;
  }}
  .rk-table tbody tr:hover td {{
    background: #1e2230;
  }}
</style>
<div class="rk-wrap">
  <table class="rk-table">
    <thead>
      <tr>
        <th>#</th>
        <th>Player</th>
        <th style="text-align:center">DG Rank</th>
        <th>Win %</th>
        <th>Top 5 %</th>
        <th>Top 10 %</th>
        <th style="text-align:center">Make Cut</th>
        <th style="text-align:center">Augusta Fit</th>
        <th style="text-align:center">vs DG</th>
      </tr>
    </thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
</div>
"""


# ---------------------------------------------------------------------------
# Page layout — header
# ---------------------------------------------------------------------------

today_str = date.today().strftime("%b %-d, %Y")

col_left, col_right = st.columns([3, 1])
with col_left:
    st.markdown(
        "<h2 style='font-size:1.75rem;font-weight:700;margin-bottom:0'>"
        "2026 Masters Tournament — Pre-Tournament Model</h2>",
        unsafe_allow_html=True,
    )
with col_right:
    st.markdown(
        "<div style='text-align:right;padding-top:16px;font-weight:600;color:#4CAF50'>"
        "Augusta National &nbsp;·&nbsp; Apr 9–12</div>",
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Controls row
# ---------------------------------------------------------------------------

ctrl_model, ctrl_filter, ctrl_sort = st.columns([3, 4, 2])

with ctrl_model:
    model_choice = st.segmented_control(
        label="Model",
        options=["manual", "regression", "ensemble"],
        format_func=lambda x: x.capitalize(),
        default=st.session_state.get("selected_model", "ensemble"),
        key="selected_model",
        label_visibility="collapsed",
    )

with ctrl_filter:
    filter_choice = st.pills(
        label="View",
        options=["All players", "Top 20", "Contenders (win >3%)"],
        default="All players",
        selection_mode="single",
    )

with ctrl_sort:
    st.markdown(
        "<div style='text-align:right;padding-top:10px;color:#666;font-size:0.82rem'>"
        "Sort&thinsp;by:&nbsp;Win&nbsp;%</div>",
        unsafe_allow_html=True,
    )

# Guard: segmented_control returns None until first interaction
active_model = model_choice or st.session_state.get("selected_model", "ensemble")

# ---------------------------------------------------------------------------
# Load + transform data
# ---------------------------------------------------------------------------

df = load_rankings(active_model)

for col in ["win_pct", "top5_pct", "top10_pct", "mc_pct"]:
    df[col] = df[col] * 100

# Fit grades computed over full field before any filter
df["fit_grade"] = assign_fit_grades(df["augusta_fit_score"])

df.insert(0, "rank", range(1, len(df) + 1))

# ---------------------------------------------------------------------------
# Apply filter
# ---------------------------------------------------------------------------

if filter_choice == "Top 20":
    display_df = df.head(20).copy()
elif filter_choice == "Contenders (win >3%)":
    display_df = df[df["win_pct"] > 3.0].copy()
else:
    display_df = df.copy()

# ---------------------------------------------------------------------------
# Subtitle (depends on sim count — read from parquet metadata if available,
# otherwise fall back to a sensible default)
# ---------------------------------------------------------------------------

st.caption(f"100,000 simulations · Augusta fit model · Updated {today_str}")
st.divider()

# ---------------------------------------------------------------------------
# Render HTML table
# ---------------------------------------------------------------------------

html = build_html_table(display_df)
st.markdown(html, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

n_showing = len(display_df)
n_total   = len(df)
st.markdown(
    f"<div style='font-size:12px;color:#666;margin-top:10px;padding-top:6px;"
    f"border-top:1px solid #2a2d35'>"
    f"Showing {n_showing} of {n_total} players &nbsp;·&nbsp; "
    f"Click any row for full player breakdown &nbsp;·&nbsp; "
    f"<em>vs DG</em> = your win% minus DataGolf's pre-tournament win% "
    f"(placeholder — DG 2026 predictions not yet ingested)"
    f"</div>",
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Player selector (for cross-page navigation to Page 2)
# ---------------------------------------------------------------------------

with st.expander("Select player for deep dive →", expanded=False):
    all_names = df["player_name"].apply(format_name).tolist()
    selected = st.selectbox(
        "Player",
        options=all_names,
        index=0,
        key="_player_selectbox",
        label_visibility="collapsed",
    )
    if selected:
        st.session_state["selected_player"] = selected
