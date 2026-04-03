import os

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PARQUET_PATH = os.path.join(os.path.dirname(__file__), "../../data/rankings_cache.parquet")

GRADE_BINS   = [0.0, 0.08, 0.15, 0.25, 0.40, 0.55, 0.70, 0.83, 1.0]
GRADE_LABELS = ["A+", "A",  "A-", "B+", "B",  "B-", "C+", "C"]

GRADE_COLORS = {
    "A+": "background-color: #1B5E20; color: #A5D6A7",
    "A":  "background-color: #2E7D32; color: #C8E6C9",
    "A-": "background-color: #388E3C; color: #DCEDC8",
    "B+": "background-color: #E65100; color: #FFE0B2",
    "B":  "background-color: #EF6C00; color: #FFF3E0",
    "B-": "background-color: #F57C00; color: #FFF8E1",
    "C+": "background-color: #B71C1C; color: #FFCDD2",
    "C":  "background-color: #C62828; color: #FFEBEE",
}

DISPLAY_COLS = [
    "rank", "Player", "dg_rank",
    "win_pct", "top5_pct", "top10_pct",
    "mc_pct", "fit_grade", "vs_dg",
    # hidden
    "datagolf_id", "player_name", "country", "country_code",
    "flag", "augusta_fit_score", "dg_win_pct", "dg_win_pct_pct", "model_type",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def assign_fit_grades(series: pd.Series) -> pd.Series:
    return pd.qcut(
        series.rank(method="first"),
        q=GRADE_BINS,
        labels=GRADE_LABELS,
    ).astype(str)


def country_code_to_flag(code: str) -> str:
    if not code or len(code) != 2:
        return ""
    return (
        chr(ord(code[0].upper()) - ord("A") + 0x1F1E6)
        + chr(ord(code[1].upper()) - ord("A") + 0x1F1E6)
    )


def format_player_name(name: str) -> str:
    parts = name.split(", ", 1)
    return f"{parts[1]} {parts[0]}" if len(parts) == 2 else name


# ---------------------------------------------------------------------------
# Data loading (cached)
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
# Page layout
# ---------------------------------------------------------------------------

# Header
col_left, col_right = st.columns([3, 1])
with col_left:
    st.markdown(
        "<h2 style='font-size:1.8rem; font-weight:700; margin-bottom:0;'>"
        "2026 Masters Tournament — Pre-Tournament Model</h2>",
        unsafe_allow_html=True,
    )
with col_right:
    st.markdown(
        "<div style='text-align:right; padding-top:14px; font-weight:600; color:#4CAF50;'>"
        "Augusta National &nbsp;·&nbsp; Apr 9–12</div>",
        unsafe_allow_html=True,
    )

st.caption("50,000 simulations · Augusta fit model · Updated Apr 2, 2026")
st.divider()

# Controls row
ctrl_model, ctrl_filter, ctrl_sort = st.columns([2, 4, 2])

with ctrl_model:
    model_choice = st.segmented_control(
        label="Model",
        options=["regression", "manual"],
        format_func=lambda x: "Regression weights" if x == "regression" else "Manual weights",
        key="model_type",
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
        "<div style='text-align:right; padding-top:8px; color:#888; font-size:0.85rem;'>"
        "Sort by: Win %</div>",
        unsafe_allow_html=True,
    )

# Load data
df = load_rankings(model_choice)

# ---------------------------------------------------------------------------
# Transformations (applied to full field before any filtering)
# ---------------------------------------------------------------------------

for col in ["win_pct", "top5_pct", "top10_pct", "mc_pct"]:
    df[col] = df[col] * 100

df["dg_win_pct_pct"] = df["dg_win_pct"] * 100

# vs-DG delta — null DG win% → delta = 0
df["vs_dg"] = df["win_pct"] - df["dg_win_pct_pct"].fillna(df["win_pct"])

# Augusta fit grade (stable over full field before filter)
df["fit_grade"] = assign_fit_grades(df["augusta_fit_score"])

# Player display: "First Last" + newline + flag + country name
df["flag"] = df["country_code"].apply(country_code_to_flag)
df["Player"] = (
    df["player_name"].apply(format_player_name)
    + "\n"
    + df["flag"] + " " + df["country"].fillna("")
)

df.insert(0, "rank", range(1, len(df) + 1))

# ---------------------------------------------------------------------------
# Apply filter
# ---------------------------------------------------------------------------

if filter_choice == "Top 20":
    display_df = df.head(20)
elif filter_choice == "Contenders (win >3%)":
    display_df = df[df["win_pct"] > 3.0]
else:
    display_df = df

# Enforce column order
display_df = display_df[DISPLAY_COLS]

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------

def _style_row(row):
    styles = [""] * len(row)
    idx = row.index.tolist()

    if "fit_grade" in idx:
        styles[idx.index("fit_grade")] = GRADE_COLORS.get(row["fit_grade"], "")

    if "vs_dg" in idx:
        v = row["vs_dg"]
        if isinstance(v, (int, float)):
            if v > 0:
                styles[idx.index("vs_dg")] = "color: #4CAF50; font-weight: bold"
            elif v < 0:
                styles[idx.index("vs_dg")] = "color: #E53935; font-weight: bold"

    return styles


styled = (
    display_df.style
    .bar(subset=["win_pct"],   color="#2E7D32", vmin=0, vmax=display_df["win_pct"].max())
    .bar(subset=["top5_pct"],  color="#1565C0", vmin=0, vmax=display_df["top5_pct"].max())
    .bar(subset=["top10_pct"], color="#E65100", vmin=0, vmax=display_df["top10_pct"].max())
    .apply(_style_row, axis=1)
)

# ---------------------------------------------------------------------------
# Column config
# ---------------------------------------------------------------------------

column_config = {
    "rank":      st.column_config.NumberColumn("#",          width="small",  format="%d"),
    "Player":    st.column_config.TextColumn("Player",       width="medium"),
    "dg_rank":   st.column_config.NumberColumn("DG Rank",    width="small",  format="%d"),
    "win_pct":   st.column_config.NumberColumn("Win %",      format="%.1f%%", width="medium"),
    "top5_pct":  st.column_config.NumberColumn("Top 5 %",    format="%.1f%%", width="medium"),
    "top10_pct": st.column_config.NumberColumn("Top 10 %",   format="%.1f%%", width="medium"),
    "mc_pct":    st.column_config.NumberColumn("Make Cut",   format="%.0f%%", width="small"),
    "fit_grade": st.column_config.TextColumn("Augusta Fit",  width="small"),
    "vs_dg":     st.column_config.NumberColumn(
                     "vs DG", format="%+.1f%%", width="small",
                     help="Your model win% minus DataGolf's pre-tournament win%"),
    # Hidden
    "datagolf_id":       None,
    "player_name":       None,
    "country":           None,
    "country_code":      None,
    "flag":              None,
    "augusta_fit_score": None,
    "dg_win_pct":        None,
    "dg_win_pct_pct":    None,
    "model_type":        None,
}

# ---------------------------------------------------------------------------
# Render table
# ---------------------------------------------------------------------------

st.dataframe(
    styled,
    column_config=column_config,
    hide_index=True,
    use_container_width=True,
    key="rankings_table",
    on_select="rerun",
    selection_mode="single-row",
)

# Footer
n_showing = len(display_df)
n_total = len(df)
st.caption(
    f"Showing {n_showing} of {n_total} players · Click any row for full player breakdown  "
    f"· vs DG Model = your model win% minus DataGolf's pre-tournament win%"
)
