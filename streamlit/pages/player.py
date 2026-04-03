"""Page 2 — Player Deep Dive.

Reads mart_player_model_inputs + mart_simulation_results via read_only DuckDB.
Reads dg_win_pct from rankings_cache.parquet (avoids write-lock conflicts).
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_DB_PATH = os.path.join(os.path.dirname(__file__), "../../data/masters.duckdb")
_PARQUET_PATH = os.path.join(os.path.dirname(__file__), "../../data/rankings_cache.parquet")

# ---------------------------------------------------------------------------
# Grade / badge helpers (mirrors rankings.py — same percentile thresholds)
# ---------------------------------------------------------------------------

GRADE_BADGE_CSS = {
    "A+": ("background:#1B5E20;color:#A5D6A7", "A+"),
    "A":  ("background:#2E7D32;color:#C8E6C9", "A"),
    "B+": ("background:#004D40;color:#80CBC4", "B+"),
    "B":  ("background:#0D47A1;color:#90CAF9", "B"),
    "C+": ("background:#E65100;color:#FFCC80", "C+"),
    "C":  ("background:#B71C1C;color:#EF9A9A", "C"),
}


def assign_fit_grades(series: pd.Series) -> pd.Series:
    pct = series.rank(pct=True, ascending=False)

    def _g(p: float) -> str:
        if p <= 0.10: return "A+"
        if p <= 0.25: return "A"
        if p <= 0.45: return "B+"
        if p <= 0.65: return "B"
        if p <= 0.80: return "C+"
        return "C"

    return pct.apply(_g)


def grade_badge_html(grade: str) -> str:
    style, label = GRADE_BADGE_CSS.get(grade, ("background:#555;color:#eee", grade))
    return (
        f'<span style="{style};padding:3px 10px;border-radius:5px;'
        f'font-weight:700;font-size:14px;letter-spacing:0.04em">{label}</span>'
    )


def format_name(name: str) -> str:
    parts = name.split(", ", 1)
    return f"{parts[1]} {parts[0]}" if len(parts) == 2 else name


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_all_model_inputs() -> pd.DataFrame:
    """Full field from DuckDB — used for ranking within the field."""
    conn = duckdb.connect(_DB_PATH, read_only=True)
    df = conn.execute("SELECT * FROM mart_player_model_inputs").df()
    conn.close()
    return df


@st.cache_data(ttl=300)
def load_simulation_results() -> pd.DataFrame:
    """All sim results for all model types."""
    conn = duckdb.connect(_DB_PATH, read_only=True)
    df = conn.execute("SELECT * FROM mart_simulation_results").df()
    conn.close()
    return df


@st.cache_data(ttl=300)
def load_dg_predictions() -> pd.DataFrame:
    """DG win% per player from parquet (ensemble model rows have dg_win_pct)."""
    df = pd.read_parquet(_PARQUET_PATH)
    return df[df["model_type"] == "ensemble"][["datagolf_id", "dg_win_pct"]].copy()


# ---------------------------------------------------------------------------
# SG rank helper
# ---------------------------------------------------------------------------

def compute_sg_ranks(all_inputs: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with field rank for each SG category."""
    rank_cols = ["sg_app", "sg_putt", "sg_ott", "sg_arg", "sg_overall_rolling"]
    ranks = all_inputs[["datagolf_id"]].copy()
    for col in rank_cols:
        ranks[f"{col}_rank"] = all_inputs[col].rank(ascending=False, method="min").astype("Int64")
    return ranks


# ---------------------------------------------------------------------------
# HTML rendering helpers
# ---------------------------------------------------------------------------

def _stat_card(label: str, value_html: str, sub_html: str = "") -> str:
    return f"""
<div style="background:#1A1D23;border:1px solid #2a2d35;border-radius:8px;
            padding:14px 18px;min-width:130px;flex:1">
  <div style="font-size:11px;color:#9aa0ad;text-transform:uppercase;
              letter-spacing:0.06em;margin-bottom:6px">{label}</div>
  <div style="font-size:22px;font-weight:700;color:#f0f0f0;line-height:1.1">{value_html}</div>
  {f'<div style="font-size:11px;color:#666;margin-top:4px">{sub_html}</div>' if sub_html else ''}
</div>"""


def _pill(label: str, value_pct: float, color: str) -> str:
    return f"""
<div style="background:#1A1D23;border:1px solid #2a2d35;border-radius:10px;
            padding:16px 22px;text-align:center;flex:1;min-width:110px">
  <div style="font-size:11px;color:#9aa0ad;text-transform:uppercase;
              letter-spacing:0.06em;margin-bottom:8px">{label}</div>
  <div style="font-size:30px;font-weight:800;color:{color};line-height:1">{value_pct:.1f}%</div>
</div>"""


def _sg_cell(label: str, value: float | None, rank: int | None,
             weight_pct: int | None) -> str:
    if value is None or pd.isna(value):
        val_html = '<span style="color:#555">—</span>'
        sign_color = "#555"
    else:
        sign = "+" if value >= 0 else ""
        sign_color = "#4CAF50" if value >= 0 else "#E53935"
        val_html = f'<span style="color:{sign_color}">{sign}{value:.3f}</span>'

    rank_html = (
        f'<div style="font-size:11px;color:#9aa0ad;margin-top:2px">'
        f'#{rank} in field</div>'
        if rank and not pd.isna(rank) else ""
    )
    weight_html = (
        f'<div style="font-size:10px;color:#555;margin-top:3px">'
        f'Augusta weight: {weight_pct}%</div>'
        if weight_pct is not None else ""
    )

    return f"""
<div style="background:#1A1D23;border:1px solid #2a2d35;border-radius:8px;
            padding:14px 16px;flex:1;min-width:120px">
  <div style="font-size:11px;color:#9aa0ad;text-transform:uppercase;
              letter-spacing:0.05em;margin-bottom:6px">{label}</div>
  <div style="font-size:20px;font-weight:700;line-height:1">{val_html}</div>
  {rank_html}
  {weight_html}
</div>"""


def _model_input_row(label: str, weight: float, value: float | None,
                     contribution: float | None, dimmed: bool = False) -> str:
    alpha = "0.45" if dimmed else "1"
    if value is None or pd.isna(value):
        val_str = "—"
        val_color = "#555"
        contrib_str = "—"
    else:
        sign = "+" if value >= 0 else ""
        val_str = f"{sign}{value:.3f}"
        val_color = "#4CAF50" if value >= 0 else "#E53935"
        if contribution is not None and not pd.isna(contribution):
            csign = "+" if contribution >= 0 else ""
            contrib_str = f"{csign}{contribution:.3f}"
        else:
            contrib_str = "—"

    return f"""
<tr style="opacity:{alpha}">
  <td style="padding:8px 12px;color:#ccc;font-size:13px">{label}</td>
  <td style="padding:8px 12px;color:#666;font-size:12px;text-align:center">{weight:.0%}</td>
  <td style="padding:8px 12px;font-size:13px;font-weight:600;
             color:{val_color};text-align:right">{val_str}</td>
  <td style="padding:8px 12px;color:#aaa;font-size:12px;text-align:right">{contrib_str}</td>
</tr>"""


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

# -- Session-state defaults --------------------------------------------------
if "selected_model" not in st.session_state:
    st.session_state["selected_model"] = "ensemble"

# -- Load data ---------------------------------------------------------------
all_inputs = load_all_model_inputs()
sim_results = load_simulation_results()
dg_preds = load_dg_predictions()

# Pre-compute field-wide SG ranks
sg_ranks = compute_sg_ranks(all_inputs)

# Merge dg_win_pct onto all_inputs for reference
all_inputs = all_inputs.merge(dg_preds, on="datagolf_id", how="left")

# Compute fit grades over full field (percentile-based)
all_inputs["fit_grade"] = assign_fit_grades(all_inputs["augusta_fit_score"])

# -- Player selector ---------------------------------------------------------
player_names_raw = sorted(all_inputs["player_name"].tolist())
display_names = [format_name(n) for n in player_names_raw]

# Map display name → raw DB name
name_map = {format_name(n): n for n in player_names_raw}

# Pre-select from session state if set from Page 1
default_display = st.session_state.get("selected_player", display_names[0])
default_idx = display_names.index(default_display) if default_display in display_names else 0

st.markdown(
    "<h2 style='font-size:1.6rem;font-weight:700;margin-bottom:4px'>"
    "Player Deep Dive</h2>",
    unsafe_allow_html=True,
)

selected_display = st.selectbox(
    "Select player",
    options=display_names,
    index=default_idx,
    key="_player_deep_dive",
    label_visibility="collapsed",
)
st.session_state["selected_player"] = selected_display

raw_name = name_map[selected_display]

# -- Pull player row ---------------------------------------------------------
prow = all_inputs[all_inputs["player_name"] == raw_name].iloc[0]
prank_row = sg_ranks[sg_ranks["datagolf_id"] == prow["datagolf_id"]].iloc[0]

# Sim results for this player (all 3 model types)
psim = sim_results[sim_results["datagolf_id"] == prow["datagolf_id"]].copy()
psim_by_model = psim.set_index("model_type")

def sim_val(model: str, col: str, default: float = 0.0) -> float:
    if model in psim_by_model.index:
        return psim_by_model.loc[model, col]
    return default

st.divider()

# ---------------------------------------------------------------------------
# Section 1 — Probability pills (ensemble default, show all 3)
# ---------------------------------------------------------------------------

active_model = st.session_state.get("selected_model", "ensemble")

st.markdown(
    f"<div style='font-size:12px;color:#9aa0ad;margin-bottom:8px'>"
    f"Showing <strong style='color:#f0f0f0'>{active_model.capitalize()}</strong> model probabilities</div>",
    unsafe_allow_html=True,
)

pills_html = '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px">'
pills_html += _pill("Win %",      sim_val(active_model, "win_pct")   * 100, "#4CAF50")
pills_html += _pill("Top 5 %",    sim_val(active_model, "top5_pct")  * 100, "#1565C0")
pills_html += _pill("Top 10 %",   sim_val(active_model, "top10_pct") * 100, "#E65100")
pills_html += _pill("Make Cut %", sim_val(active_model, "mc_pct")    * 100, "#78909C")
pills_html += '</div>'
st.markdown(pills_html, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Section 2 — SG Breakdown grid
# ---------------------------------------------------------------------------

st.markdown(
    "<div style='font-size:13px;font-weight:600;color:#9aa0ad;text-transform:uppercase;"
    "letter-spacing:0.08em;margin-bottom:10px'>Strokes Gained — Current Form</div>",
    unsafe_allow_html=True,
)

sg_weights = {"sg_app": 28, "sg_putt": 20, "sg_ott": 4, "sg_arg": 18}
sg_labels  = {
    "sg_app":             "Approach",
    "sg_putt":            "Putting",
    "sg_ott":             "Off the Tee",
    "sg_arg":             "Around Green",
    "sg_overall_rolling": "SG Total",
}

sg_cells_html = '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px">'
for col, label in sg_labels.items():
    val = prow.get(col)
    rank_val = prank_row.get(f"{col}_rank")
    weight = sg_weights.get(col)
    sg_cells_html += _sg_cell(label, val, rank_val, weight)
sg_cells_html += '</div>'

st.markdown(sg_cells_html, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Section 3 — Augusta Profile
# ---------------------------------------------------------------------------

st.markdown(
    "<div style='font-size:13px;font-weight:600;color:#9aa0ad;text-transform:uppercase;"
    "letter-spacing:0.08em;margin-bottom:10px'>Augusta National Profile</div>",
    unsafe_allow_html=True,
)

n_appearances = int(prow["augusta_seasons_played"]) if pd.notna(prow["augusta_seasons_played"]) else 0
fit_grade = prow["fit_grade"]

dist_vs_avg = prow.get("driving_dist_vs_avg")
if dist_vs_avg is not None and not pd.isna(dist_vs_avg):
    sign = "+" if dist_vs_avg >= 0 else ""
    dist_html = f"{sign}{dist_vs_avg:.1f} yds"
    dist_color = "#4CAF50" if dist_vs_avg >= 0 else "#E53935"
    dist_val_html = f'<span style="color:{dist_color}">{dist_html}</span>'
else:
    dist_val_html = '<span style="color:#555">—</span>'

augusta_hist = prow.get("augusta_sg_total")
if augusta_hist is not None and not pd.isna(augusta_hist) and n_appearances > 0:
    hist_sign = "+" if augusta_hist >= 0 else ""
    hist_html = f"{hist_sign}{augusta_hist:.3f}"
    hist_color = "#4CAF50" if augusta_hist >= 0 else "#E53935"
    hist_val_html = f'<span style="color:{hist_color}">{hist_html}</span>'
    hist_sub = f"{n_appearances} Masters appearance{'s' if n_appearances != 1 else ''}"
else:
    hist_val_html = '<span style="color:#555">No history</span>'
    hist_sub = "No Augusta data (2019+)"

profile_html = '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px">'
profile_html += _stat_card(
    "Augusta Appearances",
    f'<span style="color:#f0f0f0">{n_appearances}</span>',
    "since 2019"
)
profile_html += _stat_card(
    "Augusta Hist SG",
    hist_val_html,
    hist_sub,
)
profile_html += _stat_card(
    "Driving vs Field",
    dist_val_html,
    "vs field avg (yards)"
)
profile_html += _stat_card(
    "Augusta Fit",
    grade_badge_html(fit_grade),
    f"fit score: {prow['augusta_fit_score']:.3f}"
)
profile_html += '</div>'
st.markdown(profile_html, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Section 4 — Model inputs panel
# ---------------------------------------------------------------------------

st.markdown(
    "<div style='font-size:13px;font-weight:600;color:#9aa0ad;text-transform:uppercase;"
    "letter-spacing:0.08em;margin-bottom:10px'>Manual Model Inputs</div>",
    unsafe_allow_html=True,
)

sg_overall   = prow.get("sg_overall_rolling")
aug_hist_sg  = prow.get("augusta_sg_total")
fit_score    = prow.get("augusta_fit_score")
momentum     = prow.get("momentum_delta")
aug_mu       = prow.get("augusta_mu")

# Contributions (before activity discount) — weight × value
def contrib(weight: float, value) -> float | None:
    if value is None or pd.isna(value):
        return None
    return weight * value

aug_hist_dimmed = (aug_hist_sg is None or pd.isna(aug_hist_sg) or
                   (n_appearances == 0))

activity_label = ""
is_null = bool(prow.get("sg_overall_is_null", False))
recent = prow.get("recent_starts", 8)
if is_null or (recent is not None and recent < 4):
    activity_label = " &nbsp;<span style='color:#E65100;font-size:11px'>(activity discount ×0.60)</span>"
elif recent is not None and recent < 8:
    activity_label = " &nbsp;<span style='color:#E65100;font-size:11px'>(activity discount ×0.82)</span>"

inputs_table = f"""
<table style="width:100%;border-collapse:collapse;background:#1A1D23;
              border:1px solid #2a2d35;border-radius:8px;overflow:hidden;
              margin-bottom:20px">
  <thead>
    <tr style="background:#141619">
      <th style="padding:8px 12px;text-align:left;font-size:11px;color:#9aa0ad;
                 text-transform:uppercase;letter-spacing:0.05em;font-weight:600">
        Component</th>
      <th style="padding:8px 12px;text-align:center;font-size:11px;color:#9aa0ad;
                 text-transform:uppercase;letter-spacing:0.05em;font-weight:600">
        Weight</th>
      <th style="padding:8px 12px;text-align:right;font-size:11px;color:#9aa0ad;
                 text-transform:uppercase;letter-spacing:0.05em;font-weight:600">
        Value</th>
      <th style="padding:8px 12px;text-align:right;font-size:11px;color:#9aa0ad;
                 text-transform:uppercase;letter-spacing:0.05em;font-weight:600">
        Contribution</th>
    </tr>
  </thead>
  <tbody>
    {_model_input_row("SG Overall (rolling)", 0.40, sg_overall, contrib(0.40, sg_overall))}
    {_model_input_row("Augusta Hist SG", 0.30, aug_hist_sg, contrib(0.30, aug_hist_sg), dimmed=aug_hist_dimmed)}
    {_model_input_row("Augusta Fit Score", 0.20, fit_score, contrib(0.20, fit_score))}
    {_model_input_row("Trajectory (momentum)", 0.10, momentum, contrib(0.10, momentum))}
  </tbody>
</table>"""
st.markdown(inputs_table, unsafe_allow_html=True)

if aug_mu is not None and not pd.isna(aug_mu):
    mu_sign = "+" if aug_mu >= 0 else ""
    st.markdown(
        f"<div style='font-size:12px;color:#9aa0ad;margin-top:-12px;margin-bottom:20px'>"
        f"Augusta μ (manual, after activity discount): "
        f"<strong style='color:#f0f0f0'>{mu_sign}{aug_mu:.3f}</strong>{activity_label}</div>",
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Section 5 — vs DG comparison panel
# ---------------------------------------------------------------------------

st.markdown(
    "<div style='font-size:13px;font-weight:600;color:#9aa0ad;text-transform:uppercase;"
    "letter-spacing:0.08em;margin-bottom:10px'>Win Probability Comparison</div>",
    unsafe_allow_html=True,
)

dg_row = dg_preds[dg_preds["datagolf_id"] == prow["datagolf_id"]]
dg_win = float(dg_row["dg_win_pct"].iloc[0]) * 100 if len(dg_row) and not pd.isna(dg_row["dg_win_pct"].iloc[0]) else None

def _model_pill(label: str, pct: float | None, color: str = "#f0f0f0") -> str:
    val_str = f"{pct:.1f}%" if pct is not None else "—"
    return f"""
<div style="background:#1A1D23;border:1px solid #2a2d35;border-radius:8px;
            padding:12px 18px;text-align:center;flex:1;min-width:100px">
  <div style="font-size:11px;color:#9aa0ad;text-transform:uppercase;
              letter-spacing:0.05em;margin-bottom:6px">{label}</div>
  <div style="font-size:22px;font-weight:700;color:{color}">{val_str}</div>
</div>"""

comparison_html = '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:20px">'
comparison_html += _model_pill("Manual",     sim_val("manual",     "win_pct") * 100, "#78909C")
comparison_html += _model_pill("Regression", sim_val("regression", "win_pct") * 100, "#78909C")
comparison_html += _model_pill("Ensemble",   sim_val("ensemble",   "win_pct") * 100, "#4CAF50")
comparison_html += _model_pill("DataGolf",   dg_win, "#1565C0" if dg_win else "#555")
comparison_html += '</div>'
st.markdown(comparison_html, unsafe_allow_html=True)

if dg_win is None:
    st.caption("DataGolf 2026 pre-tournament predictions not yet ingested — run ingestion/ingest_current_dg_predictions.py to populate.")
