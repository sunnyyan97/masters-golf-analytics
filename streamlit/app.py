import streamlit as st

st.set_page_config(layout="wide", page_title="2026 Masters Model")

if "selected_model" not in st.session_state:
    st.session_state["selected_model"] = "ensemble"

rankings_page = st.Page("pages/rankings.py", title="Pre-Tournament Rankings", icon="🏌️", default=True)
player_page   = st.Page("pages/player.py",   title="Player Deep Dive",         icon="📊")

pg = st.navigation([rankings_page, player_page])
pg.run()
