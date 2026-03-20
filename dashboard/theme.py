"""Theme configurations for Streamlit dashboard."""

def get_theme(is_dark: bool):
    if is_dark:
        bg_app = "linear-gradient(135deg, #0a0f1e 0%, #0f172a 50%, #0a0f1e 100%)"
        bg_sidebar = "linear-gradient(180deg, #0d1424 0%, #111827 100%)"
        text_main = "#e2e8f0"
        text_hdr = "#f1f5f9"
        text_muted = "#94a3b8"
        border = "rgba(99,102,241,0.25)"
        metric_bg = "linear-gradient(135deg, rgba(99,102,241,0.12) 0%, rgba(139,92,246,0.08) 100%)"
        tab_bg = "rgba(15,23,42,0.8)"
        code_bg = "rgba(15,23,42,0.9)"
        txt_bg = "rgba(30,41,59,0.8)"
        hero_bg = "linear-gradient(135deg, rgba(99,102,241,0.15) 0%, rgba(139,92,246,0.1) 50%, rgba(6,182,212,0.08) 100%)"
    else:
        bg_app = "linear-gradient(135deg, #f8fafc 0%, #f1f5f9 50%, #f8fafc 100%)"
        bg_sidebar = "linear-gradient(180deg, #ffffff 0%, #f8fafc 100%)"
        text_main = "#334155"
        text_hdr = "#0f172a"
        text_muted = "#64748b"
        border = "rgba(99,102,241,0.15)"
        metric_bg = "linear-gradient(135deg, #ffffff 0%, #f8fafc 100%)"
        tab_bg = "rgba(255,255,255,0.8)"
        code_bg = "rgba(241,245,249,0.9)"
        txt_bg = "#ffffff"
        hero_bg = "linear-gradient(135deg, rgba(99,102,241,0.05) 0%, rgba(139,92,246,0.05) 50%, rgba(6,182,212,0.02) 100%)"

    css = f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
        html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; }}
        
        .stApp {{ background: {bg_app} !important; }}
        [data-testid="stSidebar"] {{ background: {bg_sidebar} !important; border-right: 1px solid {border}; }}
        [data-testid="stSidebar"] * {{ color: {text_main} !important; }}
        
        [data-testid="stMetric"] {{ background: {metric_bg}; border: 1px solid {border}; border-radius: 16px; padding: 20px 16px; }}
        [data-testid="stMetricLabel"] {{ color: {text_muted} !important; font-size: 0.78rem !important; text-transform: uppercase; font-weight: 500 !important; }}
        [data-testid="stMetricValue"] {{ color: {text_hdr} !important; font-size: 1.7rem !important; font-weight: 700 !important; }}
        
        h1, h2, h3 {{ color: {text_hdr} !important; font-weight: 700 !important; }}
        h1 {{ background: none !important; -webkit-text-fill-color: {text_hdr} !important; }}
        hr {{ border-color: {border} !important; }}
        [data-testid="stDataFrame"] {{ border: 1px solid {border}; border-radius: 12px; }}
        
        [data-testid="stTextInput"] input, [data-testid="stSelectbox"] > div > div {{
            background: {txt_bg} !important; border: 1px solid {border} !important; border-radius: 10px !important; color: {text_main} !important;
        }}
        [data-testid="stButton"] button {{ background: linear-gradient(135deg, #6366f1, #8b5cf6) !important; border-radius: 8px !important; color: white !important; font-weight: 600 !important; border: none !important; }}
        [data-testid="stRadio"] label, [data-testid="stRadio"] div, p, span {{ color: {text_main} !important; }}
        
        [data-testid="stTabs"] [role="tablist"] {{ background: {tab_bg}; border-radius: 12px; padding: 4px; border: 1px solid {border}; }}
        [data-testid="stTabs"] [role="tab"] {{ color: {text_muted} !important; font-weight: 600 !important; }}
        [data-testid="stTabs"] [role="tab"][aria-selected="true"] {{ background: linear-gradient(135deg, #6366f1, #8b5cf6) !important; color: #ffffff !important; }}
        
        .section-card, .rec-card {{ background: {txt_bg}; border: 1px solid {border}; border-radius: 16px; padding: 24px; }}
        .rec-card {{ padding: 12px 16px; display: flex; align-items: center; gap: 10px; margin-bottom: 8px; border-radius: 12px; }}
        
        .hero-banner {{ background: {hero_bg}; border: 1px solid {border}; border-radius: 20px; padding: 28px 32px; margin-bottom: 28px; }}
        .hero-title {{ font-size: 1.6rem; font-weight: 800; margin: 0; color: {text_hdr} !important; }}
        .hero-subtitle {{ color: {text_muted} !important; font-size: 0.9rem; margin-top: 4px; margin-bottom: 0; }}
        .hero-pill {{ display: inline-block; background: rgba(99,102,241,0.15); border: 1px solid rgba(99,102,241,0.3); border-radius: 999px; padding: 3px 12px; font-size: 0.75rem; font-weight: 600; color: #6366f1; margin-right: 6px; margin-top: 12px; }}
        
        [data-testid="stCode"] {{ background: {code_bg} !important; border: 1px solid {border} !important; border-radius: 10px !important; }}
    </style>
    """

    chart_layout = dict(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color=text_muted, size=12),
        title_font=dict(color=text_hdr, size=14, family="Inter"),
        legend=dict(
            bgcolor=tab_bg,
            bordercolor=border,
            borderwidth=1,
            font=dict(color=text_muted),
        ),
        xaxis=dict(
            gridcolor=border, linecolor=border, tickcolor=border,
            tickfont=dict(color=text_muted), title_font=dict(color=text_muted),
        ),
        yaxis=dict(
            gridcolor=border, linecolor=border, tickcolor=border,
            tickfont=dict(color=text_muted), title_font=dict(color=text_muted),
        ),
        margin=dict(t=40, b=40, l=20, r=20),
    )

    chart_layout_rot = {**chart_layout, "xaxis": {**chart_layout["xaxis"], "tickangle": 45}}

    return css, chart_layout, chart_layout_rot, text_hdr
