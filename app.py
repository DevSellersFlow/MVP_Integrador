"""
app.py
======
Interface Streamlit do SellersFlow.

Funcionalidades:
  - Upload de planilha Amazon + template marketplace
  - Seleção de marketplace
  - Preview lado a lado (Amazon vs Output mapeado)
  - Visualização do mapeamento aplicado (estratégia + confiança)
  - Botão "Sugerir mapping com IA"
  - Checkbox "Aplicar enriquecimento com IA"
  - Barra de progresso
  - Alertas de validação
  - Sistema de aprendizado (confirmar/corrigir mapeamentos)
  - Download do arquivo gerado

Execute:
  python -m streamlit run app.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Garante que os módulos do projeto sejam encontrados
sys.path.insert(0, str(Path(__file__).parent))

import re
import zipfile

import uuid
import shutil
import pandas as pd
import streamlit as st

from pipeline import SellersFlowPipeline, PipelineResult
from core.mapper import MARKETPLACE_MAPPINGS, MappingResult, FieldMappingDecision

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ─── Constantes ───────────────────────────────────────────────────────────────

MARKETPLACES = ["Selecione o Marketplace", "Amazon", "Magalu", "Mercado Livre", "Shopee", "Temu", "Vendor", "Walmart"]
SOURCE_MARKETPLACES = ["Selecione o Marketplace", "Amazon", "Magalu", "Mercado Livre", "Shopee", "Temu", "Vendor", "Walmart"]

STRATEGY_LABELS = {
    "fixed+synonym": ("🟢 Fixo + Sinônimo", "green"),
    "learned": ("🔵 Aprendido", "blue"),
    "similarity": ("🟡 Similaridade", "orange"),
    "ai": ("🤖 IA", "violet"),
    "unmapped": ("🔴 Não mapeado", "red"),
}

CONFIDENCE_COLORS = {
    (0.9, 1.01): "🟢",
    (0.7, 0.9): "🟡",
    (0.0, 0.7): "🔴",
}


def confidence_icon(score: float) -> str:
    for (lo, hi), icon in CONFIDENCE_COLORS.items():
        if lo <= score < hi:
            return icon
    return "⚪"

# ─── Validação de template ────────────────────────────────────────────────────

_TEMPLATE_SIGNATURES: dict = {
    "Mercado Livre": lambda sheets: (
        len(sheets) >= 3
        and any(s.strip().lower() == "ajuda" for s in sheets)
        and any("extra" in s.strip().lower() for s in sheets)
    ),
    "Amazon":  lambda sheets: any(s.strip().lower() in ("template", "modelo") for s in sheets),
    "Magalu":  lambda sheets: any(s.strip().lower() == "produto" for s in sheets),
    "Shopee":  lambda sheets: any(s.strip().lower() == "modelo" for s in sheets),
    "Temu":    lambda sheets: any(s.strip().lower() == "template" for s in sheets),
    "Vendor":  lambda sheets: any(s.strip().lower().startswith("modelo-") for s in sheets),
    "Walmart": lambda sheets: any("product content and site exp" in s.strip().lower() for s in sheets),
}


def _get_sheet_names(file_bytes: bytes) -> list:
    try:
        import io as _io
        with zipfile.ZipFile(_io.BytesIO(file_bytes)) as zf:
            wb_xml = zf.read("xl/workbook.xml").decode("utf-8", errors="replace")
        return re.findall(r'<sheet[^>]*name="([^"]+)"', wb_xml)
    except Exception:
        return []


def validate_template_marketplace(file_bytes: bytes, selected: str):
    if selected == "Selecione o Marketplace":
        return True, ""
    sheets = _get_sheet_names(file_bytes)
    if not sheets:
        return True, ""
    sig = _TEMPLATE_SIGNATURES.get(selected)
    if sig is None or sig(sheets):
        return True, ""
    detected = next((n for n, fn in _TEMPLATE_SIGNATURES.items() if fn(sheets)), None)
    if detected:
        msg = (
            f"O template enviado parece ser do **{detected}**, "
            f"mas o marketplace selecionado é **{selected}**. "
            "Por favor, envie o template correto."
        )
    else:
        sheet_list = ", ".join(f"`{s}`" for s in sheets[:5])
        msg = (
            f"O template não foi reconhecido como um template de **{selected}**. "
            f"Abas encontradas: {sheet_list}. Verifique se está enviando o arquivo correto."
        )
    return False, msg


# ─── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Integrador SellersFlow",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS customizado ──────────────────────────────────────────────────────────

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

    /* ── Tokens SellersFlow ── */
    :root {
        --sf-blue:      #008CFF;
        --sf-blue-dark: #0062B2;
        --sf-blue-deep: #0D192E;
        --sf-blue-navy: #2A3954;
        --sf-blue-mid:  #424F67;
        --sf-blue-lt:   #5FC7F4;
        --sf-blue-pale: #A5DFF8;
        --bg-base:      #060E1A;
        --bg-surface:   #0B1626;
        --bg-raised:    #101E30;
        --bg-elevated:  #152438;
        --border-sub:   rgba(42,57,84,0.6);
        --border-def:   rgba(66,79,103,0.5);
        --border-acc:   rgba(0,140,255,0.35);
        --text-pri:     #E8F4FF;
        --text-sec:     #8BA5C4;
        --text-muted:   #4A6480;
        --text-acc:     #5FC7F4;
    }

    /* ── 1. Remover barra branca do topo ── */
    header[data-testid="stHeader"] {
        background: transparent !important;
        border-bottom: none !important;
        height: 0 !important;
        min-height: 0 !important;
        padding: 0 !important;
    }
    #MainMenu, footer, header { visibility: hidden; }
    .stDeployButton { display: none; }

    /* ── Base ── */
    html, body, .stApp {
        background: var(--bg-base) !important;
        font-family: 'DM Sans', sans-serif;
        color: var(--text-pri);
    }
    .stApp::before {
        content: '';
        position: fixed;
        inset: 0;
        background-image:
            linear-gradient(rgba(0,140,255,0.025) 1px, transparent 1px),
            linear-gradient(90deg, rgba(0,140,255,0.025) 1px, transparent 1px);
        background-size: 44px 44px;
        pointer-events: none;
        z-index: 0;
    }
    .main .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 3rem;
        max-width: 1400px;
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background: var(--bg-surface) !important;
        border-right: 1px solid var(--border-sub) !important;
    }
    [data-testid="stSidebar"] .block-container { padding-top: 1.2rem; }

    /* ── Logo ── */
    .sf-logo-wrap {
        display: flex; align-items: center; gap: 10px;
        padding: 0 0 1rem; border-bottom: 1px solid var(--border-sub);
        margin-bottom: 1.2rem;
    }
    .sf-logo-img {
        width: 36px; height: 36px; object-fit: contain; border-radius: 8px;
        filter: drop-shadow(0 0 8px rgba(0,140,255,0.45));
    }
    .sf-logo-text {
        font-size: 1.05rem; font-weight: 700; letter-spacing: -0.02em;
        background: linear-gradient(135deg, #008CFF, #5FC7F4);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        background-clip: text; line-height: 1.1;
    }
    .sf-logo-sub {
        font-size: 0.62rem; color: var(--text-muted);
        letter-spacing: 0.07em; text-transform: uppercase; font-weight: 500;
    }

    /* ── Sidebar labels ── */
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] .stMarkdown p {
        color: var(--text-sec) !important;
        font-size: 0.83rem !important; font-weight: 500 !important;
    }

    /* ── 2. Checkboxes IA — texto mais visível ── */
    [data-testid="stCheckbox"] label,
    [data-testid="stCheckbox"] span,
    [data-testid="stCheckbox"] p {
        color: #C8DCF0 !important;
        font-size: 0.87rem !important;
        font-weight: 500 !important;
        opacity: 1 !important;
    }
    [data-testid="stCheckbox"] svg {
        color: var(--sf-blue) !important;
    }

    /* ── Selectbox ── */
    [data-testid="stSelectbox"] > div > div {
        background: var(--bg-raised) !important;
        border: 1px solid var(--border-def) !important;
        border-radius: 10px !important;
        color: var(--text-pri) !important;
        font-size: 0.88rem !important;
    }

    /* ── File uploader compacto ── */
    [data-testid="stFileUploader"] {
        background: transparent !important;
        border: none !important;
        padding: 0 !important;
    }
    [data-testid="stFileUploaderDropzoneInstructions"],
    [data-testid="stFileUploader"] section > div > div > p,
    [data-testid="stFileUploader"] section > div > small {
        display: none !important;
    }
    [data-testid="stFileUploader"] section {
        padding: 0 !important;
        min-height: 0 !important;
        border: none !important;
        background: transparent !important;
    }
    [data-testid="stFileUploader"] button {
        background: var(--bg-elevated) !important;
        border: 1px solid var(--border-def) !important;
        border-radius: 8px !important;
        color: var(--text-sec) !important;
        font-size: 0.8rem !important;
        padding: 4px 12px !important;
        height: 30px !important;
        min-height: 0 !important;
        transition: border-color 0.2s, color 0.2s !important;
    }
    [data-testid="stFileUploader"] button:hover {
        border-color: var(--sf-blue) !important;
        color: var(--text-acc) !important;
    }

    /* ── Botão primário ── */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #0062B2 0%, #008CFF 60%, #32A3FF 100%) !important;
        border: none !important; border-radius: 10px !important;
        color: white !important; font-family: 'DM Sans', sans-serif !important;
        font-weight: 600 !important; font-size: 0.9rem !important;
        box-shadow: 0 2px 14px rgba(0,140,255,0.35) !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 4px 22px rgba(0,140,255,0.55) !important;
        transform: translateY(-1px) !important;
    }
    .stButton > button[kind="primary"]:disabled {
        background: var(--bg-elevated) !important;
        box-shadow: none !important; color: var(--text-muted) !important;
    }
    .stButton > button:not([kind="primary"]) {
        background: var(--bg-raised) !important;
        border: 1px solid var(--border-def) !important;
        border-radius: 6px !important; color: var(--text-sec) !important;
        font-size: 0.82rem !important; transition: all 0.15s !important;
    }
    .stButton > button:not([kind="primary"]):hover {
        border-color: var(--sf-blue) !important; color: var(--text-acc) !important;
    }

    /* ── Download button ── */
    [data-testid="stDownloadButton"] > button {
        background: linear-gradient(135deg, #0062B2 0%, #008CFF 60%, #32A3FF 100%) !important;
        border: none !important; border-radius: 10px !important;
        color: white !important; font-weight: 600 !important;
        box-shadow: 0 2px 14px rgba(0,140,255,0.35) !important;
        transition: all 0.2s ease !important;
    }
    [data-testid="stDownloadButton"] > button:hover {
        box-shadow: 0 4px 22px rgba(0,140,255,0.55) !important;
        transform: translateY(-1px) !important;
    }

    /* ── Progress bar ── */
    [data-testid="stProgress"] > div > div {
        background: var(--bg-elevated) !important; border-radius: 999px !important;
    }
    [data-testid="stProgress"] > div > div > div {
        background: linear-gradient(90deg, #0062B2, #008CFF, #5FC7F4) !important;
        border-radius: 999px !important;
    }

    /* ── Metrics ── */
    [data-testid="stMetric"] {
        background: var(--bg-surface) !important;
        border: 1px solid var(--border-sub) !important;
        border-radius: 14px !important; padding: 1rem 1.2rem !important;
        transition: border-color 0.2s, box-shadow 0.2s;
    }
    [data-testid="stMetric"]:hover {
        border-color: var(--border-acc) !important;
        box-shadow: 0 0 20px rgba(0,140,255,0.12) !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.68rem !important; font-weight: 600 !important;
        text-transform: uppercase !important; letter-spacing: 0.1em !important;
        color: var(--text-muted) !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.8rem !important; font-weight: 700 !important;
        color: var(--text-pri) !important; font-family: 'DM Mono', monospace !important;
    }

    /* ── Tabs ── */
    [data-testid="stTabs"] [role="tablist"] {
        border-bottom: 1px solid var(--border-sub) !important;
    }
    [data-testid="stTabs"] button[role="tab"] {
        background: transparent !important; border: none !important;
        border-bottom: 2px solid transparent !important;
        color: var(--text-muted) !important; font-family: 'DM Sans', sans-serif !important;
        font-size: 0.85rem !important; font-weight: 500 !important;
        padding: 0.65rem 1.2rem !important; transition: all 0.2s !important;
    }
    [data-testid="stTabs"] button[role="tab"]:hover {
        color: var(--text-sec) !important; background: var(--bg-elevated) !important;
    }
    [data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
        color: var(--text-acc) !important;
        border-bottom-color: var(--sf-blue) !important; font-weight: 600 !important;
    }

    /* ── Expander ── */
    [data-testid="stExpander"] {
        background: var(--bg-surface) !important;
        border: 1px solid var(--border-sub) !important; border-radius: 10px !important;
    }
    [data-testid="stExpander"] summary {
        font-size: 0.87rem !important; font-weight: 500 !important;
        color: var(--text-sec) !important; background: var(--bg-raised) !important;
        padding: 0.65rem 1rem !important;
    }

    /* ── Dataframe ── */
    [data-testid="stDataFrame"] {
        border: 1px solid var(--border-sub) !important; border-radius: 10px !important;
    }

    /* ── Multiselect tags ── */
    [data-baseweb="tag"] {
        background: rgba(0,140,255,0.14) !important;
        border: 1px solid rgba(0,140,255,0.28) !important;
        border-radius: 6px !important; color: var(--text-acc) !important;
        font-size: 0.78rem !important;
    }

    /* ── Code ── */
    code, pre { font-family: 'DM Mono', monospace !important; font-size: 0.8rem !important; }

    /* ── Caption ── */
    [data-testid="stCaptionContainer"] {
        color: var(--text-muted) !important; font-size: 0.78rem !important;
    }

    /* ── Divider ── */
    hr { border-color: var(--border-sub) !important; margin: 0.8rem 0 !important; }

    /* ── Scrollbar ── */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: var(--bg-base); }
    ::-webkit-scrollbar-thumb { background: var(--bg-elevated); border-radius: 3px; }

    /* ── Page title ── */
    .sf-page-title {
        font-size: 1.55rem; font-weight: 700; letter-spacing: -0.03em;
        background: linear-gradient(135deg, #E8F4FF 20%, #5FC7F4 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .sf-page-sub {
        font-size: 0.82rem; color: var(--text-muted);
        margin-top: 2px; letter-spacing: 0.01em;
    }

    /* ── Map table ── */
    .map-row {
        display: flex; align-items: center; padding: 0.45rem 0.6rem;
        border-bottom: 1px solid var(--border-sub); font-size: 0.82rem;
        transition: background 0.12s;
    }
    .map-row:hover { background: var(--bg-elevated); }
    .map-col { flex: 1; padding: 0 0.4rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .map-arrow { color: var(--text-muted); padding: 0 0.5rem; }

    /* ── Badges ── */
    .badge {
        display: inline-block; padding: 2px 9px; border-radius: 20px;
        font-size: 0.7rem; font-weight: 600; letter-spacing: 0.03em;
    }
    .badge-green  { background: rgba(0,180,80,0.12);  color: #34D37A; border: 1px solid rgba(0,180,80,0.2); }
    .badge-blue   { background: rgba(0,140,255,0.12); color: #5FC7F4; border: 1px solid rgba(0,140,255,0.25); }
    .badge-orange { background: rgba(255,150,0,0.12); color: #FBAD3C; border: 1px solid rgba(255,150,0,0.2); }
    .badge-violet { background: rgba(150,80,255,0.12);color: #C084FC; border: 1px solid rgba(150,80,255,0.2); }
    .badge-red    { background: rgba(255,60,60,0.12); color: #F87171; border: 1px solid rgba(255,60,60,0.2); }

    /* ── Empty state ── */
    .sf-empty {
        background: var(--bg-surface); border: 1px solid var(--border-sub);
        border-radius: 20px; padding: 4rem 2rem; text-align: center;
    }
    .sf-empty-icon { font-size: 2.8rem; margin-bottom: 1rem; }
    .sf-empty-title {
        font-size: 1.05rem; font-weight: 600; color: var(--text-sec);
        margin-bottom: 0.4rem; letter-spacing: -0.01em;
    }
    .sf-empty-desc { font-size: 0.82rem; color: var(--text-muted); line-height: 1.7; }
    .sf-empty-desc strong { color: var(--text-acc); }
</style>
""", unsafe_allow_html=True)


# ─── Estado da sessão ─────────────────────────────────────────────────────────

def _init_state():
    defaults = {
        "pipeline_result": None,
        "last_marketplace": None,
        "last_source_mp": "Amazon",  # marketplace de origem da última execução
        "corrections": {},      # dest_col → source_col (aprendizado)
        "session_output_dir": None,  # pasta exclusiva desta sessão
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


_init_state()

# ── Pasta de saída exclusiva desta sessão ────────────────────────────────
if st.session_state.session_output_dir is None:
    import tempfile as _tf
    _sid = uuid.uuid4().hex[:8]
    _sess_dir = _tf.gettempdir() + f'/sellersflow_{_sid}'
    import os as _os; _os.makedirs(_sess_dir, exist_ok=True)
    st.session_state.session_output_dir = _sess_dir

_SESSION_DIR: str = st.session_state.session_output_dir

import atexit as _atexit
_atexit.register(lambda: __import__('shutil').rmtree(_SESSION_DIR, ignore_errors=True))


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div class="sf-logo-wrap">
        <img class="sf-logo-img" src="data:image/png;base64,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" />
        <div>
            <div class="sf-logo-text">SellersFlow</div>
            <div class="sf-logo-sub">Catálogos Multi-marketplace</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("**🔄 Conversão**")
    col_src, col_arr, col_dst = st.columns([5, 1, 5])
    with col_src:
        source_marketplace = st.selectbox(
            "Origem", SOURCE_MARKETPLACES, key="source_mp",
            help="Marketplace de onde vêm os dados"
        )
    with col_arr:
        st.markdown("<div style='text-align:center;padding-top:28px;font-size:1.2rem'>→</div>", unsafe_allow_html=True)
    with col_dst:
        dest_options = [m for m in MARKETPLACES if m != "Selecione o Marketplace" and m != source_marketplace]
        marketplace = st.selectbox(
            "Destino", ["Selecione o Marketplace"] + dest_options, key="dest_mp",
            help="Marketplace para onde os dados serão convertidos"
        )

    st.markdown("**📤 Arquivos**")
    source_label = f"Planilha {source_marketplace}"
    amazon_file = st.file_uploader(
        source_label, type=["xlsx", "xlsm", "xls"], key="amazon_upload"
    )
    dest_file = st.file_uploader(
        f"Template {marketplace}" if marketplace != "Selecione o Marketplace" else "Template destino",
        type=["xlsx", "xlsm", "xls"], key="dest_upload"
    )

    # ── Validação de compatibilidade de template ──────────────────────────
    template_ok = True
    if dest_file and marketplace != "Selecione o Marketplace":
        ok, msg = validate_template_marketplace(dest_file.read(), marketplace)
        dest_file.seek(0)
        if not ok:
            st.error(msg, icon="⚠️")
            template_ok = False

    if amazon_file and source_marketplace != "Amazon":
        ok_src, msg_src = validate_template_marketplace(amazon_file.read(), source_marketplace)
        amazon_file.seek(0)
        if not ok_src:
            st.warning(f"Arquivo de origem: {msg_src}", icon="⚠️")

    use_ai_mapping = False
    use_ai_enrich = False

    st.divider()
    run_btn = st.button(
        "▶ Processar",
        type="primary",
        disabled=not (amazon_file and dest_file and template_ok and marketplace != "Selecione o Marketplace"),
        use_container_width=True,
    )

    if st.session_state.pipeline_result:
        res: PipelineResult = st.session_state.pipeline_result
        st.markdown("**📊 Última execução**")
        st.caption(f"⏱ {res.elapsed_seconds}s")
        if res.mapping_result:
            cov = res.mapping_result.coverage
            st.progress(cov, text=f"Cobertura: {cov:.0%}")


# ─── Pipeline trigger ─────────────────────────────────────────────────────────

if run_btn and amazon_file and dest_file:
    with st.spinner("Processando..."):
        pipeline = SellersFlowPipeline(output_dir=_SESSION_DIR)
        _src_label_run = source_marketplace
        progress = st.progress(0, text=f"Lendo planilha {_src_label_run}...")

        # Simula progresso por etapas
        import time

        progress.progress(20, text=f"Lendo planilha {_src_label_run}...")
        time.sleep(0.1)

        # Limpar arquivos gerados anteriormente nesta sessão
        import glob as _glob
        for _old in _glob.glob(_SESSION_DIR + "/*.xls*"):
            try:
                import os as _os2; _os2.unlink(_old)
            except OSError:
                pass

        progress.progress(45, text="Construindo mapeamento...")
        result = pipeline.run(
            amazon_file=amazon_file,
            template_file=dest_file,
            marketplace=marketplace,
            use_ai=use_ai_mapping,
            enrich_ai=use_ai_enrich,
            source_marketplace=source_marketplace,
        )

        progress.progress(80, text="Preenchendo template...")
        time.sleep(0.1)
        progress.progress(100, text="Concluído!")
        time.sleep(0.3)
        progress.empty()

        st.session_state.pipeline_result = result
        st.session_state.last_marketplace = marketplace
        st.session_state.last_source_mp = source_marketplace


# ─── Main content ─────────────────────────────────────────────────────────────

st.markdown("""
<div style="padding-bottom:1.2rem; border-bottom:1px solid rgba(42,57,84,0.6); margin-bottom:1.5rem;">
    <div class="sf-page-title">Integrador SellersFlow</div>
    <div class="sf-page-sub">Motor inteligente de transformação de catálogos multi-marketplace</div>
</div>
""", unsafe_allow_html=True)

result: Optional[PipelineResult] = st.session_state.pipeline_result

if result is None:
    # ── Estado vazio ──────────────────────────────────────────────────────
    st.markdown("""
    <div class="sf-empty">
        <div class="sf-empty-icon">📦</div>
        <div class="sf-empty-title">Nenhuma planilha processada</div>
        <div class="sf-empty-desc">
            Faça upload das planilhas na barra lateral<br>
            e clique em <strong>Processar</strong>
        </div>
    </div>
    """, unsafe_allow_html=True)

else:
    # ── Erros críticos ────────────────────────────────────────────────────
    if result.errors:
        for err in result.errors:
            st.error(f"❌ {err}")

    # ── Métricas ──────────────────────────────────────────────────────────
    if result.read_result and result.mapping_result:
        mr = result.mapping_result
        rr = result.read_result

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            _src_name = st.session_state.get("last_source_mp", "Origem")
            st.metric(f"📋 Linhas {_src_name}", rr.valid_rows)
        with col2:
            st.metric("🗺 Cobertura", f"{mr.coverage:.0%}")
        with col3:
            st.metric("🎯 Confiança Média", f"{mr.avg_confidence:.0%}")
        with col4:
            st.metric("⏱ Tempo", f"{result.elapsed_seconds}s")

    # ── Avisos e validação ────────────────────────────────────────────────
    if result.warnings:
        with st.expander(f"⚠️ {len(result.warnings)} avisos"):
            for w in result.warnings:
                st.warning(w)

    if result.fill_result and result.fill_result.validation_issues:
        issues = result.fill_result.validation_issues
        errors = [i for i in issues if i.severity == "error"]
        warnings = [i for i in issues if i.severity == "warning"]

        if errors:
            with st.expander(f"🚨 {len(errors)} erros de validação", expanded=True):
                for issue in errors:
                    st.error(f"**{issue.column}**: {issue.message}")
        if warnings:
            with st.expander(f"⚠️ {len(warnings)} alertas de validação"):
                for issue in warnings:
                    st.warning(f"**{issue.column}**: {issue.message}")

    # ── Tabs principais ───────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs([
        "🗺 Mapeamento",
        "📊 Preview de Dados",
        "📥 Download",
    ])

    # ── Tab 1: Mapeamento ─────────────────────────────────────────────────
    with tab1:
        if result.mapping_result:
            mr = result.mapping_result
            decisions = mr.decisions

            # Filtros
            col_f1, col_f2, _ = st.columns([1, 1, 2])
            with col_f1:
                filter_strategy = st.multiselect(
                    "Estratégia",
                    ["fixed+synonym", "learned", "similarity", "ai", "unmapped"],
                    default=["fixed+synonym", "learned", "similarity", "ai", "unmapped"],
                    format_func=lambda x: STRATEGY_LABELS.get(x, (x, ""))[0],
                )
            with col_f2:
                min_confidence = st.slider("Confiança mínima", 0.0, 1.0, 0.0, 0.05)

            filtered = [
                d for d in decisions
                if d.strategy in filter_strategy
                and d.confidence >= min_confidence
            ]

            # Header da tabela
            st.markdown("""
            <div class="map-row" style="border-bottom:1px solid #333; font-weight:600; color:#888; font-size:0.75rem; text-transform:uppercase;">
                <div class="map-col">Coluna Destino</div>
                <div class="map-arrow">→</div>
                <div class="map-col">Coluna Origem</div>
                <div class="map-col">Estratégia</div>
                <div class="map-col">Confiança</div>
                <div class="map-col">Notas</div>
            </div>
            """, unsafe_allow_html=True)

            for d in filtered:
                label, color = STRATEGY_LABELS.get(d.strategy, (d.strategy, ""))
                conf_icon = confidence_icon(d.confidence)
                source_display = d.source_col or "—"
                badge_class = f"badge badge-{color}" if color else "badge"

                st.markdown(f"""
                <div class="map-row">
                    <div class="map-col" style="color:#c0c0e0"><strong>{d.dest_col}</strong></div>
                    <div class="map-arrow">→</div>
                    <div class="map-col" style="color:#8080a0">{source_display}</div>
                    <div class="map-col"><span class="{badge_class}">{label}</span></div>
                    <div class="map-col">{conf_icon} {d.confidence:.0%}</div>
                    <div class="map-col" style="color:#555;font-size:0.75rem">{d.notes}</div>
                </div>
                """, unsafe_allow_html=True)

            # ── Campos não mapeados ────────────────────────────────────────
            if mr.unmapped_dest:
                st.markdown("---")
                st.markdown(f"**🔴 {len(mr.unmapped_dest)} campos destino sem match:**")
                st.code(", ".join(mr.unmapped_dest))

            # ── Aprendizado manual ────────────────────────────────────────
            with st.expander("✏️ Corrigir mapeamento (aprendizado)"):
                st.caption("Selecione um campo destino e indique a coluna Amazon correta. O sistema aprenderá para execuções futuras.")
                dest_options = [d.dest_col for d in decisions]
                source_options = list(result.read_result.df.columns) if result.read_result else []

                col_a, col_b, col_c = st.columns([2, 2, 1])
                with col_a:
                    sel_dest = st.selectbox("Campo destino", dest_options, key="learn_dest")
                with col_b:
                    sel_source = st.selectbox("Coluna Amazon correta", source_options, key="learn_src")
                with col_c:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("💾 Salvar", key="learn_save"):
                        pipeline = SellersFlowPipeline()
                        pipeline.learn_mapping(
                            st.session_state.get("last_marketplace") or marketplace,
                            sel_dest,
                            sel_source,
                        )
                        st.success(f"Aprendido: **{sel_dest}** → **{sel_source}**")

    # ── Tab 2: Preview ────────────────────────────────────────────────────
    with tab2:
        if result.read_result and result.read_result.df is not None:
            df = result.read_result.df

            col_left, col_right = st.columns(2)

            with col_left:
                _src_label = st.session_state.get("last_source_mp", "Origem")
                st.markdown(f"### 📥 Dados {_src_label}")
                st.caption(f"{len(df)} linhas · {len(df.columns)} colunas · idioma: {result.read_result.language}")
                # Mostra apenas colunas mapeadas + primeiros N
                if result.mapping_result:
                    mapped_src_cols = [
                        d.source_col
                        for d in result.mapping_result.decisions
                        if d.source_col and d.source_col in df.columns
                    ]
                    preview_cols = mapped_src_cols[:15] or list(df.columns[:15])
                else:
                    preview_cols = list(df.columns[:15])

                st.dataframe(
                    df[preview_cols].head(20),
                    use_container_width=True,
                    height=350,
                )

            with col_right:
                st.markdown("### 📤 Output mapeado")
                if result.mapping_result and result.read_result:
                    mr = result.mapping_result
                    df_amazon = result.read_result.df
                    # Constrói preview do output
                    # Usa col_idx como chave para evitar sobrescrever colunas
                    # com nomes duplicados (ex: "Sku Id" aparece 2x no Temu)
                    output_preview_indexed: dict[int, tuple[str, list]] = {}
                    for d in mr.decisions:
                        if d.source_idx is not None:
                            vals = df_amazon.iloc[:, d.source_idx].tolist() if d.source_col else [""] * len(df_amazon)
                        else:
                            vals = [""] * len(df_amazon)
                        output_preview_indexed[id(d)] = (d.dest_col, vals)
                    # Monta df só com colunas mapeadas (source_col não-None)
                    mapped_decisions = [d for d in mr.decisions if d.source_col]
                    output_preview = {}
                    seen_dest: dict[str, int] = {}
                    for d in mapped_decisions:
                        label = d.dest_col
                        if label in seen_dest:
                            seen_dest[label] += 1
                            label = f"{d.dest_col} ({seen_dest[d.dest_col]})"
                        else:
                            seen_dest[label] = 1
                        output_preview[label] = df_amazon.iloc[:, d.source_idx].tolist()
                    df_out = pd.DataFrame(output_preview).head(20)
                    st.caption(f"{len(df_out)} linhas · {len(output_preview)} colunas mapeadas")
                    st.dataframe(df_out, use_container_width=True, height=350)
                else:
                    st.info("Sem dados de output disponíveis.")
        else:
            st.info("Nenhum dado Amazon carregado.")

    # ── Tab 3: Download ───────────────────────────────────────────────────
    with tab3:
        if result.fill_result and result.fill_result.output_path:
            output_path = result.fill_result.output_path
            try:
                with open(output_path, "rb") as f:
                    file_bytes = f.read()

                st.success(f"✅ Arquivo gerado com sucesso — {result.fill_result.rows_written} linhas preenchidas")

                fname = Path(output_path).name
                st.download_button(
                    label=f"📥 Baixar {fname}",
                    data=file_bytes,
                    file_name=fname,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    use_container_width=True,
                )

                # Resumo de validação
                if result.fill_result.validation_issues:
                    st.markdown("---")
                    st.markdown("**Relatório de validação:**")
                    for issue in result.fill_result.validation_issues:
                        icon = "🚨" if issue.severity == "error" else "⚠️"
                        st.markdown(f"{icon} `{issue.column}`: {issue.message}")
                else:
                    st.markdown("✅ Todos os campos obrigatórios estão preenchidos.")

            except FileNotFoundError:
                st.error("Arquivo de saída não encontrado. Tente processar novamente.")
        elif result.has_errors:
            st.error("Não foi possível gerar o arquivo. Verifique os erros acima.")
        else:
            st.info("Processe as planilhas para gerar o arquivo de download.")
