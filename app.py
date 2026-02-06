# app.py
# -*- coding: utf-8 -*-

import io
import re
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st


# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="FNDE | Dashboard Executivo",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR_DEFAULT = Path("data")


# =========================================================
# FORMATTERS
# =========================================================
def _to_float(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def br_int(v) -> str:
    """Inteiro pt-BR: 4.500.000"""
    n = int(round(_to_float(v), 0))
    return f"{n:,}".replace(",", ".")


def brl_int(v) -> str:
    """Moeda pt-BR sem centavos: R$ 4.500.000"""
    return f"R$ {br_int(v)}"


def df_to_excel_bytes(sheets: List[Tuple[str, pd.DataFrame]]) -> bytes:
    """Gera um XLSX em memória com várias abas."""
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, df in sheets:
            safe = re.sub(r"[\[\]\:\*\?\/\\]", "_", name)[:31]  # limite do Excel
            df.to_excel(writer, sheet_name=safe, index=False)
    out.seek(0)
    return out.read()


# =========================================================
# MUNICIPIO NORMALIZATION
# (teu fnde.py salva MAUES e BOA_VISTA_DO_RAMOS)
# =========================================================
MUN_KEYS_ORDER = ["MAUES", "BOA_VISTA_DO_RAMOS"]
MUN_LABEL = {
    "MAUES": "Maués",
    "BOA_VISTA_DO_RAMOS": "Boa Vista do Ramos",
}


def normalize_municipio_key(raw: str) -> str:
    s = (raw or "").strip().upper()
    s = s.replace("Á", "A").replace("Ã", "A").replace("Â", "A")
    s = s.replace("É", "E").replace("Ê", "E")
    s = s.replace("Í", "I")
    s = s.replace("Ó", "O").replace("Ô", "O").replace("Õ", "O")
    s = s.replace("Ú", "U")
    s = s.replace("-", "_").replace(" ", "_")

    if "BOA" in s and "RAM" in s:
        return "BOA_VISTA_DO_RAMOS"
    if "MAU" in s:
        return "MAUES"
    # fallback: devolve o que vier
    return s


def enforce_schema(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Garante que df sempre tenha as colunas necessárias (MunicipioKey, MunicipioLabel, Valor_num, Ano, etc.)
    mesmo que o CSV venha com colunas diferentes ou faltando.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()

    # Normaliza possíveis nomes de Município
    if "Municipio" not in out.columns:
        for cand in ["Município", "MUNICIPIO", "MUNICÍPIO"]:
            if cand in out.columns:
                out["Municipio"] = out[cand]
                break
    if "Municipio" not in out.columns:
        out["Municipio"] = ""

    # Ano
    if "Ano" not in out.columns:
        out["Ano"] = year
    out["Ano"] = pd.to_numeric(out["Ano"], errors="coerce").fillna(year).astype(int)

    # Valor_num
    if "Valor_num" in out.columns:
        out["Valor_num"] = pd.to_numeric(out["Valor_num"], errors="coerce").fillna(0.0)
    else:
        if "Valor_str" in out.columns:
            s = (
                out["Valor_str"].astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            out["Valor_num"] = pd.to_numeric(s, errors="coerce").fillna(0.0)
        else:
            out["Valor_num"] = 0.0

    # MunicipioKey / Label
    out["Municipio_raw"] = out["Municipio"].astype(str)
    out["MunicipioKey"] = out["Municipio_raw"].apply(normalize_municipio_key)
    out["MunicipioLabel"] = out["MunicipioKey"].map(MUN_LABEL).fillna(out["Municipio_raw"])

    # Campos opcionais usados no dashboard
    needed_cols = [
        "ProgramaGrupo", "Programa", "RazaoSocial", "CNPJ", "CNPJ_formatado",
        "OB", "Parcela", "Mes", "DataPgto"
    ]
    for c in needed_cols:
        if c not in out.columns:
            out[c] = ""

    # Mes / DataPgto / Mes_dt
    if "Mes" in out.columns:
        out["Mes"] = out["Mes"].astype(str).str.slice(0, 7)

    if "DataPgto" in out.columns:
        out["DataPgto"] = pd.to_datetime(out["DataPgto"], errors="coerce")
        need = out["Mes"].isna() | (out["Mes"].astype(str).str.strip() == "") | (out["Mes"].astype(str) == "NaT")
        if need.any():
            out.loc[need, "Mes"] = out.loc[need, "DataPgto"].dt.strftime("%Y-%m")
    else:
        out["DataPgto"] = pd.NaT

    out["Mes_dt"] = pd.to_datetime(out["Mes"].astype(str) + "-01", errors="coerce")

    # limpeza leve
    out["ProgramaGrupo"] = out["ProgramaGrupo"].fillna("").astype(str)
    out["Programa"] = out["Programa"].fillna("").astype(str)
    out["RazaoSocial"] = out["RazaoSocial"].fillna("").astype(str)

    return out


# =========================================================
# FILE DISCOVERY
# =========================================================
def find_years_available(data_dir: Path) -> List[int]:
    if not data_dir.exists():
        return []
    years = set()
    for p in data_dir.glob("liberacoes_tidy_AM_*.csv"):
        # liberacoes_tidy_AM_2025_MAUES.csv
        # liberacoes_tidy_AM_2025_MAUES_BVR.csv
        parts = p.name.split("_")
        if len(parts) >= 4:
            try:
                years.add(int(parts[3]))
            except Exception:
                pass
    return sorted(list(years))


def preferred_consolidated_file(data_dir: Path, year: int) -> Optional[Path]:
    # preferir consolidado MAUES_BVR
    cand = data_dir / f"liberacoes_tidy_AM_{year}_MAUES_BVR.csv"
    if cand.exists():
        return cand
    # fallback: qualquer tidy do ano
    any_files = list(data_dir.glob(f"liberacoes_tidy_AM_{year}_*.csv"))
    return any_files[0] if any_files else None


# =========================================================
# LOADING
# =========================================================
@st.cache_data(show_spinner=False)
def load_tidy_year(data_dir: Path, year: int) -> pd.DataFrame:
    p = preferred_consolidated_file(data_dir, year)
    if p is None or not p.exists():
        return pd.DataFrame()

    df = pd.read_csv(p, dtype=str, encoding="utf-8-sig")
    if df.empty:
        return df

    # garante esquema sempre
    df = enforce_schema(df, year)
    return df


# =========================================================
# FILTER ENGINE
# =========================================================
def apply_filters(
    df: pd.DataFrame,
    year: int,
    mes_ref: str,
    mun_keys: List[str],
    grupos: List[str],
    programas: List[str],
    escolas: List[str],
    busca_escola: str,
) -> pd.DataFrame:
    if df.empty:
        return df

    out = df[df["Ano"] == year].copy()

    if mes_ref and mes_ref != "Todos":
        out = out[out["Mes"] == mes_ref]

    if mun_keys:
        out = out[out["MunicipioKey"].isin(mun_keys)]

    if grupos:
        out = out[out["ProgramaGrupo"].isin(grupos)]

    if programas:
        out = out[out["Programa"].isin(programas)]

    if escolas:
        out = out[out["RazaoSocial"].isin(escolas)]

    if busca_escola.strip():
        pat = busca_escola.strip().lower()
        out = out[out["RazaoSocial"].str.lower().str.contains(pat, na=False)]

    return out


# =========================================================
# SAFE MULTISELECT WITH SELECT ALL / CLEAR
# =========================================================
def multiselect_with_buttons(
    label: str,
    options: List[str],
    key: str,
    default: Optional[List[str]] = None,
    help_text: Optional[str] = None,
) -> List[str]:
    if default is None:
        default = []

    if key not in st.session_state:
        st.session_state[key] = list(default)

    def _select_all():
        st.session_state[key] = list(options)

    def _clear():
        st.session_state[key] = []

    cols = st.columns([1, 1], gap="small")
    with cols[0]:
        st.button("Selecionar todos", on_click=_select_all, key=f"{key}__btn_all", use_container_width=True)
    with cols[1]:
        st.button("Limpar", on_click=_clear, key=f"{key}__btn_clear", use_container_width=True)

    return st.multiselect(
        label,
        options=options,
        key=key,
        help=help_text,
    )


# =========================================================
# KPI + CHARTS
# =========================================================
def kpis_for_mun(df: pd.DataFrame, mun_key: str) -> Tuple[float, int, int]:
    if df is None or df.empty:
        return 0.0, 0, 0

    if "MunicipioKey" not in df.columns:
        df = enforce_schema(df, int(df["Ano"].iloc[0]) if "Ano" in df.columns and len(df) else 0)

    d = df[df["MunicipioKey"] == mun_key] if "MunicipioKey" in df.columns else df.iloc[0:0]
    total = float(d["Valor_num"].sum()) if ("Valor_num" in d.columns and not d.empty) else 0.0
    pagamentos = int(len(d)) if not d.empty else 0
    entidades = int(d["CNPJ"].nunique()) if ("CNPJ" in d.columns and not d.empty) else 0
    return total, pagamentos, entidades


def render_kpis_side_by_side(df_filt: pd.DataFrame):
    c1, c2 = st.columns(2, gap="large")

    left_key = "MAUES"
    right_key = "BOA_VISTA_DO_RAMOS"

    with c1:
        st.subheader(f"🟦 {MUN_LABEL.get(left_key, left_key)}")
        total, pags, ents = kpis_for_mun(df_filt, left_key)
        k1, k2, k3 = st.columns(3)
        k1.metric("Total Recebido", brl_int(total))
        k2.metric("Pagamentos (linhas)", br_int(pags))
        k3.metric("Entidades (CNPJ)", br_int(ents))

    with c2:
        st.subheader(f"🟩 {MUN_LABEL.get(right_key, right_key)}")
        total, pags, ents = kpis_for_mun(df_filt, right_key)
        k1, k2, k3 = st.columns(3)
        k1.metric("Total Recebido", brl_int(total))
        k2.metric("Pagamentos (linhas)", br_int(pags))
        k3.metric("Entidades (CNPJ)", br_int(ents))


def chart_evolucao_mensal_por_municipio(df_filt: pd.DataFrame):
    if df_filt.empty:
        st.info("Sem dados para os filtros selecionados.")
        return

    evo = (
        df_filt.groupby(["Mes_dt", "Mes", "MunicipioLabel"], as_index=False)
        .agg(Total=("Valor_num", "sum"))
        .sort_values(["Mes_dt", "MunicipioLabel"])
    )

    pivot = evo.pivot_table(index="Mes", columns="MunicipioLabel", values="Total", aggfunc="sum").fillna(0.0)
    pivot.index = pd.to_datetime(pivot.index + "-01", errors="coerce")
    pivot = pivot.sort_index()
    pivot.index = pivot.index.strftime("%Y-%m")

    st.line_chart(pivot)


def chart_evolucao_mensal_todos_programas(df_filt: pd.DataFrame):
    chart_evolucao_mensal_por_municipio(df_filt)


def chart_evolucao_por_grupo(df_filt: pd.DataFrame, grupos_ordem: Optional[List[str]] = None):
    if df_filt.empty:
        st.info("Sem dados para os filtros selecionados.")
        return

    base = df_filt.copy()
    base["ProgramaGrupo"] = base["ProgramaGrupo"].fillna("").astype(str).str.strip()
    base = base[base["ProgramaGrupo"] != ""]
    if base.empty:
        st.info("Sem ProgramaGrupo preenchido para os filtros selecionados.")
        return

    grupos = sorted(base["ProgramaGrupo"].unique().tolist())
    if grupos_ordem:
        ordered = [g for g in grupos_ordem if g in grupos]
        rest = [g for g in grupos if g not in ordered]
        grupos = ordered + rest

    for g in grupos:
        st.markdown(f"### {g}")
        dg = base[base["ProgramaGrupo"] == g]

        evo = (
            dg.groupby(["Mes_dt", "Mes", "MunicipioLabel"], as_index=False)
            .agg(Total=("Valor_num", "sum"))
            .sort_values(["Mes_dt", "MunicipioLabel"])
        )

        pivot = evo.pivot_table(index="Mes", columns="MunicipioLabel", values="Total", aggfunc="sum").fillna(0.0)
        pivot.index = pd.to_datetime(pivot.index + "-01", errors="coerce")
        pivot = pivot.sort_index()
        pivot.index = pivot.index.strftime("%Y-%m")

        st.line_chart(pivot)


def chart_distribuicao_por_grupo_barras(df_filt: pd.DataFrame):
    if df_filt.empty:
        st.info("Sem dados para os filtros selecionados.")
        return

    base = df_filt.copy()
    base["ProgramaGrupo"] = base["ProgramaGrupo"].fillna("").astype(str).str.strip()
    base = base[base["ProgramaGrupo"] != ""]
    if base.empty:
        st.info("Sem ProgramaGrupo preenchido para os filtros selecionados.")
        return

    agg = (
        base.groupby(["MunicipioKey", "MunicipioLabel", "ProgramaGrupo"], as_index=False)
        .agg(Total=("Valor_num", "sum"))
    )

    c1, c2 = st.columns(2, gap="large")
    for col, mun_key in [(c1, "MAUES"), (c2, "BOA_VISTA_DO_RAMOS")]:
        with col:
            mlabel = MUN_LABEL.get(mun_key, mun_key)
            st.markdown(f"### {mlabel}")
            dm = agg[agg["MunicipioKey"] == mun_key].sort_values("Total", ascending=False)
            if dm.empty:
                st.caption("Sem dados para este município com os filtros atuais.")
                continue

            top = dm.head(12).copy()
            bar = top.set_index("ProgramaGrupo")[["Total"]]
            st.bar_chart(bar)

            tbl = top.copy()
            tbl["Total (R$)"] = tbl["Total"].apply(brl_int)
            st.dataframe(tbl[["ProgramaGrupo", "Total (R$)"]], use_container_width=True, hide_index=True)


def top_programas(df_filt: pd.DataFrame, top_n: int = 30) -> pd.DataFrame:
    if df_filt.empty:
        return pd.DataFrame()
    g = (
        df_filt.groupby(["Ano", "Mes", "MunicipioLabel", "Programa"], as_index=False)
        .agg(Total=("Valor_num", "sum"), Pagamentos=("Valor_num", "count"))
        .sort_values(["Ano", "Mes", "MunicipioLabel", "Total"], ascending=[True, True, True, False])
    )
    g["Total (R$)"] = g["Total"].apply(brl_int)
    g["Pagamentos"] = g["Pagamentos"].apply(br_int)
    return g.head(top_n)


def top_escolas(df_filt: pd.DataFrame, top_n: int = 30) -> pd.DataFrame:
    if df_filt.empty:
        return pd.DataFrame()
    g = (
        df_filt.groupby(["Ano", "Mes", "MunicipioLabel", "RazaoSocial", "CNPJ_formatado"], as_index=False)
        .agg(Total=("Valor_num", "sum"), Pagamentos=("Valor_num", "count"))
        .sort_values(["Ano", "Mes", "MunicipioLabel", "Total"], ascending=[True, True, True, False])
    )
    g["Total (R$)"] = g["Total"].apply(brl_int)
    g["Pagamentos"] = g["Pagamentos"].apply(br_int)
    return g.head(top_n)


def comparativos_municipios_por_programa(df_filt: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
    if df_filt.empty:
        return pd.DataFrame()
    g = (
        df_filt.groupby(["Programa", "MunicipioLabel"], as_index=False)
        .agg(Total=("Valor_num", "sum"))
    )
    tot = g.groupby("Programa", as_index=False).agg(TotalGeral=("Total", "sum")).sort_values("TotalGeral", ascending=False)
    keep = tot.head(top_n)["Programa"].tolist()
    g = g[g["Programa"].isin(keep)]
    pivot = g.pivot_table(index="Programa", columns="MunicipioLabel", values="Total", aggfunc="sum").fillna(0.0)
    pivot = pivot.reset_index()
    pivot["TotalGeral"] = pivot.drop(columns=["Programa"]).sum(axis=1)
    pivot = pivot.sort_values("TotalGeral", ascending=False).drop(columns=["TotalGeral"])
    for c in pivot.columns:
        if c != "Programa":
            pivot[c] = pivot[c].apply(brl_int)
    return pivot


# =========================================================
# REGULARIZAÇÃO / ALERTAS
# =========================================================
def load_regularizacao(data_dir: Path, year: int) -> pd.DataFrame:
    p1 = data_dir / f"regularizacao_{year}.csv"
    if p1.exists():
        return pd.read_csv(p1, dtype=str, encoding="utf-8-sig")

    p2 = data_dir / "regularizacao.csv"
    if p2.exists():
        return pd.read_csv(p2, dtype=str, encoding="utf-8-sig")

    return pd.DataFrame()


def render_regularizacao(df_reg: pd.DataFrame):
    st.subheader("🚨 Regularização / Alertas")

    if df_reg.empty:
        st.info(
            "Ainda não existe arquivo de regularização.\n\n"
            "👉 Próximo passo: no `fnde.py`, gerar `data/regularizacao_YYYY.csv`.\n"
            "Quando esse arquivo existir, esta aba vai preencher automaticamente."
        )
        return

    if "MunicipioKey" not in df_reg.columns:
        for cand in ["Municipio", "Município"]:
            if cand in df_reg.columns:
                df_reg["MunicipioKey"] = df_reg[cand].astype(str).apply(normalize_municipio_key)
                break
        if "MunicipioKey" not in df_reg.columns:
            df_reg["MunicipioKey"] = ""

    df_reg["MunicipioLabel"] = df_reg["MunicipioKey"].map(MUN_LABEL).fillna(df_reg.get("Municipio", ""))

    c1, c2 = st.columns(2, gap="large")
    for col, mun_key in [(c1, "MAUES"), (c2, "BOA_VISTA_DO_RAMOS")]:
        with col:
            st.markdown(f"### {MUN_LABEL.get(mun_key, mun_key)}")
            d = df_reg[df_reg["MunicipioKey"] == mun_key].copy()
            st.metric("Qtde com alerta", br_int(len(d)))
            cols = [c for c in ["RazaoSocial", "CNPJ_formatado", "Alerta", "Observacao", "Programa"] if c in d.columns]
            if not cols:
                cols = d.columns.tolist()
            st.dataframe(d[cols], use_container_width=True, hide_index=True)


# =========================================================
# SIDEBAR: DATA DIR + RELOAD
# =========================================================
with st.sidebar:
    st.header("Dados")

    project_dir = st.text_input(
        "Pasta do projeto (ou /data)",
        value=str(Path(".").resolve()),
        help="Aponte para a pasta onde existe o subdiretório 'data' com os CSVs gerados pelo fnde.py.",
        key="project_dir",
    )

    proj = Path(project_dir)
    data_dir = proj / "data"
    if not data_dir.exists():
        data_dir = DATA_DIR_DEFAULT

    st.caption(f"Pasta efetiva de leitura: {data_dir}")

    with st.expander("Atualização / Recarregar", expanded=False):
        st.caption("Se você acabou de gerar CSVs novos, clique para recarregar o cache.")
        if st.button("Recarregar dados (limpar cache)", use_container_width=True):
            st.cache_data.clear()
            st.success("Cache limpo. Recarregado na próxima execução.")


# =========================================================
# LOAD YEAR OPTIONS
# =========================================================
years = find_years_available(data_dir)
if not years:
    st.error(
        "Não encontrei CSVs `liberacoes_tidy_AM_*.csv` na pasta de dados.\n\n"
        f"👉 Verifique se existe: `{data_dir}` e se o `fnde.py` gerou os arquivos."
    )
    st.stop()


# =========================================================
# MAIN TITLE
# =========================================================
st.title("📊 FNDE | Dashboard Executivo")
st.caption("Baseado exclusivamente nos CSVs gerados pelo `fnde.py` (pasta /data). Valores sem centavos (R$).")


# =========================================================
# SIDEBAR FILTERS
# =========================================================
with st.sidebar:
    st.header("Filtros")
    with st.expander("Abrir/Fechar filtros", expanded=True):
        year = st.selectbox("Ano", years, index=len(years) - 1, key="year_sel")

        df_year = load_tidy_year(data_dir, year)
        if df_year.empty:
            st.warning(f"Sem dados carregados para {year}.")
        else:
            st.success(f"Arquivos TIDY encontrados para {year}: OK")

        # mês referência (YYYY-MM)
        mes_opts = ["Todos"]
        if not df_year.empty:
            mes_vals = df_year["Mes"].dropna().astype(str)
            mes_vals = sorted([m for m in mes_vals.unique().tolist() if re.match(r"^\d{4}-\d{2}$", m)])
            mes_opts = ["Todos"] + mes_vals

        mes_ref = st.selectbox("Mês (referência)", mes_opts, index=0, key="mes_ref")

        # municípios (por key)
        mun_opts = [k for k in MUN_KEYS_ORDER]
        if not df_year.empty:
            present = df_year["MunicipioKey"].dropna().astype(str).unique().tolist()
            mun_opts = [k for k in MUN_KEYS_ORDER if k in present] or mun_opts

        mun_sel_labels = multiselect_with_buttons(
            "Município",
            options=[MUN_LABEL.get(k, k) for k in mun_opts],
            key="mun_sel_labels",
            default=[MUN_LABEL.get(k, k) for k in mun_opts],
            help_text="Maués sempre aparece primeiro. Você pode filtrar 1 ou ambos.",
        )
        label_to_key = {MUN_LABEL.get(k, k): k for k in mun_opts}
        mun_sel = [label_to_key.get(x, x) for x in mun_sel_labels]

        # grupos e programas
        grupo_opts = []
        prog_opts = []
        escola_opts = []

        if not df_year.empty:
            grupo_opts = sorted([g for g in df_year["ProgramaGrupo"].dropna().astype(str).unique().tolist() if g.strip()])
            prog_opts = sorted([p for p in df_year["Programa"].dropna().astype(str).unique().tolist() if p.strip()])
            escola_opts = sorted([e for e in df_year["RazaoSocial"].dropna().astype(str).unique().tolist() if e.strip()])

        st.markdown("---")
        grupos_sel = multiselect_with_buttons(
            "Grupo (ProgramaGrupo)",
            options=grupo_opts,
            key="grupo_sel",
            default=[],
            help_text="Deixe vazio para não restringir.",
        )

        programas_sel = multiselect_with_buttons(
            "Programa",
            options=prog_opts,
            key="programa_sel",
            default=[],
            help_text="Deixe vazio para não restringir.",
        )

        st.markdown("---")
        busca_escola = st.text_input(
            "Buscar escola/entidade (texto)",
            value="",
            help="Filtra por parte do nome (Razão Social).",
            key="busca_escola",
        )

        escolas_sel = multiselect_with_buttons(
            "Escola/Entidade (Razão Social)",
            options=escola_opts,
            key="escola_sel",
            default=[],
            help_text="Opcional. Se selecionar aqui, restringe para essas escolas.",
        )

        st.caption("Dica: se algum gráfico parecer zerado, normalmente é filtro (Grupo/Programa/Escola).")


# =========================================================
# APPLY FILTERS
# =========================================================
df_year = load_tidy_year(data_dir, year)
df_filt = apply_filters(
    df=df_year,
    year=year,
    mes_ref=mes_ref,
    mun_keys=mun_sel,
    grupos=grupos_sel,
    programas=programas_sel,
    escolas=escolas_sel,
    busca_escola=busca_escola,
)
df_filt = enforce_schema(df_filt, year) if not df_filt.empty else df_filt

if df_year.empty:
    st.warning("Sem dados carregados para o ano selecionado.")
elif df_filt.empty:
    st.warning("Sem dados para os filtros selecionados. Ajuste os filtros na lateral.")


# =========================================================
# TABS
# =========================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Visão Geral",
    "Top Programas / Escolas",
    "Comparativos",
    "Regularização / Alertas",
    "Tabela Detalhada / Downloads",
])


# =========================================================
# TAB 1 — VISÃO GERAL
# =========================================================
with tab1:
    st.subheader(f"Visão Geral — {year} | Mês (ref.): {mes_ref}")

    render_kpis_side_by_side(df_filt)
    st.divider()

    st.markdown("## Evolução mensal (por município) — todos os programas")
    chart_evolucao_mensal_todos_programas(df_filt)

    st.divider()
    st.markdown("## Evolução mensal por Grupo (ProgramaGrupo)")
    st.caption("Um gráfico por Grupo. Linhas por município. (Mensal, sem dias.)")
    chart_evolucao_por_grupo(df_filt)

    st.divider()
    st.markdown("## Distribuição por Grupo (R$)")
    chart_distribuicao_por_grupo_barras(df_filt)


# =========================================================
# TAB 2 — TOP PROGRAMAS / ESCOLAS
# =========================================================
with tab2:
    st.subheader("Top Programas / Escolas")
    st.caption("Mostra Ano e Mês de referência (Mes) para leitura rápida.")

    c1, c2 = st.columns(2, gap="large")

    with c1:
        st.markdown("### Top Programas")
        tp = top_programas(df_filt, top_n=40)
        if tp.empty:
            st.info("Sem dados para Top Programas com os filtros atuais.")
        else:
            st.dataframe(
                tp[["Ano", "Mes", "MunicipioLabel", "Programa", "Total (R$)", "Pagamentos"]],
                use_container_width=True,
                hide_index=True,
            )

    with c2:
        st.markdown("### Top Escolas / Entidades")
        te = top_escolas(df_filt, top_n=40)
        if te.empty:
            st.info("Sem dados para Top Escolas com os filtros atuais.")
        else:
            st.dataframe(
                te[["Ano", "Mes", "MunicipioLabel", "RazaoSocial", "CNPJ_formatado", "Total (R$)", "Pagamentos"]],
                use_container_width=True,
                hide_index=True,
            )


# =========================================================
# TAB 3 — COMPARATIVOS
# =========================================================
with tab3:
    st.subheader("Comparativos")
    st.caption("Comparação Maués x Boa Vista do Ramos (mesma base filtrada).")

    comp = comparativos_municipios_por_programa(df_filt, top_n=20)
    if comp.empty:
        st.info("Sem dados para Comparativos com os filtros atuais.")
    else:
        st.markdown("### Top Programas — comparação por município (Total R$)")
        st.dataframe(comp, use_container_width=True, hide_index=True)


# =========================================================
# TAB 4 — REGULARIZAÇÃO / ALERTAS
# =========================================================
with tab4:
    df_reg = load_regularizacao(data_dir, year)
    render_regularizacao(df_reg)


# =========================================================
# TAB 5 — TABELA DETALHADA / DOWNLOADS
# =========================================================
with tab5:
    st.subheader("Tabela Detalhada / Downloads")
    if df_filt.empty:
        st.info("Sem dados para os filtros selecionados.")
    else:
        d = df_filt.copy()

        d["Valor (R$)"] = d["Valor_num"].apply(brl_int)

        cols = [
            "Ano", "Mes", "MunicipioLabel",
            "ProgramaGrupo", "Programa", "Parcela", "OB",
            "RazaoSocial", "CNPJ_formatado",
            "Valor (R$)",
        ]
        if "DataPgto" in d.columns:
            cols.insert(2, "DataPgto")

        if "Mes_dt" in d.columns:
            d = d.sort_values(
                ["Ano", "Mes_dt", "MunicipioLabel", "ProgramaGrupo", "Programa", "RazaoSocial"],
                ascending=True,
            )

        st.dataframe(d[cols], use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("## Downloads (Excel) — leitura rápida mensal")

        m1 = (
            d.groupby(["Ano", "Mes", "MunicipioLabel"], as_index=False)
            .agg(Total=("Valor_num", "sum"), Pagamentos=("Valor_num", "count"), Entidades=("CNPJ", "nunique"))
        )
        m1["Total (R$)"] = m1["Total"].apply(brl_int)
        m1["Pagamentos"] = m1["Pagamentos"].apply(br_int)
        m1["Entidades"] = m1["Entidades"].apply(br_int)
        m1 = m1[["Ano", "Mes", "MunicipioLabel", "Total (R$)", "Pagamentos", "Entidades"]].sort_values(
            ["Ano", "Mes", "MunicipioLabel"]
        )

        m2 = (
            d.groupby(["Ano", "Mes", "MunicipioLabel", "ProgramaGrupo"], as_index=False)
            .agg(Total=("Valor_num", "sum"))
        )
        m2["Total (R$)"] = m2["Total"].apply(brl_int)
        m2 = m2[["Ano", "Mes", "MunicipioLabel", "ProgramaGrupo", "Total (R$)"]].sort_values(
            ["Ano", "Mes", "MunicipioLabel", "Total (R$)"],
            ascending=[True, True, True, False],
        )

        m3 = (
            d.groupby(["Ano", "Mes", "MunicipioLabel", "Programa"], as_index=False)
            .agg(Total=("Valor_num", "sum"))
        )
        m3["Total (R$)"] = m3["Total"].apply(brl_int)
        m3 = m3[["Ano", "Mes", "MunicipioLabel", "Programa", "Total (R$)"]].sort_values(
            ["Ano", "Mes", "MunicipioLabel", "Total (R$)"],
            ascending=[True, True, True, False],
        )

        m4 = (
            d.groupby(["Ano", "Mes", "MunicipioLabel", "RazaoSocial", "CNPJ_formatado", "Programa"], as_index=False)
            .agg(Total=("Valor_num", "sum"))
        )
        m4["Total (R$)"] = m4["Total"].apply(brl_int)
        m4 = m4[["Ano", "Mes", "MunicipioLabel", "RazaoSocial", "CNPJ_formatado", "Programa", "Total (R$)"]].sort_values(
            ["Ano", "Mes", "MunicipioLabel", "Total (R$)"],
            ascending=[True, True, True, False],
        )

        xls = df_to_excel_bytes([
            ("Consolidado_Mensal_Municipio", m1),
            ("Mensal_por_Grupo", m2),
            ("Mensal_por_Programa", m3),
            ("Mensal_Escolas_Programa", m4),
            ("Detalhado_Filtrado", d.drop(columns=["Valor_num"], errors="ignore").copy()),
        ])

        st.download_button(
            "📥 Baixar Excel (consolidado mensal + detalhado)",
            data=xls,
            file_name=f"fnde_dashboard_{year}_mes_{mes_ref.replace('-', '') if mes_ref != 'Todos' else 'todos'}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

st.caption(
    "Notas: leitura por mês (sem centavos). Maués sempre à esquerda e BVR à direita. "
    "Se aparecer 'Sem dados', revise filtros (principalmente Grupo/Programa/Escola)."
)
