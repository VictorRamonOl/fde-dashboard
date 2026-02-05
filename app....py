# app.py
# FNDE/PDDE - Dashboard (Draft)
# Lê CSVs gerados pelo seu script (data/liberacoes_tidy_*.csv, data/liberacoes_resumo_*.csv, data/entidades_*.csv)
# e monta um dashboard básico (filtrável) sem depender de XLS extra.

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd
import streamlit as st


# =========================
# Config
# =========================
st.set_page_config(
    page_title="FNDE/PDDE - Dashboard (Draft)",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"


# =========================
# Helpers
# =========================
def _safe_read_csv(path: Path) -> pd.DataFrame:
    """Le CSV tentando alguns padrões comuns."""
    # Seu script parece salvar CSV padrão com vírgula.
    # Mas deixo robusto pra ; também.
    try:
        df = pd.read_csv(path, dtype=str, encoding="utf-8")
        if df.shape[1] == 1 and df.columns[0].count(";") > 0:
            # caso tenha lido tudo numa coluna só
            df = pd.read_csv(path, dtype=str, encoding="utf-8", sep=";")
        return df
    except UnicodeDecodeError:
        # fallback
        df = pd.read_csv(path, dtype=str, encoding="latin-1")
        if df.shape[1] == 1 and df.columns[0].count(";") > 0:
            df = pd.read_csv(path, dtype=str, encoding="latin-1", sep=";")
        return df


def _to_float_money(series: pd.Series) -> pd.Series:
    """Converte strings monetárias BR ('1.234,56') para float."""
    if series is None:
        return series
    s = series.astype(str).str.strip()
    s = s.replace({"nan": np.nan, "None": np.nan, "": np.nan})
    # remove R$, espaços
    s = s.str.replace("R$", "", regex=False).str.strip()
    # remove separador de milhar e troca decimal
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Escolhe a primeira coluna existente (case-insensitive) dentre candidates."""
    cols = list(df.columns)
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in cols:
            return cand
        c_low = cand.lower()
        if c_low in lower_map:
            return lower_map[c_low]
    return None


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza nomes comuns se existirem."""
    df = df.copy()

    # strip column names
    df.columns = [c.strip() for c in df.columns]

    # Alguns scripts geram "Total (R$)" e outros "Total" etc.
    # Não vamos forçar rename agressivo; só criar colunas padronizadas auxiliares.
    col_mun = _pick_col(df, ["Municipio", "Município", "MUNICIPIO"])
    col_ano = _pick_col(df, ["Ano", "ANO"])
    col_prog = _pick_col(df, ["Programa", "PROGRAMA"])
    col_total = _pick_col(df, ["Total (R$)", "Total_R$", "Total", "TOTAL", "Valor (R$)", "Valor", "VALOR"])
    col_pagtos = _pick_col(df, ["Pagamentos", "PAGAMENTOS", "Pago (R$)", "Pago", "Total Pago (R$)"])

    if col_mun and col_mun != "Municipio":
        df.rename(columns={col_mun: "Municipio"}, inplace=True)
    if col_ano and col_ano != "Ano":
        df.rename(columns={col_ano: "Ano"}, inplace=True)
    if col_prog and col_prog != "Programa":
        df.rename(columns={col_prog: "Programa"}, inplace=True)

    # colunas numéricas auxiliares (mantém original também)
    if col_total:
        df["__total_num"] = _to_float_money(df[col_total])
        df["__total_label"] = col_total
    else:
        df["__total_num"] = np.nan
        df["__total_label"] = ""

    if col_pagtos:
        df["__pagamentos_num"] = _to_float_money(df[col_pagtos])
        df["__pagamentos_label"] = col_pagtos
    else:
        df["__pagamentos_num"] = np.nan
        df["__pagamentos_label"] = ""

    # ano numérico (para ordenar)
    if "Ano" in df.columns:
        df["__ano_num"] = pd.to_numeric(df["Ano"], errors="coerce")
    else:
        df["__ano_num"] = np.nan

    return df


def _find_files(pattern: str, prefer_data_dir: bool = True) -> List[Path]:
    """Procura arquivos em data/ e na raiz (fallback)."""
    found = []
    if prefer_data_dir and DATA_DIR.exists():
        found.extend(sorted(DATA_DIR.glob(pattern)))
    found.extend(sorted(BASE_DIR.glob(pattern)))

    # dedup mantendo ordem
    seen = set()
    unique = []
    for p in found:
        if p.resolve() not in seen:
            unique.append(p)
            seen.add(p.resolve())
    return unique


def _parse_from_filename(path: Path) -> dict:
    """
    Extrai metadados do nome:
    liberacoes_tidy_AM_2025_MAUES.csv
    liberacoes_tidy_AM_2025_MAUES_BVR.csv
    entidades_MAUES_AM_2025.csv
    """
    name = path.stem

    meta = {"uf": None, "ano": None, "municipio_tag": None, "tipo": None}

    if name.startswith("liberacoes_tidy_"):
        meta["tipo"] = "tidy"
        # liberacoes_tidy_AM_2025_MAUES(_BVR)
        parts = name.split("_")
        # parts: ['liberacoes','tidy','AM','2025','MAUES',...]
        if len(parts) >= 5:
            meta["uf"] = parts[2]
            meta["ano"] = parts[3]
            meta["municipio_tag"] = "_".join(parts[4:])
    elif name.startswith("liberacoes_resumo_"):
        meta["tipo"] = "resumo"
        parts = name.split("_")
        # ['liberacoes','resumo','AM','2025','MAUES',...]
        if len(parts) >= 5:
            meta["uf"] = parts[2]
            meta["ano"] = parts[3]
            meta["municipio_tag"] = "_".join(parts[4:])
    elif name.startswith("entidades_"):
        meta["tipo"] = "entidades"
        parts = name.split("_")
        # entidades_MAUES_AM_2025
        # ['entidades','MAUES','AM','2025']
        if len(parts) >= 4:
            meta["municipio_tag"] = "_".join(parts[1:-2])
            meta["uf"] = parts[-2]
            meta["ano"] = parts[-1]

    return meta


@st.cache_data(show_spinner=False)
def load_all_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """Carrega tudo: tidy, resumo e entidades."""
    tidy_files = _find_files("liberacoes_tidy_*.csv", prefer_data_dir=True)
    resumo_files = _find_files("liberacoes_resumo_*.csv", prefer_data_dir=True)
    ent_files = _find_files("entidades_*.csv", prefer_data_dir=True)

    info = {
        "tidy_files": tidy_files,
        "resumo_files": resumo_files,
        "ent_files": ent_files,
        "base_dir": str(BASE_DIR),
        "data_dir": str(DATA_DIR),
    }

    tidy_all = []
    for fp in tidy_files:
        df = _safe_read_csv(fp)
        df = _normalize_columns(df)
        meta = _parse_from_filename(fp)
        df["__file"] = fp.name
        df["__tipo"] = meta.get("tipo")
        df["__uf"] = meta.get("uf")
        df["__ano_file"] = meta.get("ano")
        df["__municipio_file"] = meta.get("municipio_tag")
        tidy_all.append(df)

    resumo_all = []
    for fp in resumo_files:
        df = _safe_read_csv(fp)
        df = _normalize_columns(df)
        meta = _parse_from_filename(fp)
        df["__file"] = fp.name
        df["__tipo"] = meta.get("tipo")
        df["__uf"] = meta.get("uf")
        df["__ano_file"] = meta.get("ano")
        df["__municipio_file"] = meta.get("municipio_tag")
        resumo_all.append(df)

    ent_all = []
    for fp in ent_files:
        df = _safe_read_csv(fp)
        df = _normalize_columns(df)
        meta = _parse_from_filename(fp)
        df["__file"] = fp.name
        df["__tipo"] = meta.get("tipo")
        df["__uf"] = meta.get("uf")
        df["__ano_file"] = meta.get("ano")
        df["__municipio_file"] = meta.get("municipio_tag")
        ent_all.append(df)

    tidy_df = pd.concat(tidy_all, ignore_index=True) if tidy_all else pd.DataFrame()
    resumo_df = pd.concat(resumo_all, ignore_index=True) if resumo_all else pd.DataFrame()
    ent_df = pd.concat(ent_all, ignore_index=True) if ent_all else pd.DataFrame()

    return tidy_df, resumo_df, ent_df, info


def _fmt_brl(x: float) -> str:
    if pd.isna(x):
        return "-"
    # formata 1.234.567,89
    s = f"{x:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def _sort_safe(df: pd.DataFrame, by: List[str], ascending: List[bool]) -> pd.DataFrame:
    """Ordena apenas pelas colunas que existem."""
    existing = [c for c in by if c in df.columns]
    if not existing:
        return df
    # Ajusta ascending no mesmo tamanho
    asc = []
    for c in existing:
        i = by.index(c)
        asc.append(ascending[i] if i < len(ascending) else True)
    return df.sort_values(existing, ascending=asc)


# =========================
# UI - Header
# =========================
st.title("FNDE/PDDE - Dashboard (Draft)")
st.caption("Baseado nos CSVs já gerados na pasta do projeto (pasta `data/`). Sem depender de XLS extras.")


# =========================
# Load data
# =========================
tidy_df, resumo_df, ent_df, info = load_all_data()

# Sidebar: arquivos e validação
with st.sidebar:
    st.header("Dados")

    st.write("**Pasta do projeto:**")
    st.code(info["base_dir"], language="text")

    if Path(info["data_dir"]).exists():
        st.write("**Pasta data/:**")
        st.code(info["data_dir"], language="text")
    else:
        st.warning("Pasta `data/` não encontrada. Vou procurar CSVs na raiz do projeto.")

    if len(info["tidy_files"]) == 0:
        st.error("Não encontrei arquivos `liberacoes_tidy_*.csv` na pasta `data/` nem na raiz.")
        st.stop()
    else:
        st.success(f"Encontrados {len(info['tidy_files'])} arquivo(s) TIDY.")

    with st.expander("Ver lista de arquivos encontrados"):
        st.write("**TIDY**")
        for p in info["tidy_files"]:
            st.write(f"- {p}")
        st.write("**RESUMO**")
        for p in info["resumo_files"]:
            st.write(f"- {p}")
        st.write("**ENTIDADES**")
        for p in info["ent_files"]:
            st.write(f"- {p}")


# =========================
# Filters
# =========================
# Preferir Ano/Municipio/Programa do próprio CSV; senão usar tags do arquivo
if "Ano" in tidy_df.columns and tidy_df["Ano"].notna().any():
    anos = sorted([a for a in tidy_df["Ano"].dropna().unique().tolist() if str(a).strip() != ""])
else:
    anos = sorted([a for a in tidy_df["__ano_file"].dropna().unique().tolist() if str(a).strip() != ""])

if "Municipio" in tidy_df.columns and tidy_df["Municipio"].notna().any():
    municipios = sorted([m for m in tidy_df["Municipio"].dropna().unique().tolist() if str(m).strip() != ""])
else:
    municipios = sorted([m for m in tidy_df["__municipio_file"].dropna().unique().tolist() if str(m).strip() != ""])

programas = sorted([p for p in tidy_df["Programa"].dropna().unique().tolist()]) if "Programa" in tidy_df.columns else []

colf1, colf2, colf3, colf4 = st.columns([1, 2, 2, 2])

with colf1:
    ano_sel = st.selectbox("Ano", options=anos, index=0 if anos else None)

with colf2:
    muni_sel = st.multiselect("Município", options=municipios, default=municipios[:1] if municipios else [])

with colf3:
    prog_sel = st.multiselect("Programa", options=programas, default=[])

with colf4:
    arquivo_sel = st.multiselect(
        "Arquivos (opcional)",
        options=sorted(tidy_df["__file"].unique().tolist()),
        default=[],
        help="Se vazio, considera todos os arquivos TIDY encontrados.",
    )

# aplica filtros
df = tidy_df.copy()

# filtro ano (tenta por Ano; senão por __ano_file)
if ano_sel:
    if "Ano" in df.columns and df["Ano"].notna().any():
        df = df[df["Ano"].astype(str) == str(ano_sel)]
    else:
        df = df[df["__ano_file"].astype(str) == str(ano_sel)]

# filtro município (tenta por Municipio; senão por __municipio_file)
if muni_sel:
    if "Municipio" in df.columns and df["Municipio"].notna().any():
        df = df[df["Municipio"].isin(muni_sel)]
    else:
        df = df[df["__municipio_file"].isin(muni_sel)]

# filtro programa
if prog_sel and "Programa" in df.columns:
    df = df[df["Programa"].isin(prog_sel)]

# filtro arquivo
if arquivo_sel:
    df = df[df["__file"].isin(arquivo_sel)]

df = _normalize_columns(df)


# =========================
# KPIs
# =========================
total = float(df["__total_num"].sum(skipna=True)) if "__total_num" in df.columns else 0.0
pagtos = float(df["__pagamentos_num"].sum(skipna=True)) if "__pagamentos_num" in df.columns else np.nan

k1, k2, k3, k4 = st.columns(4)
k1.metric("Registros (linhas)", value=f"{len(df):,}".replace(",", "."))
k2.metric("Total (somado)", value=_fmt_brl(total))
k3.metric("Pagamentos (somado)", value=_fmt_brl(pagtos) if not pd.isna(pagtos) else "-")
k4.metric("Arquivos usados", value=str(df["__file"].nunique()))


# =========================
# Tabs
# =========================
tab1, tab2, tab3, tab4 = st.tabs(["Visão Geral", "Programas", "Entidades (Escolas)", "Diagnóstico / Debug"])

with tab1:
    st.subheader("Tabela base (filtrada)")

    # Escolhe colunas "boas" para exibir, sem quebrar
    show_cols = []
    for c in ["Municipio", "Ano", "Programa"]:
        if c in df.columns:
            show_cols.append(c)

    # tenta achar colunas chave comuns
    for c in ["CNPJ", "Nome Escola", "Escola", "Unidade Executora", "Parcela", "Data", "Competência"]:
        if c in df.columns and c not in show_cols:
            show_cols.append(c)

    # adiciona coluna monetária original (label)
    total_label = df["__total_label"].dropna().iloc[0] if "__total_label" in df.columns and df["__total_label"].notna().any() else None
    if total_label and total_label in df.columns and total_label not in show_cols:
        show_cols.append(total_label)

    pag_label = df["__pagamentos_label"].dropna().iloc[0] if "__pagamentos_label" in df.columns and df["__pagamentos_label"].notna().any() else None
    if pag_label and pag_label in df.columns and pag_label not in show_cols:
        show_cols.append(pag_label)

    # se não achou nada, mostra tudo
    if not show_cols:
        show_cols = df.columns.tolist()

    # Ordenação segura (corrige seu erro do 'Total')
    # Primeiro tenta por Ano/Municipio e depois pelo total numérico (quando existir).
    df_view = df.copy()
    if "__total_num" in df_view.columns:
        df_view["Total_calc"] = df_view["__total_num"]
        sort_by = [c for c in ["Ano", "Municipio", "Total_calc"] if c in df_view.columns]
        df_view = _sort_safe(df_view, sort_by, [True, True, False])
    else:
        df_view = _sort_safe(df_view, [c for c in ["Ano", "Municipio"] if c in df_view.columns], [True, True])

    st.dataframe(
        df_view[show_cols].reset_index(drop=True),
        use_container_width=True,
        hide_index=True,
    )

    # Download
    st.download_button(
        "Baixar CSV (filtrado)",
        data=df_view.to_csv(index=False).encode("utf-8"),
        file_name="fnde_pdde_filtrado.csv",
        mime="text/csv",
    )

with tab2:
    st.subheader("Resumo por Programa")

    if "Programa" not in df.columns:
        st.warning("Coluna 'Programa' não encontrada no TIDY. Verifique o CSV gerado.")
    else:
        grp = df.groupby(["Programa"], dropna=False).agg(
            Total=("__total_num", "sum"),
            Pagamentos=("__pagamentos_num", "sum"),
            Linhas=("__file", "count"),
        ).reset_index()

        grp["Total (R$)"] = grp["Total"].apply(_fmt_brl)
        grp["Pagamentos (R$)"] = grp["Pagamentos"].apply(_fmt_brl)
        grp = grp.sort_values("Total", ascending=False)

        st.dataframe(
            grp[["Programa", "Total (R$)", "Pagamentos (R$)", "Linhas"]],
            use_container_width=True,
            hide_index=True,
        )

        st.download_button(
            "Baixar resumo por programa (CSV)",
            data=grp.to_csv(index=False).encode("utf-8"),
            file_name="fnde_resumo_programa.csv",
            mime="text/csv",
        )

with tab3:
    st.subheader("Entidades / Escolas (cadastro, prestação de contas, destinação)")

    if ent_df.empty:
        st.warning("Não encontrei arquivos `entidades_*.csv` (na pasta data/ ou raiz).")
    else:
        ent = ent_df.copy()

        # Filtra ano/município similar ao tidy
        if ano_sel:
            if "__ano_file" in ent.columns:
                ent = ent[ent["__ano_file"].astype(str) == str(ano_sel)]
        if muni_sel:
            if "__municipio_file" in ent.columns:
                # municípios nos entidades podem estar em tags (MAUES, BOA_VISTA_DO_RAMOS etc)
                ent = ent[ent["__municipio_file"].isin(muni_sel) | ent["__municipio_file"].isin([m.replace(" ", "_").upper() for m in muni_sel])]

        # Colunas esperadas
        preferred = []
        for c in ["Nome Escola", "CNPJ", "Unidade Executora", "Prestação de Contas", "Dados Cadastrais - PDDEWeb", "Destinação"]:
            if c in ent.columns:
                preferred.append(c)

        if not preferred:
            # se o CSV tiver cabeçalho diferente, mostra tudo
            preferred = [c for c in ent.columns if not c.startswith("__")]

        # Ajuste visual: "Destinação" pode vir com quebras/linhas; deixa como está
        ent_show = ent[preferred].copy()
        st.dataframe(ent_show, use_container_width=True, hide_index=True)

        st.download_button(
            "Baixar entidades (CSV filtrado)",
            data=ent_show.to_csv(index=False).encode("utf-8"),
            file_name="fnde_entidades_filtrado.csv",
            mime="text/csv",
        )

with tab4:
    st.subheader("Diagnóstico / Debug rápido (para não travar)")

    st.write("**Amostra do DF filtrado (primeiras 20 linhas):**")
    st.dataframe(df.head(20), use_container_width=True)

    st.write("**Colunas encontradas no TIDY:**")
    st.code("\n".join(df.columns.tolist()), language="text")

    st.write("**Colunas monetárias detectadas:**")
    if "__total_label" in df.columns and df["__total_label"].notna().any():
        st.success(f"Total detectado como: {df['__total_label'].dropna().iloc[0]}")
    else:
        st.warning("Não detectei coluna de Total. Verifique se existe 'Total (R$)' ou 'Valor (R$)' no CSV.")

    if "__pagamentos_label" in df.columns and df["__pagamentos_label"].notna().any():
        st.success(f"Pagamentos detectado como: {df['__pagamentos_label'].dropna().iloc[0]}")
    else:
        st.info("Não detectei coluna de Pagamentos (isso é ok se seu TIDY não tiver essa info).")

    st.write("**Arquivos TIDY usados agora:**")
    st.code("\n".join(sorted(df["__file"].unique().tolist())), language="text")


# Footer
st.caption("Draft funcional: o objetivo aqui é você conseguir mostrar algo hoje e refinar depois.")
