# fnde.py
# -*- coding: utf-8 -*-

import re
import time
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup


# =========================
# CONFIG
# =========================
BASE_SIMAD = "https://www.fnde.gov.br/pls/simad/"
URL_ENTIDADES = BASE_SIMAD + "internet_fnde.liberacoes_result_pc"  # lista de entidades encontradas
URL_DETALHE = BASE_SIMAD + "internet_fnde.liberacoes_result_pc"    # detalhe por entidade (via p_cgc)

UF = "AM"

# IDs dos municípios (via select do site)
MUNICIPIOS: Dict[str, str] = {
    "MAUES": "130290",
    "BOA_VISTA_DO_RAMOS": "130068",
}

# Anos
ANOS: List[int] = [2025, 2026]

# Timeout / retry
TIMEOUT = 30
SLEEP_BETWEEN_REQUESTS = 0.2

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

DATA_DIR = Path("data")


# =========================
# HELPERS
# =========================
PT_MONTH = {
    "JAN": 1, "FEV": 2, "MAR": 3, "ABR": 4, "MAI": 5, "JUN": 6,
    "JUL": 7, "AGO": 8, "SET": 9, "OUT": 10, "NOV": 11, "DEZ": 12,
}

PENDENCIA_HINTS = [
    # sinais de pendência / bloqueio / ação necessária
    r"\bpend[eê]ncia\b",
    r"\bpendente\b",
    r"\bn[aã]o\s+atualizou\b",
    r"\bn[aã]o\s+possui\s+uex\b",
    r"\bn[aã]o\s+possui\s+uex\s+cadastrada\b",
    r"\benquanto\s+n[aã]o\s+realizada\b",
    r"\bn[aã]o\s+repassar[aá]\b",
    r"\bn[aã]o\s+repasse\b",
    r"\bprecisa\s+regularizar\b",
    r"\bn[aã]o\s+consta\b.*\bpend[eê]ncia\b",  # às vezes vem misto; tratamos abaixo com regra de OK primeiro
]

OK_HINTS = [
    # sinais de "sem pendência"
    r"\bn[aã]o\s+consta\b.*\bpend[eê]ncia\b",
    r"\bn[aã]o\s+há\s+pend[eê]ncia\b",
    r"\bsem\s+pend[eê]ncia\b",
]


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def format_cnpj_14(digits14: str) -> str:
    d = only_digits(digits14).zfill(14)
    return f"{d[0:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"


def clean_text(v: str) -> str:
    """Remove quebras e espaços ruins para não quebrar CSV."""
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def brl_to_float(v: str) -> Optional[float]:
    """Converte '3.275,00' -> 3275.00"""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(".", "").replace(" ", "").replace("\xa0", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_br_date(s: str) -> Optional[pd.Timestamp]:
    """
    Aceita:
      - '06/NOV/2025'
      - '06/nov/25'
      - '24/ABR/2025'
      - '25/NOV/2025'
    """
    if s is None:
        return None
    raw = str(s).strip()
    if not raw:
        return None

    raw = raw.replace("-", "/").replace(".", "/")
    parts = raw.split("/")
    if len(parts) != 3:
        return None

    dd = parts[0].strip()
    mm = parts[1].strip().upper()
    yy = parts[2].strip()

    if mm.isdigit():
        month = int(mm)
    else:
        mm3 = mm[:3]
        month = PT_MONTH.get(mm3)
        if not month:
            return None

    if len(yy) == 2:
        yy = "20" + yy

    try:
        return pd.Timestamp(year=int(yy), month=int(month), day=int(dd))
    except Exception:
        return None


def safe_get(url: str, params: dict) -> requests.Response:
    r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r


def pick_main_table(html: str) -> Optional[pd.DataFrame]:
    """
    Pegamos a tabela que contém 'Data Pgto' (cabeçalho dos pagamentos).
    """
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return None

    for t in tables:
        flat = t.astype(str).fillna("").values.flatten()
        if any("Data Pgto" in x for x in flat):
            return t
    return None


# =========================
# DEDUPE / NORMALIZAÇÃO DE PROGRAMA
# =========================
def _is_only_number(s: str) -> bool:
    s = clean_text(s)
    return bool(re.fullmatch(r"\d+", s))


def _is_bad_program_group(title: str) -> bool:
    """
    Evita capturar '1', '2', 'BANCO DO BRASIL', etc como nome de programa/grupo.
    """
    t = clean_text(title)
    if not t:
        return True
    if _is_only_number(t):
        return True
    if t.upper() in {"BANCO DO BRASIL"}:
        return True
    if t.upper() in {"(VAZIO)", "VAZIO"}:
        return True
    return False


def normalize_programa_parcela(programa: str, parcela: str, program_group: str) -> Tuple[str, str]:
    """
    Regra:
    - Se 'programa' vier vazio ou igual à 'parcela', usa program_group.
    - Se 'programa' for só número (ex: '1','2','3'), trata como parcela e usa program_group como programa.
    """
    programa = clean_text(programa)
    parcela = clean_text(parcela)
    program_group = clean_text(program_group)

    # Se "Programa" veio numérico, isso quase sempre é a parcela
    if _is_only_number(programa):
        if not parcela:
            parcela = programa
        programa = ""

    # Se programa está vazio ou igual à parcela, usa o nome do bloco
    if (not programa) or (programa == parcela):
        if program_group and not _is_bad_program_group(program_group):
            programa = program_group

    return programa, parcela


def make_payment_id(row: dict) -> str:
    """
    ID estável de pagamento. Usa campos que definem unicidade prática.
    """
    key = "|".join([
        str(row.get("UF", "")),
        str(row.get("MunicipioID", "")),
        str(row.get("Ano", "")),
        str(row.get("CNPJ", "")),
        str(row.get("DataPgto", "")),
        str(row.get("OB", "")),
        f"{float(row.get('Valor_num', 0.0)):.2f}",
        str(row.get("Conta", "")),
        str(row.get("Agencia", "")),
        str(row.get("Banco", "")),
    ])
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def dedupe_and_flag_conflicts(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    - Remove duplicatas EXATAS (mesma chave + mesmo valor etc).
    - Marca conflitos: mesma chave "lógica" (CNPJ+Data+OB+Conta) com valores diferentes.
    """
    if df.empty:
        return df, pd.DataFrame()

    df = df.copy()

    # 1) remove duplicatas exatas pelo PaymentID
    df["PaymentID"] = df.apply(lambda r: make_payment_id(r.to_dict()), axis=1)
    df = df.drop_duplicates(subset=["PaymentID"], keep="first").copy()

    # 2) conflitos: mesma OB no mesmo dia/conta/CNPJ mas valores diferentes
    conflict_key = ["UF", "MunicipioID", "Ano", "CNPJ", "DataPgto", "OB", "Conta", "Agencia", "Banco"]
    g = df.groupby(conflict_key, dropna=False)["Valor_num"].nunique().reset_index(name="n_valores")
    conflicts_keys = g[g["n_valores"] > 1]

    if conflicts_keys.empty:
        return df, pd.DataFrame()

    df_conf = (
        df.merge(conflicts_keys[conflict_key], on=conflict_key, how="inner")
          .sort_values(conflict_key + ["Valor_num"])
    )
    return df, df_conf


# =========================
# EXTRAÇÃO DE ENTIDADES (CNPJ / RAZÃO)
# =========================
def extract_entidades(ano: int, municipio_id: str) -> pd.DataFrame:
    """
    Abre a tela de resultado e extrai:
      - CNPJ formatado
      - Razão social
      - onclick (enviarFormulario(...)) para pegar o p_cgc (14 dígitos)
    """
    params = {
        "p_uf": UF,
        "p_ano": str(ano),
        "p_municipio": municipio_id,
        "p_programa": "",
    }
    r = safe_get(URL_ENTIDADES, params=params)
    soup = BeautifulSoup(r.text, "lxml")

    anchors = soup.find_all("a", onclick=True)
    rows = []
    for a in anchors:
        oc = a.get("onclick", "")
        if "enviarFormulario" not in oc:
            continue

        cnpj_fmt = clean_text(a.get_text() or "")

        razao = ""
        tr = a.find_parent("tr")
        if tr:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                razao = clean_text(tds[1].get_text() or "")

        m = re.search(
            r"enviarFormulario\(\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*\)",
            oc
        )
        if not m:
            continue

        p_ano, p_programa, p_uf, p_municipio, p_tp_entidade, p_cgc = m.groups()
        p_cgc_digits = only_digits(p_cgc).zfill(14)

        if not re.search(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", cnpj_fmt):
            cnpj_fmt = format_cnpj_14(p_cgc_digits)

        rows.append({
            "Ano": int(p_ano) if str(p_ano).isdigit() else ano,
            "UF": p_uf or UF,
            "MunicipioID": p_municipio or municipio_id,
            "CNPJ": p_cgc_digits,               # string 14 dígitos
            "CNPJ_formatado": cnpj_fmt,
            "RazaoSocial": razao,
            "onclick": oc,
        })

    df = pd.DataFrame(rows).drop_duplicates(subset=["CNPJ"])
    return df


# =========================
# EXTRAÇÃO DE PAGAMENTOS (DETALHE)
# =========================
def extract_pagamentos_for_cnpj(
    ano: int,
    municipio_id: str,
    municipio_nome: str,
    cnpj_14: str,
    cnpj_fmt: str,
    razao: str,
) -> pd.DataFrame:
    """
    Para um CNPJ, chama a página de detalhe com p_cgc,
    e transforma a tabela em formato TIDY.
    """
    params = {
        "p_ano": str(ano),
        "p_programa": "",
        "p_uf": UF,
        "p_municipio": municipio_id,
        "p_tp_entidade": "",
        "p_cgc": cnpj_14,
    }

    r = safe_get(URL_DETALHE, params=params)
    html = r.text

    t = pick_main_table(html)
    if t is None or t.empty:
        return pd.DataFrame()

    df = t.copy()
    df = df.astype(str).replace("nan", "").fillna("")

    current_program_group = ""
    current_cols: List[str] = []
    out_rows = []

    for _, row in df.iterrows():
        cells = [clean_text(x) for x in row.tolist()]
        cells_clean = [c for c in cells if c and c.upper() != "NAN"]
        if not cells_clean:
            continue

        if cells_clean[0].upper().startswith("TOTAL"):
            continue

        if any(c == "Data Pgto" for c in cells_clean):
            current_cols = cells[:]  # cabeçalho do bloco
            continue

        # linha de "título" do bloco (nome do programa/grupo)
        if not current_cols:
            uniq = []
            for c in cells_clean:
                if c not in uniq:
                    uniq.append(c)
            candidate = clean_text(" ".join(uniq))
            if not _is_bad_program_group(candidate):
                current_program_group = candidate
            continue

        rec = {}
        for i, col in enumerate(current_cols):
            col = clean_text(col)
            if not col:
                continue
            rec[col] = cells[i].strip() if i < len(cells) else ""

        data_pgto = rec.get("Data Pgto", "")
        if not data_pgto:
            # às vezes o site joga o título no meio — atualiza program_group se for válido
            uniq = []
            for c in cells_clean:
                if c not in uniq:
                    uniq.append(c)
            maybe_title = clean_text(" ".join(uniq))
            if (len(maybe_title) > 12) and ("BANCO" not in maybe_title.upper()) and (not _is_bad_program_group(maybe_title)):
                current_program_group = maybe_title
            continue

        dt = parse_br_date(data_pgto)
        valor_str = clean_text(rec.get("Valor", ""))
        valor_num = brl_to_float(valor_str)

        ob = clean_text(rec.get("OB", ""))
        parcela = clean_text(rec.get("Parcela", ""))
        programa_raw = clean_text(rec.get("Programa", ""))  # às vezes vem "1"
        programa, parcela = normalize_programa_parcela(programa_raw, parcela, current_program_group)

        banco = clean_text(rec.get("Banco", ""))
        agencia = clean_text(rec.get("Agência", "")) or clean_text(rec.get("Agencia", ""))
        conta = clean_text(rec.get("C/C", "")) or clean_text(rec.get("CC", ""))

        out_rows.append({
            "UF": UF,
            "Municipio": municipio_nome,
            "MunicipioID": municipio_id,
            "Ano": int(ano),
            "DataPgto": dt.date().isoformat() if dt is not None else "",
            "Mes": dt.strftime("%Y-%m") if dt is not None else "",
            "OB": ob,
            "Valor_str": valor_str,
            "Valor_num": float(valor_num) if valor_num is not None else 0.0,
            "ProgramaGrupo": clean_text(current_program_group),
            "Programa": programa,
            "Parcela": parcela,
            "Banco": banco,
            "Agencia": agencia,
            "Conta": conta,
            "CNPJ": str(cnpj_14),
            "CNPJ_formatado": clean_text(cnpj_fmt),
            "RazaoSocial": clean_text(razao),
        })

    return pd.DataFrame(out_rows)


def build_resumo(df_tidy: pd.DataFrame) -> pd.DataFrame:
    if df_tidy.empty:
        return pd.DataFrame()

    gcols = ["UF", "Municipio", "Ano", "CNPJ", "CNPJ_formatado", "RazaoSocial"]
    out = (
        df_tidy
        .groupby(gcols, dropna=False)
        .agg(
            TotalRecebido=("Valor_num", "sum"),
            QtdePagamentos=("Valor_num", "count"),
            PrimeiroPgto=("DataPgto", "min"),
            UltimoPgto=("DataPgto", "max"),
        )
        .reset_index()
        .sort_values(["Municipio", "Ano", "TotalRecebido"], ascending=[True, True, False])
    )
    return out


# =========================
# PDDE INFO (REGULARIZAÇÃO/ALERTAS) - LEITURA DE RELATÓRIO
# =========================
def _normalize_colname(c: str) -> str:
    c = clean_text(c).lower()
    # remove acentos simples sem depender de libs externas
    c = (
        c.replace("á", "a").replace("à", "a").replace("ã", "a").replace("â", "a")
         .replace("é", "e").replace("ê", "e")
         .replace("í", "i")
         .replace("ó", "o").replace("ô", "o").replace("õ", "o")
         .replace("ú", "u")
         .replace("ç", "c")
    )
    c = re.sub(r"[^a-z0-9]+", "_", c).strip("_")
    return c


def _looks_like_html_bytes(b: bytes) -> bool:
    head = b[:4000].lower()
    return (b"<html" in head) or (b"<!doctype html" in head) or (b"<table" in head)


def _read_pddeinfo_file(path: Path) -> Optional[pd.DataFrame]:
    """
    Lê arquivo do PDDE Info que pode vir:
      - .xlsx / .xls
      - .xls que na verdade é HTML (muito comum)
      - .html
    Retorna DF único com colunas do relatório.
    """
    try:
        raw = path.read_bytes()
        if len(raw) < 200:  # arquivo vazio
            return None

        # Se for HTML disfarçado
        if _looks_like_html_bytes(raw) or path.suffix.lower() in [".html", ".htm"]:
            try:
                tables = pd.read_html(StringIO(raw.decode("utf-8", errors="ignore")))
                if not tables:
                    return None
                # escolhe a maior tabela
                t = max(tables, key=lambda x: x.shape[0] * x.shape[1])
                return t
            except Exception:
                return None

        # Tenta Excel (xlsx/xls) com engines diferentes
        # Observação: muitas vezes o site baixa "xls" que é HTML, já tratado acima.
        try:
            df = pd.read_excel(path, dtype=str, engine="openpyxl")
            return df
        except Exception:
            pass

        try:
            # xlrd pode não estar instalado; se estiver, ajuda em .xls antigos
            df = pd.read_excel(path, dtype=str, engine="xlrd")
            return df
        except Exception:
            pass

        # Último fallback: tenta ler como HTML por string
        try:
            s = raw.decode("utf-8", errors="ignore")
            tables = pd.read_html(StringIO(s))
            if tables:
                return max(tables, key=lambda x: x.shape[0] * x.shape[1])
        except Exception:
            pass

        return None
    except Exception:
        return None


def _find_pddeinfo_file(ano: int, municipio_nome: str) -> Optional[Path]:
    """
    Procura arquivo PDDE Info em data/ aceitando variações de extensão e nome.
    Ex.: pddeinfo_AM_2025_MAUES.xlsx
         pddeinfo_AM_2025_MAUES.xls
         pddeinfo_AM_2025_MAUES.xlsx.xls
         pddeinfo_AM_2025_MAUES.html
    """
    mun = municipio_nome.upper()
    candidates = []

    # padrões mais específicos primeiro
    patterns = [
        f"pddeinfo_{UF}_{ano}_{mun}.*",
        f"pddeinfo_{UF}_{ano}_{mun}*.*",
        f"*pddeinfo*{UF}*{ano}*{mun}*",
    ]
    for pat in patterns:
        candidates.extend(sorted(DATA_DIR.glob(pat)))

    # remove duplicados mantendo ordem
    seen = set()
    uniq = []
    for p in candidates:
        if p.resolve() in seen:
            continue
        seen.add(p.resolve())
        uniq.append(p)

    # prefere arquivos maiores
    uniq.sort(key=lambda p: p.stat().st_size if p.exists() else 0, reverse=True)

    return uniq[0] if uniq else None


def _classify_status(text: str) -> str:
    """
    Classifica texto em OK / PENDENTE / DESCONHECIDO.
    Regra: se bate OK_HINTS -> OK (prioridade)
           senão se bate PENDENCIA_HINTS -> PENDENTE
           senão -> DESCONHECIDO
    """
    t = clean_text(text).lower()
    if not t:
        return "DESCONHECIDO"

    for rx in OK_HINTS:
        if re.search(rx, t, flags=re.IGNORECASE):
            return "OK"

    for rx in PENDENCIA_HINTS:
        if re.search(rx, t, flags=re.IGNORECASE):
            return "PENDENTE"

    return "DESCONHECIDO"


def build_regularizacao_from_pddeinfo(ano: int, municipio_nome: str) -> pd.DataFrame:
    """
    Gera um dataframe com alertas/regularização a partir do PDDE Info (relatório detalhado).
    Você baixa do site e coloca em data/ com nome pddeinfo_AM_YYYY_MUNICIPIO.(xlsx|xls|html).
    """
    path = _find_pddeinfo_file(ano=ano, municipio_nome=municipio_nome)
    if not path:
        print(f"⚠️ [REG] Não achei Excel/HTML do PDDE Info para {ano} / {municipio_nome}. Coloque o arquivo na pasta data/.")
        return pd.DataFrame()

    df_raw = _read_pddeinfo_file(path)
    if df_raw is None or df_raw.empty:
        print(f"⚠️ [REG] Falha lendo PDDE Info: {path.name} (arquivo vazio ou formato não reconhecido).")
        return pd.DataFrame()

    # normaliza colunas
    df = df_raw.copy()
    df.columns = [clean_text(c) for c in df.columns]
    norm = {_normalize_colname(c): c for c in df.columns}

    def pick_col(*keys: str) -> Optional[str]:
        for k in keys:
            if k in norm:
                return norm[k]
        return None

    # colunas típicas (variam um pouco conforme relatório)
    col_municipio = pick_col("munic", "municipio", "municipio_fnde")
    col_rede = pick_col("rede_de_ensino", "rede", "co_esfera_adm", "esfera_adm")
    col_cod = pick_col("cod", "cod_escola", "codigo_da_escola", "codigo_escola")
    col_nome = pick_col("nome_escola", "nome", "escola", "unidade_escolar")
    col_cnpj = pick_col("cnpj")
    col_uex = pick_col("unidade_executora", "unidadeexecutora", "u_executora", "uex", "unid_exec")
    col_pc = pick_col("prestacao_de_contas", "prestacao_contas", "prestacao_de_contas_pddeweb")
    col_dc = pick_col("dados_cadastrais_pddeweb", "dados_cadastrais", "dados_cadastrais_pdde", "dados_cadastrais_pdde_web")

    # fallback: tenta localizar por "contas" / "cadastrais" no nome original
    if col_pc is None:
        for c in df.columns:
            if "Prestação" in c or "Prestacao" in c or "contas" in c.lower():
                col_pc = c
                break
    if col_dc is None:
        for c in df.columns:
            if "Dados Cadastrais" in c or "cadastrais" in c.lower():
                col_dc = c
                break

    # garante campos básicos
    def get_series(col: Optional[str]) -> pd.Series:
        if col and col in df.columns:
            return df[col].astype(str).map(clean_text)
        return pd.Series([""] * len(df))

    s_mun = get_series(col_municipio)
    s_rede = get_series(col_rede)
    s_cod = get_series(col_cod)
    s_nome = get_series(col_nome)
    s_cnpj = get_series(col_cnpj)
    s_uex = get_series(col_uex)
    s_pc = get_series(col_pc)
    s_dc = get_series(col_dc)

    # se o relatório não trouxe Município, usa o selecionado
    if s_mun.str.len().sum() == 0:
        s_mun = pd.Series([municipio_nome] * len(df))

    out = pd.DataFrame({
        "UF": UF,
        "Municipio": s_mun.replace("", municipio_nome).str.upper(),
        "Ano": int(ano),
        "RedeEnsino": s_rede,
        "CodEscola": s_cod,
        "NomeEscola": s_nome,
        "CNPJ": s_cnpj.map(lambda x: only_digits(x).zfill(14) if only_digits(x) else ""),
        "CNPJ_formatado": s_cnpj.map(lambda x: format_cnpj_14(only_digits(x)) if only_digits(x) else ""),
        "UnidadeExecutora": s_uex,
        "PrestacaoDeContas": s_pc,
        "DadosCadastrais": s_dc,
    })

    out["StatusPrestacaoContas"] = out["PrestacaoDeContas"].map(_classify_status)
    out["StatusDadosCadastrais"] = out["DadosCadastrais"].map(_classify_status)

    # Flags úteis
    out["PendenciaPrestacao"] = out["StatusPrestacaoContas"].eq("PENDENTE")
    out["PendenciaCadastral"] = out["StatusDadosCadastrais"].eq("PENDENTE")
    out["TemPendencia"] = out["PendenciaPrestacao"] | out["PendenciaCadastral"]

    # limpa linhas inúteis (sem código e sem nome)
    out = out[~((out["CodEscola"].astype(str).str.strip() == "") & (out["NomeEscola"].astype(str).str.strip() == ""))].copy()

    print(f"✅ [REG] PDDE Info lido: {path.name} | linhas: {len(out)}")
    return out


# =========================
# PIPELINE PRINCIPAL
# =========================
def run_for_municipio_ano(
    municipio_nome: str,
    municipio_id: str,
    ano: int
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    print(f"\n=== {municipio_nome} ({municipio_id}) | {ano} ===")

    # 1) ENTIDADES
    df_ent = extract_entidades(ano=ano, municipio_id=municipio_id)
    print(f"Entidades (CNPJ) encontradas: {len(df_ent)}")

    ent_out = DATA_DIR / f"entidades_{municipio_nome}_AM_{ano}.csv"
    df_ent.drop(columns=["onclick"], errors="ignore").to_csv(ent_out, index=False, encoding="utf-8-sig")
    print(f"CSV entidades salvo: {ent_out}")

    # 2) DETALHES (pagamentos)
    all_pay = []
    for i, r in df_ent.iterrows():
        cnpj14 = str(r["CNPJ"]).zfill(14)
        cnpj_fmt = r.get("CNPJ_formatado", format_cnpj_14(cnpj14))
        razao = r.get("RazaoSocial", "")

        df_pay = extract_pagamentos_for_cnpj(
            ano=ano,
            municipio_id=municipio_id,
            municipio_nome=municipio_nome,
            cnpj_14=cnpj14,
            cnpj_fmt=cnpj_fmt,
            razao=razao,
        )
        if not df_pay.empty:
            all_pay.append(df_pay)

        if (i + 1) % 10 == 0:
            print(f"[{municipio_nome}] pagamentos extraídos até agora: {sum(len(x) for x in all_pay)}")

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    df_tidy = pd.concat(all_pay, ignore_index=True) if all_pay else pd.DataFrame()

    # DEDUPE seguro + conflitos
    df_tidy, df_conf = dedupe_and_flag_conflicts(df_tidy)

    df_resumo = build_resumo(df_tidy)

    # 3) SALVAR POR MUNICIPIO+ANO
    tidy_out = DATA_DIR / f"liberacoes_tidy_AM_{ano}_{municipio_nome}.csv"
    resumo_out = DATA_DIR / f"liberacoes_resumo_AM_{ano}_{municipio_nome}.csv"

    df_tidy.to_csv(tidy_out, index=False, encoding="utf-8-sig")
    df_resumo.to_csv(resumo_out, index=False, encoding="utf-8-sig")

    # salva conflitos (se existirem) para auditoria
    if not df_conf.empty:
        conf_out = DATA_DIR / f"liberacoes_conflitos_AM_{ano}_{municipio_nome}.csv"
        df_conf.to_csv(conf_out, index=False, encoding="utf-8-sig")
        print(f"⚠️ Conflitos (mesma OB com valores diferentes) salvos: {conf_out} | linhas: {len(df_conf)}")

    print(f"✅ CSV TIDY salvo: {tidy_out} | linhas: {len(df_tidy)}")
    print(f"✅ CSV RESUMO salvo: {resumo_out} | linhas: {len(df_resumo)}")

    return df_ent, df_tidy, df_resumo


def main():
    ensure_dirs()

    # regularização por ano (consolidado)
    for ano in ANOS:
        tidy_year = []
        resumo_year = []
        reg_year = []

        for municipio_nome, municipio_id in MUNICIPIOS.items():
            _, df_tidy, df_resumo = run_for_municipio_ano(municipio_nome, municipio_id, ano)

            if not df_tidy.empty:
                tidy_year.append(df_tidy)
            if not df_resumo.empty:
                resumo_year.append(df_resumo)

            # PDDE Info (Regularização/Alertas) — opcional
            df_reg = build_regularizacao_from_pddeinfo(ano=ano, municipio_nome=municipio_nome)
            if not df_reg.empty:
                reg_year.append(df_reg)

        # Consolidados por ano (mantém Municipio separado via coluna)
        if tidy_year:
            df_tidy_all = pd.concat(tidy_year, ignore_index=True)
            out = DATA_DIR / f"liberacoes_tidy_AM_{ano}_MAUES_BVR.csv"
            df_tidy_all.to_csv(out, index=False, encoding="utf-8-sig")
            print(f"✅ Consolidado TIDY ({ano}) salvo: {out} | linhas: {len(df_tidy_all)}")

        if resumo_year:
            df_resumo_all = pd.concat(resumo_year, ignore_index=True)
            out = DATA_DIR / f"liberacoes_resumo_AM_{ano}_MAUES_BVR.csv"
            df_resumo_all.to_csv(out, index=False, encoding="utf-8-sig")
            print(f"✅ Consolidado RESUMO ({ano}) salvo: {out} | linhas: {len(df_resumo_all)}")

        # Regularização consolidada do ano
        if reg_year:
            df_reg_all = pd.concat(reg_year, ignore_index=True)

            # salva por ano (padrão do app.py)
            reg_out = DATA_DIR / f"regularizacao_{ano}.csv"
            df_reg_all.to_csv(reg_out, index=False, encoding="utf-8-sig")
            print(f"✅ [REG] Consolidado REGULARIZAÇÃO ({ano}) salvo: {reg_out} | linhas: {len(df_reg_all)}")

            # também salva uma visão só pendências (facilita “lista de ação”)
            pend_out = DATA_DIR / f"regularizacao_{ano}_PENDENCIAS.csv"
            df_reg_all[df_reg_all["TemPendencia"] == True].to_csv(pend_out, index=False, encoding="utf-8-sig")
            print(f"✅ [REG] Pendências ({ano}) salvo: {pend_out}")
        else:
            print(f"⚠️ [REG] Nenhum arquivo de regularização gerado para {ano} (faltou PDDE Info ou leitura falhou).")


if __name__ == "__main__":
    main()
