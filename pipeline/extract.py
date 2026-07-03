"""
pipeline/extract.py
-------------------
Baixa da Statistics Canada (CIMT) apenas os ANOS que ainda tenham meses
faltando no parquet acumulado (extracao incremental).

Fonte: CIMT-CICM dataset — pub. 71-607-x, produto 2021004
  Importacoes : CIMT-CICM_Imp_{year}.zip     -> ODPFN014_*.csv (HS10, detalhe, com provincia)
  Exportacoes : CIMT-CICM_Tot_Exp_{year}.zip -> ODPFN017_*.csv (HS8, detalhe, SEM provincia)
  Exp. domesticas (provincia): CIMT-CICM_Dom_Exp_{year}.zip -> ODPFN020_*.csv (HS2, COM provincia)
  Lookup paises: ODPF_6_CtyDesc.TXT (dentro de cada ZIP)

Base de apuracao: alfandega (customs), nao balanco de pagamentos.
Exportacoes: total (domesticas + re-exportacoes); arquivo "Tot_Exp".

Provincia nas exportacoes: a StatCan NAO atribui provincia de origem para
re-exportacoes (mercadoria estrangeira em transito, sem producao no Canada) —
isso vale tambem na tabela oficial 12-10-0175, onde "Re-export" so aparece no
nivel "Canada", nunca por provincia. Por isso combinamos duas fontes:
  - ODPFN020 (Dom_Exp, HS2 x pais x provincia): exportacoes domesticas, com provincia.
  - ODPFN017 (Tot_Exp, HS8): total (domestica + re-exportacao), sem provincia.
O residual (Tot_Exp - Dom_Exp, por data/hs2/pais) e gravado com province=None,
representando a parcela de re-exportacao nao atribuivel a uma provincia. Isso
preserva o total nacional de exportacoes exatamente igual ao ja validado.

MODO DEMO (env DEMO=1): dados sinteticos, sem acesso a internet.
MODO REAL (env DEMO=0): baixa os ZIPs da StatCan e processa.

Saida:
  data_raw/{year}_imp.parquet
  data_raw/{year}_exp.parquet
  data_raw/reconciliation.csv   <- relatorio de reconciliacao (gerado no final)
"""

from __future__ import annotations

import io
import logging
import os
import random
import re
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
LOG = logging.getLogger(__name__)

PARQUET_PATH = Path(os.environ.get("PARQUET_PATH", "data/canada_trade_full.parquet"))
RAW_DIR = Path("data_raw")
DEMO = os.environ.get("DEMO", "1") == "1"

# Ano inicial da serie historica.
# 2019: primeiro ano com lookup de pais completo (ODPF_6_CtyDesc.TXT ausente em 2014-2018).
# Para reincluir anos anteriores basta diminuir este valor (dados disponiveis desde 2014).
START_YEAR = 2019

_BASE_URL = "https://www150.statcan.gc.ca/n1/pub/71-607-x/2021004/zip"

# HS2 chapters considered "special" by StatCan — kept in totals, tagged for display
SPECIAL_HS2 = {"98", "99"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def meses_ja_no_parquet() -> set[str]:
    """Retorna meses (YYYY-MM) presentes no parquet.
    Se o parquet for do schema antigo (DEMO: coluna 'month'), descarta-o
    para que a carga completa seja refeita com o schema CIMT.
    """
    if not PARQUET_PATH.exists():
        return set()
    import pyarrow.parquet as pq
    cols = pq.read_schema(PARQUET_PATH).names
    if "date" in cols:
        df = pd.read_parquet(PARQUET_PATH, columns=["date"])
        return set(df["date"].astype(str).unique())
    # Schema antigo ou desconhecido — descarta para recriar com schema CIMT
    LOG.warning("Parquet com schema incompativel (colunas: %s) — descartando.", cols)
    PARQUET_PATH.unlink()
    return set()


def anos_a_baixar(meses_existentes: set[str]) -> list[int]:
    """
    Determina quais anos precisam ser (re)baixados.
    - Anos passados: so se algum mes estiver faltando.
    - Ano corrente: sempre (novos meses chegam ao longo do ano).
    """
    hoje = date.today()
    ano_atual = hoje.year
    anos = []
    for ano in range(START_YEAR, ano_atual + 1):
        if ano < ano_atual:
            esperados = {f"{ano}-{m:02d}" for m in range(1, 13)}
            if not esperados.issubset(meses_existentes):
                anos.append(ano)
        else:
            anos.append(ano_atual)  # sempre re-baixa o ano corrente
    return anos


def _download(url: str, dest: Path) -> Path:
    """Baixa um arquivo com retry, pulando se ja existir."""
    if dest.exists():
        LOG.info("  Cached: %s", dest.name)
        return dest
    LOG.info("  Baixando %s …", dest.name)
    for attempt in range(3):
        try:
            r = requests.get(url, stream=True, timeout=600)
            r.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                received = 0
                for chunk in r.iter_content(1 << 20):
                    f.write(chunk)
                    received += len(chunk)
            LOG.info("  OK — %.1f MB", received / 1e6)
            return dest
        except Exception as e:
            LOG.warning("  Tentativa %d falhou: %s", attempt + 1, e)
            if dest.exists():
                dest.unlink()
    raise RuntimeError(f"Falha ao baixar {url} apos 3 tentativas.")


def _parse_country_lookup(zip_path: Path) -> dict[str, str]:
    """
    Le ODPF_6_CtyDesc.TXT de dentro do ZIP e retorna {codigo: nome_ingles}.
    Formato fixed-width: CODE(2) NUMERIC(varies) DATE_FROM(6) DATE_TO(6) ENG_NAME(82) ...
    """
    mapping: dict[str, str] = {}
    with zipfile.ZipFile(zip_path) as zf:
        cty_files = [n for n in zf.namelist() if "CtyDesc" in n]
        if not cty_files:
            LOG.warning("  Arquivo CtyDesc nao encontrado em %s", zip_path.name)
            return mapping
        with zf.open(cty_files[0]) as f:
            txt = f.read().decode("utf-8-sig", errors="replace")
    for line in txt.splitlines():
        if len(line) < 10:
            continue
        code = line[:2].strip()
        if not code.isalpha() or len(code) != 2:
            continue
        # Extrai nome ingles: depois do code, numeric code, e dois blocos de 6 digitos (datas)
        m = re.match(
            r"^[A-Z]{2}\s+\d+\s+\d{6}\s+\d{6}\s+(.+?)(?:\s{3,}|$)",
            line
        )
        if m:
            mapping[code] = m.group(1).strip()
    LOG.info("  Country lookup: %d entradas", len(mapping))
    return mapping


def _hs_int_to_str(val: object, digits: int) -> str | None:
    """Converte codigo HS inteiro para string zero-padded."""
    if pd.isna(val):
        return None
    s = str(int(val))
    if len(s) > digits:
        return None
    return s.zfill(digits)


def _combinar_exportacoes_provincia(
    df_tot: pd.DataFrame, df_dom: pd.DataFrame
) -> pd.DataFrame:
    """
    Combina exportacoes totais (sem provincia) com exportacoes domesticas
    (com provincia), preservando o total exato por (date, hs2, country).

    Retorna: linhas com provincia (domesticas, por provincia) + linhas
    residuais com province=None (re-exportacoes, nao atribuiveis).
    """
    chave = ["date", "hs2", "country_code", "country_name"]
    # dropna=False: alguns registros tem country_code ausente (paises nao
    # mapeados); sem isso o groupby descarta essas linhas e o total deixa
    # de bater com o total nacional ja validado.
    dom_by_key = (
        df_dom.groupby(chave, dropna=False)["value_cad"].sum().reset_index()
        .rename(columns={"value_cad": "dom_total"})
    )
    tot_by_key = (
        df_tot.groupby(chave, dropna=False)["value_cad"].sum().reset_index()
        .rename(columns={"value_cad": "tot_total"})
    )
    merged = tot_by_key.merge(dom_by_key, on=chave, how="left")
    merged["dom_total"] = merged["dom_total"].fillna(0)
    merged["residual"] = merged["tot_total"] - merged["dom_total"]

    # IMPORTANTE: emite uma linha residual (province=None) para TODA chave de
    # Tot_Exp, mesmo quando residual == 0. O merge incremental do transform.py
    # deduplica por (date, trade_type, hs2, country_code, province) substituindo
    # a linha antiga pela nova de mesma chave; se uma chave que antes tinha
    # residual > 0 passar a ter residual == 0 (revisao de dados) e a linha for
    # omitida aqui, a linha antiga (com valor desatualizado) fica orfa no
    # parquet acumulado e o total passa a ficar errado (contagem duplicada).
    residual = merged[chave + ["residual"]].copy()
    residual = residual.rename(columns={"residual": "value_cad"})
    residual["trade_type"] = "Export"
    residual["province"] = None
    residual["value_cad"] = residual["value_cad"].astype("int64")

    cols = ["date", "trade_type", "hs2", "country_code", "country_name", "province", "value_cad"]
    combinado = pd.concat([df_dom[cols], residual[cols]], ignore_index=True)
    LOG.info(
        "  Exportacoes: %d linha(s) com provincia (domestica) + %d residual(is) "
        "(re-exportacao, sem provincia).",
        len(df_dom), len(residual),
    )
    return combinado


def _processar_zip(
    zip_path: Path,
    trade_type: str,
    country_map: dict[str, str],
    csv_filter: str,
    hs_col: str,
    hs_digits: int,
    tem_province: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Processa o CSV de detalhe (ODPFN014 ou ODPFN017) e o CSV de referencia HS2
    (ODPFN022 ou ODPFN021) de dentro de um ZIP.

    Retorna (df_detalhe, df_ref_hs2).
    """
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        # CSV de detalhe (HS10, HS8 ou HS2). Usa prefixo "ODPFN{filtro}_" completo —
        # um filtro como "020" bateria por acidente no sufixo de data (ex. "..._202012N.csv").
        detail_files = [n for n in names if n.endswith(".csv") and f"ODPFN{csv_filter}_" in n]
        # CSV de referencia HS2 (ODPFN022 para imports, ODPFN021 para exports)
        ref_id = "ODPFN022" if trade_type == "Import" else "ODPFN021"
        ref_files = [n for n in names if n.endswith(".csv") and ref_id in n]

        if not detail_files:
            raise RuntimeError(f"Nenhum CSV com '{csv_filter}' em {zip_path.name}")

        LOG.info("  Lendo detalhe: %s", detail_files[0])
        with zf.open(detail_files[0]) as f:
            df = pd.read_csv(f, low_memory=False, encoding="utf-8-sig",
                             dtype={hs_col: "Int64"})

        df_ref = pd.DataFrame()
        if ref_files:
            LOG.info("  Lendo referencia HS2: %s", ref_files[0])
            with zf.open(ref_files[0]) as f:
                df_ref = pd.read_csv(f, low_memory=False, encoding="utf-8-sig")

    # ── Normaliza nomes de colunas (encoding pode variar entre versoes) ────────
    col_map: dict[str, str] = {}
    for c in df.columns:
        cl = c.lower()
        if "year" in cl or "mois" in cl:
            col_map[c] = "ym"
        elif hs_col.lower() in cl.lower() or cl.startswith("hs"):
            col_map[c] = "hs_raw"
        elif "country" in cl or "pays" in cl:
            col_map[c] = "country_code"
        elif "province" in cl:
            col_map[c] = "province"
        elif "state" in cl or "tat" in cl:
            col_map[c] = "state"
        elif "value" in cl or "valeur" in cl:
            col_map[c] = "value_cad"
    df = df.rename(columns=col_map)

    # ── Converte YearMonth (202601) -> date (2026-01) ──────────────────────────
    ym = df["ym"].astype(str).str.zfill(6)
    df["date"] = ym.str[:4] + "-" + ym.str[4:6]
    df["trade_type"] = trade_type

    # ── HS code: zero-pad, extrair HS2 ────────────────────────────────────────
    # IMPORTANTE: zero-pad correto evita perda de registros com HS comecando em 0
    df["hs_str"] = df["hs_raw"].apply(lambda v: _hs_int_to_str(v, hs_digits))
    df = df[df["hs_str"].notna()].copy()
    df["hs2"] = df["hs_str"].str[:2]

    # ── Resolve nome do pais (sem descartar nao-mapeados) ─────────────────────
    df["country_name"] = df["country_code"].map(country_map).fillna(
        df["country_code"].apply(lambda c: f"Other ({c})")
    )

    # ── Province (imports only) ───────────────────────────────────────────────
    if tem_province and "province" in df.columns:
        df["province"] = df["province"].astype(str).replace({"nan": None, "<NA>": None})
    else:
        df["province"] = None

    # ── Agrega para nivel HS2 x pais x provincia x mes ────────────────────────
    # dropna=False: manter linhas com province=None (exportacoes nao tem provincia)
    group_cols = ["date", "trade_type", "hs2", "country_code", "country_name", "province"]
    result = (
        df.groupby(group_cols, dropna=False)["value_cad"]
        .sum()
        .reset_index()
    )
    result["value_cad"] = result["value_cad"].astype("int64")

    LOG.info("  Detalhe: %d linhas -> %d agregadas (HS2 x pais x prov x mes)",
             len(df), len(result))

    # ── Prepara DataFrame de referencia HS2 para reconciliacao ─────────────────
    if not df_ref.empty:
        # Normaliza coluna de valor na referencia
        ref_val_col = next((c for c in df_ref.columns
                            if "value" in c.lower() or "valeur" in c.lower()), None)
        ref_ym_col  = next((c for c in df_ref.columns
                            if "year" in c.lower() or "mois" in c.lower()), None)
        if ref_val_col and ref_ym_col:
            ym_ref = df_ref[ref_ym_col].astype(str).str.zfill(6)
            df_ref = df_ref[[ref_ym_col, ref_val_col]].copy()
            df_ref.columns = ["ym", "value_cad"]
            df_ref["date"] = ym_ref.str[:4] + "-" + ym_ref.str[4:6]
            df_ref = df_ref.groupby("date")["value_cad"].sum().reset_index()
            df_ref["trade_type"] = trade_type

    return result, df_ref


# ── DEMO mode ─────────────────────────────────────────────────────────────────

def baixar_demo(meses_existentes: set[str]) -> None:
    """Gera dados sinteticos para smoke-test."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    hoje = date.today().replace(day=1)
    candidatos = []
    for i in range(12, 0, -1):
        ano = hoje.year
        mes = hoje.month - i
        while mes <= 0:
            mes += 12
            ano -= 1
        candidatos.append(f"{ano:04d}-{mes:02d}")

    novos = [m for m in candidatos if m not in meses_existentes]
    if not novos:
        LOG.info("[DEMO] Nenhum mes novo. Nada a gerar.")
        return

    rng = random.Random(42)
    linhas = []
    for m in novos:
        for flow, trade in [("export", "Export"), ("import", "Import")]:
            val = rng.randint(400, 1200) * 1_000_000
            linhas.append({
                "date": m, "trade_type": trade,
                "hs2": "84", "country_code": "BR", "country_name": "Brazil",
                "province": None if trade == "Export" else "ON",
                "value_cad": val,
            })

    out = RAW_DIR / "demo.parquet"
    pd.DataFrame(linhas).to_parquet(out, index=False)
    LOG.info("[DEMO] Gerados %d mes(es) -> %s", len(novos), out)


# ── REAL mode ─────────────────────────────────────────────────────────────────

def baixar_real(meses_existentes: set[str]) -> None:
    """Baixa os ZIPs da StatCan (CIMT) e processa os anos faltantes."""
    anos = anos_a_baixar(meses_existentes)
    if not anos:
        LOG.info("[extract] Todos os meses estao presentes. Nada a baixar.")
        return

    LOG.info("[extract] Anos a processar: %s", anos)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Acumula dataframes para reconciliacao final
    recon_rows: list[dict] = []

    for ano in anos:
        LOG.info("\n── Ano %d ──────────────────────────────────", ano)

        imp_url = f"{_BASE_URL}/CIMT-CICM_Imp_{ano}.zip"
        exp_url = f"{_BASE_URL}/CIMT-CICM_Tot_Exp_{ano}.zip"
        domexp_url = f"{_BASE_URL}/CIMT-CICM_Dom_Exp_{ano}.zip"
        imp_zip = RAW_DIR / f"Imp_{ano}.zip"
        exp_zip = RAW_DIR / f"Exp_{ano}.zip"
        domexp_zip = RAW_DIR / f"DomExp_{ano}.zip"

        # Verifica disponibilidade
        try:
            r = requests.head(imp_url, timeout=15)
            if r.status_code == 404:
                LOG.warning("  Ano %d nao disponivel no CIMT (HTTP 404). Pulando.", ano)
                continue
        except Exception as e:
            LOG.warning("  Nao foi possivel verificar ano %d: %s", ano, e)
            continue

        _download(imp_url, imp_zip)
        _download(exp_url, exp_zip)
        try:
            _download(domexp_url, domexp_zip)
        except Exception as e:
            LOG.warning("  Dom_Exp %d indisponivel (%s) — exportacoes ficarao sem provincia.", ano, e)
            if domexp_zip.exists():
                domexp_zip.unlink()

        # Lookup de paises (pega do ZIP de importacoes)
        country_map = _parse_country_lookup(imp_zip)

        # Processa importacoes
        LOG.info("  Processando importacoes %d…", ano)
        df_imp, ref_imp = _processar_zip(
            imp_zip, "Import", country_map,
            csv_filter="014", hs_col="HS10", hs_digits=10, tem_province=True
        )
        df_imp.to_parquet(RAW_DIR / f"{ano}_imp.parquet", index=False)

        # Processa exportacoes totais (domestica + re-exportacao, sem provincia)
        LOG.info("  Processando exportacoes %d…", ano)
        df_exp, ref_exp = _processar_zip(
            exp_zip, "Export", country_map,
            csv_filter="017", hs_col="HS8", hs_digits=8, tem_province=False
        )

        # Processa exportacoes domesticas (com provincia) e combina com o total,
        # preservando o total nacional ja validado (ver _combinar_exportacoes_provincia).
        if domexp_zip.exists():
            LOG.info("  Processando exportacoes domesticas (provincia) %d…", ano)
            df_domexp, _ = _processar_zip(
                domexp_zip, "Export", country_map,
                csv_filter="020", hs_col="HS2", hs_digits=2, tem_province=True
            )
            df_exp_final = _combinar_exportacoes_provincia(df_exp, df_domexp)
        else:
            df_exp_final = df_exp

        df_exp_final.to_parquet(RAW_DIR / f"{ano}_exp.parquet", index=False)

        # Reconciliacao: detalhe vs referencia HS2
        for df_det, df_ref, tt in [
            (df_imp, ref_imp, "Import"),
            (df_exp, ref_exp, "Export"),
        ]:
            det_by_month = df_det.groupby("date")["value_cad"].sum().reset_index()
            det_by_month.columns = ["date", "detail_total"]

            if not df_ref.empty:
                merged = det_by_month.merge(
                    df_ref[["date", "value_cad"]].rename(columns={"value_cad": "ref_total"}),
                    on="date", how="outer"
                ).fillna(0)
                merged["gap_cad"] = merged["ref_total"] - merged["detail_total"]
                merged["gap_pct"] = (merged["gap_cad"] / merged["ref_total"].replace(0, 1) * 100).round(2)
                merged["trade_type"] = tt
                recon_rows.append(merged)
            else:
                det_by_month["ref_total"] = None
                det_by_month["gap_cad"] = None
                det_by_month["gap_pct"] = None
                det_by_month["trade_type"] = tt
                recon_rows.append(det_by_month)

    # Salva relatorio de reconciliacao
    if recon_rows:
        recon = pd.concat(recon_rows, ignore_index=True).sort_values(["trade_type", "date"])
        recon_path = RAW_DIR / "reconciliation.csv"
        recon.to_csv(recon_path, index=False)
        LOG.info("\n[reconciliacao] Relatorio salvo em %s", recon_path)

        # Resume no log
        LOG.info("\n%s", "=" * 60)
        LOG.info("RELATORIO DE RECONCILIACAO (gap = referencia_HS2 - detalhe_HS10)")
        LOG.info("%s", "=" * 60)
        for tt in ["Import", "Export"]:
            sub = recon[recon["trade_type"] == tt]
            if sub.empty:
                continue
            total_det = sub["detail_total"].sum()
            total_ref = sub["ref_total"].sum() if sub["ref_total"].notna().any() else None
            LOG.info("\n  %s:", tt)
            LOG.info("    Total detalhe (HS10/8): %15.0f CAD", total_det)
            if total_ref:
                LOG.info("    Total referencia (HS2): %15.0f CAD", total_ref)
                LOG.info("    Gap total:              %15.0f CAD (%.2f%%)",
                         total_ref - total_det,
                         (total_ref - total_det) / total_ref * 100 if total_ref else 0)
        LOG.info("%s\n", "=" * 60)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    existentes = meses_ja_no_parquet()
    LOG.info("[extract] %d mes(es) ja no parquet.", len(existentes))
    if DEMO:
        baixar_demo(existentes)
    else:
        baixar_real(existentes)


if __name__ == "__main__":
    main()
