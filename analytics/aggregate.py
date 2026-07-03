"""
analytics/aggregate.py
-----------------------
Le o parquet acumulado e gera TODOS os arquivos que o dashboard consome:
  site/data/*.json    (lidos diretamente pelo site/index.html via fetch)
  site/data_csv/*.csv (espelho CSV dos JSONs, para analise externa)

Suporta dois schemas:
  Schema CIMT (producao): date, trade_type, hs2, country_code, country_name, province, value_cad
  Schema simplificado (DEMO): date, trade_type, country_name, value_cad

Arquivos gerados:
  monthly.{json,csv}               totais mensais (exports, imports, balance)
  countries.{json,csv}             totais por pais (top N)
  countries_monthly.{json,csv}     serie mensal dos top N paises
  commodities.{json,csv}           totais por capitulo HS2
  commodities_monthly.{json,csv}   serie mensal dos top N capitulos HS2
  provinces.{json,csv}             totais por provincia (import+export, ultimos 12 meses, snapshot)
  provinces_commodities.{json,csv} provincia x HS2 (import+export, ultimos 12 meses, snapshot)
  provinces_monthly.{json,csv}     serie mensal por provincia (import+export) — usada pelo filtro De/Ate
  provinces_commodities_monthly.{json,csv}  serie mensal provincia x HS2 (import+export)
  metadata.{json,csv}              metadados do dataset

Provincia nas exportacoes: cada provincia real reflete exportacoes DOMESTICAS
(produzidas na provincia; validam contra a tabela oficial 12-10-0175). As
re-exportacoes (mercadoria estrangeira em transito) nao tem provincia de origem
atribuivel pela StatCan — nem na 12-10-0175, onde "Re-export" so aparece no
nivel "Canada" — e por isso vao para o bucket "ZZ" (Unspecified). Assim:
  soma das provincias reais (exports) = exportacao DOMESTICA nacional
  soma de tudo, incluindo ZZ         = exportacao TOTAL nacional
Nada e descartado (gap zero preservado). Ver metadata: export_domestic_by_province_cad
e export_re_export_not_allocated_cad.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
LOG = logging.getLogger(__name__)

PARQUET_PATH = Path(os.environ.get("PARQUET_PATH", "data/canada_trade_full.parquet"))
JSON_DIR = Path("site/data")
CSV_DIR  = Path("site/data_csv")
TOP_N    = 20

HS2_NAMES: dict[str, str] = {
    "01": "Live animals", "02": "Meat & offal", "03": "Fish & seafood",
    "04": "Dairy products", "05": "Other animal products", "06": "Live trees & plants",
    "07": "Vegetables", "08": "Fruit & nuts", "09": "Coffee, tea & spices",
    "10": "Cereals", "11": "Milling products", "12": "Oil seeds",
    "13": "Resins & gums", "14": "Vegetable materials", "15": "Animal/veg fats & oils",
    "16": "Prepared meat & fish", "17": "Sugars", "18": "Cocoa & cocoa products",
    "19": "Prepared cereals", "20": "Prepared vegetables", "21": "Misc food preparations",
    "22": "Beverages & spirits", "23": "Food industry residues", "24": "Tobacco",
    "25": "Salt, sulphur, stone, cement", "26": "Ores, slag & ash",
    "27": "Mineral fuels & oils", "28": "Inorganic chemicals",
    "29": "Organic chemicals", "30": "Pharmaceuticals", "31": "Fertilizers",
    "32": "Tanning & dye extracts", "33": "Cosmetics & perfumes",
    "34": "Soap & cleaning products", "35": "Protein substances",
    "36": "Explosives", "37": "Photographic goods", "38": "Misc chemical products",
    "39": "Plastics", "40": "Rubber", "41": "Raw hides & skins", "42": "Leather goods",
    "43": "Furskins", "44": "Wood & wood articles", "45": "Cork", "46": "Basketware",
    "47": "Wood pulp", "48": "Paper & paperboard", "49": "Printed books & media",
    "50": "Silk", "51": "Wool", "52": "Cotton", "53": "Vegetable textile fibres",
    "54": "Man-made filaments", "55": "Man-made staple fibres", "56": "Wadding & felt",
    "57": "Carpets", "58": "Special woven fabrics", "59": "Coated textiles",
    "60": "Knitted fabrics", "61": "Knitted apparel", "62": "Woven apparel",
    "63": "Other made-up textiles", "64": "Footwear", "65": "Headgear",
    "66": "Umbrellas", "67": "Feathers & artificial flowers",
    "68": "Stone & cement articles", "69": "Ceramic products", "70": "Glass",
    "71": "Precious metals & stones", "72": "Iron & steel",
    "73": "Articles of iron & steel", "74": "Copper", "75": "Nickel",
    "76": "Aluminium", "78": "Lead", "79": "Zinc", "80": "Tin", "81": "Other base metals",
    "82": "Tools & cutlery", "83": "Miscellaneous metal articles",
    "84": "Machinery & mechanical appliances", "85": "Electrical equipment",
    "86": "Railway equipment", "87": "Vehicles", "88": "Aircraft & spacecraft",
    "89": "Ships & boats", "90": "Optical & medical instruments",
    "91": "Clocks & watches", "92": "Musical instruments",
    "93": "Arms & ammunition", "94": "Furniture", "95": "Toys & sports equipment",
    "96": "Miscellaneous manufactures", "97": "Works of art",
    "98": "Special transactions (CA)", "99": "Confidential / low-value (CA)",
}

PROVINCE_DISPLAY: dict[str, str] = {
    "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
    "NB": "New Brunswick", "NL": "Newfoundland & Labrador",
    "NT": "Northwest Territories", "NS": "Nova Scotia", "NU": "Nunavut",
    "ON": "Ontario", "PE": "Prince Edward Island",
    "QC": "Quebec", "SK": "Saskatchewan", "YT": "Yukon",
}

# Bucket para exportacoes SEM provincia de origem (re-exportacoes: mercadoria
# estrangeira em transito). A StatCan nao atribui provincia a re-exportacoes —
# nem na tabela oficial 12-10-0175. Em vez de descartar, agregamos aqui, de modo
# que: soma das provincias reais = exportacao DOMESTICA nacional (valida contra a
# StatCan) e soma de tudo (provincias + ZZ) = exportacao TOTAL nacional.
UNSPECIFIED_CODE = "ZZ"
PROVINCE_DISPLAY[UNSPECIFIED_CODE] = "Unspecified (re-exports)"


# ── Schema normalisation ──────────────────────────────────────────────────────

def _normalizar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante schema padrao para aggregacoes.
    Detecta e normaliza schema simplificado (DEMO) se necessario.
    """
    df = df.copy()

    # Schema DEMO (month/flow)
    if "month" in df.columns:
        df = df.rename(columns={"month": "date"})
    if "flow" in df.columns:
        df["trade_type"] = df["flow"].str.capitalize()

    # Garante coluna 'partner' (nome do pais para display)
    if "country_name" in df.columns and "partner" not in df.columns:
        df["partner"] = df["country_name"]
    elif "partner" not in df.columns:
        df["partner"] = "Unknown"

    # Garante coluna 'commodity' (nome HS2 para display)
    if "hs2" in df.columns and "commodity" not in df.columns:
        df["commodity"] = df["hs2"].map(HS2_NAMES).fillna(
            df["hs2"].apply(lambda x: f"HS {x}")
        )
    elif "commodity" not in df.columns:
        df["commodity"] = "Total"

    df["value_cad"] = pd.to_numeric(df["value_cad"], errors="coerce").fillna(0).astype("int64")
    return df


# ── Aggregation helpers ────────────────────────────────────────────────────────

def _pivot_tt(df: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
    """Pivot trade_type (Import/Export) into columns."""
    return (
        df.groupby(idx + ["trade_type"])["value_cad"]
        .sum()
        .unstack("trade_type", fill_value=0)
        .reset_index()
    )


def _records(pivot: pd.DataFrame, idx_cols: list[str]) -> list[dict]:
    rows = []
    for _, row in pivot.iterrows():
        imp = int(row.get("Import", 0))
        exp = int(row.get("Export", 0))
        rec = {c: str(row[c]) if not isinstance(row[c], (int, float)) else row[c]
               for c in idx_cols}
        rec.update(imports=imp, exports=exp, total=imp + exp)
        rows.append(rec)
    return rows


# ── Builders ──────────────────────────────────────────────────────────────────

def build_monthly(df: pd.DataFrame) -> list[dict]:
    pivot = _pivot_tt(df, ["date"])
    result = []
    for _, row in pivot.iterrows():
        imp = int(row.get("Import", 0))
        exp = int(row.get("Export", 0))
        result.append({"date": str(row["date"]), "imports": imp,
                       "exports": exp, "balance": exp - imp})
    return sorted(result, key=lambda r: r["date"])


def build_countries(df: pd.DataFrame) -> list[dict]:
    pivot = _pivot_tt(df, ["partner"])
    return sorted(_records(pivot, ["partner"]), key=lambda r: -r["total"])


def build_countries_monthly(df: pd.DataFrame) -> list[dict]:
    top = df.groupby("partner")["value_cad"].sum().nlargest(TOP_N).index.tolist()
    pivot = _pivot_tt(df[df["partner"].isin(top)], ["date", "partner"])
    return sorted(_records(pivot, ["date", "partner"]),
                  key=lambda r: (r["date"], -r["total"]))


def build_commodities(df: pd.DataFrame) -> list[dict]:
    if "commodity" not in df.columns or df["commodity"].eq("Total").all():
        return []
    pivot = _pivot_tt(df, ["commodity"])
    return sorted(_records(pivot, ["commodity"]), key=lambda r: -r["total"])


def build_commodities_monthly(df: pd.DataFrame) -> list[dict]:
    if "commodity" not in df.columns or df["commodity"].eq("Total").all():
        return []
    top = df.groupby("commodity")["value_cad"].sum().nlargest(TOP_N).index.tolist()
    pivot = _pivot_tt(df[df["commodity"].isin(top)], ["date", "commodity"])
    return sorted(_records(pivot, ["date", "commodity"]),
                  key=lambda r: (r["date"], -r["total"]))


def _province_coded(df: pd.DataFrame, trade_type: str) -> pd.DataFrame:
    """Linhas de um trade_type com uma coluna 'pcode' de provincia normalizada:
    o codigo real de 2 letras, ou 'ZZ' (Unspecified) para linhas sem provincia
    de origem — i.e. re-exportacoes, que a StatCan nao atribui a provincia.
    Nada e descartado, entao a soma sobre 'pcode' == total nacional do trade_type.

    Importacoes: sempre tem provincia (despacho), entao nao geram bucket ZZ.
    Exportacoes: provincias reais = domestica; ZZ = re-exportacao (residual).
    """
    empty = df.iloc[0:0].copy()
    empty["pcode"] = pd.Series(dtype="object")
    if "province" not in df.columns:
        return empty
    sub = df[df["trade_type"] == trade_type].copy()
    if sub.empty:
        return empty
    p = sub["province"].astype("string").str.strip()
    is_real = (p.notna() & (p != "") & (p.str.lower() != "none") & (p.str.lower() != "nan")).fillna(False)
    sub["pcode"] = p.where(is_real, UNSPECIFIED_CODE).astype(str)
    return sub


def build_provinces(df: pd.DataFrame) -> list[dict]:
    """Import + export por provincia — janela dos ultimos 12 meses (snapshot).
    exports = domestica por provincia (valida); bucket ZZ carrega re-exportacao.
    """
    if "province" not in df.columns:
        return []
    imp_all = _province_coded(df, "Import")
    exp_all = _province_coded(df, "Export")
    if imp_all.empty and exp_all.empty:
        return []

    max_date = max(d for d in [
        imp_all["date"].max() if not imp_all.empty else None,
        exp_all["date"].max() if not exp_all.empty else None,
    ] if d is not None)
    max_period = pd.Period(max_date, "M")
    min_period = max_period - 11
    imp = imp_all[(imp_all["date"] >= str(min_period)) & (imp_all["date"] <= max_date)]
    exp = exp_all[(exp_all["date"] >= str(min_period)) & (exp_all["date"] <= max_date)]

    imp_agg = imp.groupby("pcode")["value_cad"].sum().rename("imports")
    exp_agg = exp.groupby("pcode")["value_cad"].sum().rename("exports")
    agg = pd.concat([imp_agg, exp_agg], axis=1).fillna(0).reset_index()
    agg.columns = ["code", "imports", "exports"]
    agg["name"] = agg["code"].map(PROVINCE_DISPLAY).fillna(agg["code"])
    agg["total"] = agg["imports"] + agg["exports"]
    agg["period_start"] = str(min_period)
    agg["period_end"]   = str(max_period)
    return sorted(agg.to_dict("records"), key=lambda r: -r["total"])


def build_provinces_commodities(df: pd.DataFrame) -> list[dict]:
    """Import + export por provincia x HS2 — janela dos ultimos 12 meses (snapshot)."""
    if "province" not in df.columns or "hs2" not in df.columns:
        return []
    imp_all = _province_coded(df, "Import")
    exp_all = _province_coded(df, "Export")
    if imp_all.empty and exp_all.empty:
        return []

    max_date = max(d for d in [
        imp_all["date"].max() if not imp_all.empty else None,
        exp_all["date"].max() if not exp_all.empty else None,
    ] if d is not None)
    max_period = pd.Period(max_date, "M")
    min_period = max_period - 11
    imp = imp_all[(imp_all["date"] >= str(min_period)) & (imp_all["date"] <= max_date)]
    exp = exp_all[(exp_all["date"] >= str(min_period)) & (exp_all["date"] <= max_date)]

    imp_agg = imp.groupby(["pcode", "hs2"])["value_cad"].sum().rename("imports")
    exp_agg = exp.groupby(["pcode", "hs2"])["value_cad"].sum().rename("exports")
    agg = pd.concat([imp_agg, exp_agg], axis=1).fillna(0).reset_index()
    agg.columns = ["code", "hs2", "imports", "exports"]
    agg["commodity"] = agg["hs2"].map(HS2_NAMES).fillna(agg["hs2"].apply(lambda x: f"HS {x}"))
    agg["total"] = agg["imports"] + agg["exports"]
    return sorted(agg.to_dict("records"), key=lambda r: (-r["total"], r["code"]))


def build_provinces_monthly(df: pd.DataFrame) -> list[dict]:
    """Import + export por provincia, serie mensal completa.
    exports por provincia real = domestica (valida contra a StatCan); o bucket
    'ZZ' (Unspecified) carrega a re-exportacao. Assim, por mes:
      soma das provincias reais (exports) = exportacao DOMESTICA nacional
      soma de tudo, incl. ZZ           = exportacao TOTAL nacional
    Usada pelo site para somar sob o filtro global De/Ate (sem janela fixa)."""
    if "province" not in df.columns:
        return []
    imp = _province_coded(df, "Import").groupby(["date", "pcode"])["value_cad"].sum().rename("imports")
    exp = _province_coded(df, "Export").groupby(["date", "pcode"])["value_cad"].sum().rename("exports")
    if imp.empty and exp.empty:
        return []
    agg = pd.concat([imp, exp], axis=1).fillna(0).reset_index()
    agg.columns = ["date", "code", "imports", "exports"]
    agg["name"] = agg["code"].map(PROVINCE_DISPLAY).fillna(agg["code"])
    agg["total"] = agg["imports"] + agg["exports"]
    return sorted(agg.to_dict("records"), key=lambda r: (r["date"], -r["total"]))


def build_provinces_commodities_monthly(df: pd.DataFrame) -> list[dict]:
    """Import + export por provincia x HS2, serie mensal completa (com bucket ZZ)."""
    if "province" not in df.columns or "hs2" not in df.columns:
        return []
    imp = _province_coded(df, "Import").groupby(["date", "pcode", "hs2"])["value_cad"].sum().rename("imports")
    exp = _province_coded(df, "Export").groupby(["date", "pcode", "hs2"])["value_cad"].sum().rename("exports")
    if imp.empty and exp.empty:
        return []
    agg = pd.concat([imp, exp], axis=1).fillna(0).reset_index()
    agg.columns = ["date", "code", "hs2", "imports", "exports"]
    agg["commodity"] = agg["hs2"].map(HS2_NAMES).fillna(agg["hs2"].apply(lambda x: f"HS {x}"))
    agg["total"] = agg["imports"] + agg["exports"]
    return sorted(agg.to_dict("records"), key=lambda r: (r["date"], -r["total"], r["code"]))


def build_metadata(df: pd.DataFrame) -> dict:
    meta = {
        "last_updated":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_source":   "Statistics Canada — CIMT (customs basis, total exports incl. re-exports)",
        "first_period":  str(df["date"].min()),
        "last_period":   str(df["date"].max()),
        "total_rows":    int(len(df)),
    }
    if "province" in df.columns:
        exp = df[df["trade_type"] == "Export"]
        meta["export_domestic_by_province_cad"] = int(exp[exp["province"].notna()]["value_cad"].sum())
        meta["export_re_export_not_allocated_cad"] = int(exp[exp["province"].isna()]["value_cad"].sum())
    return meta


# ── Save helpers ──────────────────────────────────────────────────────────────

def _save_json(obj: object, name: str) -> None:
    path = JSON_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    n = len(obj) if isinstance(obj, list) else len(obj) if isinstance(obj, dict) else "?"
    LOG.info("  %-45s (%s items)", str(path), n)


def _save_csv(obj: list[dict] | dict, name: str) -> None:
    path = CSV_DIR / name
    if isinstance(obj, dict):
        obj = [obj]
    if not obj:
        return
    pd.DataFrame(obj).to_csv(path, index=False)
    LOG.info("  %-45s (%d rows)", str(path), len(obj))


def _save(obj: object, stem: str) -> None:
    _save_json(obj, f"{stem}.json")
    _save_csv(obj, f"{stem}.csv")   # type: ignore[arg-type]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not PARQUET_PATH.exists():
        LOG.warning("[aggregate] Parquet nao encontrado em %s. Nada a gerar.", PARQUET_PATH)
        return

    JSON_DIR.mkdir(parents=True, exist_ok=True)
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    raw = pd.read_parquet(PARQUET_PATH)
    LOG.info("[aggregate] %d linha(s) carregada(s). Colunas: %s", len(raw), list(raw.columns))

    df = _normalizar(raw)

    LOG.info("[aggregate] Gerando arquivos de dashboard…")
    _save(build_monthly(df),             "monthly")
    _save(build_countries(df),           "countries")
    _save(build_countries_monthly(df),   "countries_monthly")
    _save(build_commodities(df),         "commodities")
    _save(build_commodities_monthly(df), "commodities_monthly")
    _save(build_provinces(df),           "provinces")
    _save(build_provinces_commodities(df), "provinces_commodities")
    _save(build_provinces_monthly(df),   "provinces_monthly")
    _save(build_provinces_commodities_monthly(df), "provinces_commodities_monthly")

    # metadata so em JSON
    meta = build_metadata(df)
    _save_json(meta, "metadata.json")
    _save_csv([meta], "metadata.csv")

    LOG.info("[aggregate] Done.")


if __name__ == "__main__":
    main()
