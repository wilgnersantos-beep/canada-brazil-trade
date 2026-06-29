"""
analytics/build_dashboard_data.py
----------------------------------
Gera os arquivos site/data/*.json que o dashboard consome.

Le o parquet acumulado (PARQUET_PATH) e normaliza automaticamente o schema:

  Schema simplificado (DEMO / fase inicial):
    month, flow ("export"/"import"), partner, value_cad
    -> normalizado para o schema padrao antes de agregar

  Schema padrao (producao, gerado pelo extract_statcan.py):
    date, trade_type ("Export"/"Import"), partner, commodity, value_cad, row_type

Saida (site/data/):
  monthly.json               [{date, exports, imports, balance}]
  countries.json             [{partner, exports, imports, total}]
  countries_monthly.json     [{date, partner, exports, imports, total}]
  commodities.json           [{commodity, exports, imports, total}]
  commodities_monthly.json   [{date, commodity, exports, imports, total}]
  provinces.json             [{code, name, exports, imports, total, ...}]
  provinces_commodities.json [{code, hs2, commodity, exports, imports, total}]
  metadata.json              {last_updated, first_period, last_period, ...}
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
OUT_DIR = Path("site/data")
TOP_N = 20

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
    "98": "Special transactions (CA)", "99": "Confidential trade (CA)",
}

PROVINCE_CODES: dict[str, str] = {
    "Alberta": "AB", "British Columbia": "BC", "Manitoba": "MB",
    "New Brunswick": "NB", "Newfoundland and Labrador": "NL",
    "Northwest Territories": "NT", "Nova Scotia": "NS", "Nunavut": "NU",
    "Ontario": "ON", "Prince Edward Island": "PE",
    "Quebec": "QC", "Québec": "QC", "Saskatchewan": "SK",
    "Yukon Territory": "YT", "Yukon": "YT",
    "AB": "AB", "BC": "BC", "MB": "MB", "NB": "NB", "NL": "NL",
    "NT": "NT", "NS": "NS", "NU": "NU", "ON": "ON", "PE": "PE",
    "QC": "QC", "SK": "SK", "YT": "YT",
}

PROVINCE_DISPLAY: dict[str, str] = {
    "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
    "NB": "New Brunswick", "NL": "Newfoundland & Labrador",
    "NT": "Northwest Territories", "NS": "Nova Scotia", "NU": "Nunavut",
    "ON": "Ontario", "PE": "Prince Edward Island",
    "QC": "Quebec", "SK": "Saskatchewan", "YT": "Yukon",
}


def _normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza o schema simplificado (month/flow) para o schema padrao
    (date/trade_type/partner/commodity/value_cad/row_type).
    Se ja estiver no schema padrao, devolve sem alteracoes.
    """
    if "month" in df.columns and "flow" in df.columns:
        LOG.info("Schema simplificado detectado (month/flow) — normalizando…")
        df = df.copy()
        df = df.rename(columns={"month": "date"})
        df["trade_type"] = df["flow"].str.capitalize()   # export -> Export
        if "commodity" not in df.columns:
            df["commodity"] = "Total"
        if "row_type" not in df.columns or set(df["row_type"].unique()) == {"raw"}:
            # No schema simplificado, cada linha ja e um total mensal por parceiro.
            # Marcamos como grand_total para os builders funcionarem corretamente.
            df["row_type"] = "grand_total"
        df["value_cad"] = pd.to_numeric(df["value_cad"], errors="coerce").fillna(0).astype("int64")
        LOG.info("  Normalizado: %d linhas, datas %s → %s", len(df), df["date"].min(), df["date"].max())
        return df

    # Schema padrao — garante tipos
    df = df.copy()
    if "row_type" not in df.columns:
        df["row_type"] = "detail"
    df["value_cad"] = pd.to_numeric(df.get("value_cad", 0), errors="coerce").fillna(0).astype("int64")
    return df


def _rows(df: pd.DataFrame, row_type: str) -> pd.DataFrame:
    typed = df[df["row_type"] == row_type]
    if not typed.empty:
        return typed
    for fb in ["grand_total", "country_total", "commodity_total", "detail"]:
        if fb == row_type:
            continue
        fallback = df[df["row_type"] == fb]
        if not fallback.empty:
            LOG.warning("  row_type '%s' vazio — usando '%s' como fallback", row_type, fb)
            return fallback
    return typed


def _pivot(df: pd.DataFrame, idx: list[str]) -> pd.DataFrame:
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


def build_monthly(df: pd.DataFrame) -> list[dict]:
    rows = _rows(df, "grand_total")
    pivot = _pivot(rows, ["date"])
    result = []
    for _, row in pivot.iterrows():
        imp = int(row.get("Import", 0))
        exp = int(row.get("Export", 0))
        result.append({"date": str(row["date"]), "imports": imp, "exports": exp, "balance": exp - imp})
    return sorted(result, key=lambda r: r["date"])


def build_countries(df: pd.DataFrame) -> list[dict]:
    rows = _rows(df, "country_total")
    if rows.empty:
        # Fallback para schema simplificado sem country_total
        rows = df[df["row_type"] == "grand_total"]
    pivot = _pivot(rows, ["partner"])
    return sorted(_records(pivot, ["partner"]), key=lambda r: -r["total"])


def build_countries_monthly(df: pd.DataFrame) -> list[dict]:
    rows = _rows(df, "country_total")
    if rows.empty:
        rows = df[df["row_type"] == "grand_total"]
    top = rows.groupby("partner")["value_cad"].sum().nlargest(TOP_N).index.tolist()
    pivot = _pivot(rows[rows["partner"].isin(top)], ["date", "partner"])
    return sorted(_records(pivot, ["date", "partner"]), key=lambda r: (r["date"], -r["total"]))


def build_commodities(df: pd.DataFrame) -> list[dict]:
    rows = _rows(df, "commodity_total")
    if rows.empty:
        return []
    pivot = _pivot(rows, ["commodity"])
    return sorted(_records(pivot, ["commodity"]), key=lambda r: -r["total"])


def build_commodities_monthly(df: pd.DataFrame) -> list[dict]:
    rows = _rows(df, "commodity_total")
    if rows.empty:
        return []
    top = rows.groupby("commodity")["value_cad"].sum().nlargest(TOP_N).index.tolist()
    pivot = _pivot(rows[rows["commodity"].isin(top)], ["date", "commodity"])
    return sorted(_records(pivot, ["date", "commodity"]), key=lambda r: (r["date"], -r["total"]))


def build_metadata(df: pd.DataFrame) -> dict:
    return {
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_source":  "Statistics Canada — International Trade",
        "first_period": str(df["date"].min()),
        "last_period":  str(df["date"].max()),
        "total_rows":   int(len(df)),
    }


def _save(obj: object, name: str) -> None:
    path = OUT_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    n = len(obj) if isinstance(obj, (list, dict)) else "?"
    LOG.info("  %-45s (%s items)", str(path), n)


def main() -> None:
    if not PARQUET_PATH.exists():
        LOG.warning("Parquet nao encontrado em %s. Nada a gerar.", PARQUET_PATH)
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    raw = pd.read_parquet(PARQUET_PATH)
    LOG.info("Parquet carregado: %d linhas, colunas: %s", len(raw), list(raw.columns))

    df = _normalize_schema(raw)

    LOG.info("Gerando JSONs do dashboard…")
    _save(build_monthly(df),             "monthly.json")
    _save(build_countries(df),           "countries.json")
    _save(build_countries_monthly(df),   "countries_monthly.json")
    _save(build_commodities(df),         "commodities.json")
    _save(build_commodities_monthly(df), "commodities_monthly.json")
    _save(build_metadata(df),            "metadata.json")

    # provinces.json e provinces_commodities.json requerem coluna Province
    # (disponivel apenas com extracao real via extract_statcan). Em DEMO,
    # os arquivos existentes no repo sao mantidos intactos.
    if "Province" in raw.columns or "province" in raw.columns:
        LOG.info("Coluna Province encontrada — gerando dados provinciais…")
        _build_province_files(raw, df)
    else:
        LOG.info("Sem coluna Province — provinces.json mantido estatico.")

    LOG.info("Done — site/data/ atualizado.")


def _normalize_hs6(code: object) -> str:
    if pd.isna(code):
        return "000000"
    cleaned = str(code).replace(".", "").replace(" ", "")
    return cleaned[:6].zfill(6)


def _build_province_files(raw: pd.DataFrame, df: pd.DataFrame) -> None:
    prov_col = "Province" if "Province" in raw.columns else "province"
    value_col = "Value" if "Value" in raw.columns else "value_cad"
    hs_col = "HS" if "HS" in raw.columns else None

    p = raw[raw[prov_col].notna() & (raw[prov_col].str.strip() != "")].copy()
    if p.empty:
        return

    try:
        p["date"] = pd.to_datetime(p["date"]).dt.to_period("M").astype(str)
    except Exception:
        pass

    max_date = p["date"].max()
    min_period = str(pd.Period(max_date, "M") - 11)
    p = p[(p["date"] >= min_period) & (p["date"] <= max_date)].copy()

    p["code"] = p[prov_col].map(PROVINCE_CODES).fillna(
        p[prov_col].str.strip().str[:2].str.upper()
    )
    p["name"] = p["code"].map(PROVINCE_DISPLAY).fillna(p[prov_col])
    p["value_cad"] = pd.to_numeric(p[value_col], errors="coerce").fillna(0).astype("int64")
    trade_col = "trade_type" if "trade_type" in p.columns else "flow"

    agg = (
        p.groupby(["code", "name", trade_col])["value_cad"]
        .sum().unstack(trade_col, fill_value=0).reset_index()
    )
    provs = []
    for _, row in agg.iterrows():
        imp = int(row.get("Import", row.get("import", 0)))
        exp = int(row.get("Export", row.get("export", 0)))
        provs.append({"code": str(row["code"]), "name": str(row["name"]),
                      "exports": exp, "imports": imp, "total": imp + exp,
                      "period_start": min_period, "period_end": max_date})
    _save(sorted(provs, key=lambda r: -r["total"]), "provinces.json")

    if hs_col:
        p["hs2"] = p[hs_col].apply(_normalize_hs6).str[:2]
        p["commodity"] = p["hs2"].map(HS2_NAMES).fillna(p["hs2"].apply(lambda x: f"HS {x}"))
        agg2 = (
            p.groupby(["code", "hs2", "commodity", trade_col])["value_cad"]
            .sum().unstack(trade_col, fill_value=0).reset_index()
        )
        coms = []
        for _, row in agg2.iterrows():
            imp = int(row.get("Import", row.get("import", 0)))
            exp = int(row.get("Export", row.get("export", 0)))
            coms.append({"code": str(row["code"]), "hs2": str(row["hs2"]),
                         "commodity": str(row["commodity"]),
                         "exports": exp, "imports": imp, "total": imp + exp})
        _save(sorted(coms, key=lambda r: (-r["total"], r["code"])), "provinces_commodities.json")


if __name__ == "__main__":
    main()
