"""
analytics/forecast.py
---------------------
Projecao de curto prazo (1..6 meses) do comercio bilateral Canada<->Brasil.

Roda no PIPELINE (junto do aggregate.py, no job mensal) — nada e calculado no
navegador. Le a MESMA serie mensal bilateral que o dashboard usa (parquet
acumulado, country_code == 'BR') e gera site/data/forecast.json.

Metodologia (simples e comprovada; nada de deep learning numa serie curta):
  - Tres series: importacoes (CA<-BR), exportacoes (CA->BR) e comercio total.
  - Modelo: SARIMAX log (tipo "airline") — captura tendencia + sazonalidade
    multiplicativa e da intervalos de previsao analiticos (80% e 95%). Como e
    ajustado em log, os intervalos ficam positivos e assimetricos.
  - Choque de 2020 (COVID): tratado escolhendo, por serie, o inicio do treino
    (2019 = serie cheia, ou 2021 = pos-COVID) que MELHORA o backtest.
  - Baseline obrigatorio: "ingenuo sazonal" (repete o mesmo mes do ano anterior).
  - Selecao de modelo e confianca por BACKTEST de origem movel (nao por AIC).

Regra de exibicao (honestidade):
  - "reliable"    : bate o baseline e erro baixo (MAPE h=3 <= LIMIAR_RELIABLE).
  - "directional" : bate o baseline mas com erro alto (ex.: exportacoes).
  - "low"         : NAO bate o baseline -> marcada como pouco confiavel.
O dashboard rotula cada serie conforme esse campo.

Ultimo mes: se parecer PARCIAL/incompleto (outlier baixo), e excluido do treino.
A StatCan revisa meses recentes — ver metadata.trained_through / note.

Saida: site/data/forecast.json
"""

from __future__ import annotations

import json
import logging
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

warnings.simplefilter("ignore")  # silencia ConvergenceWarning etc. no job

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
LOG = logging.getLogger(__name__)

PARQUET_PATH = Path(os.environ.get("PARQUET_PATH", "data/canada_trade_full.parquet"))
JSON_DIR = Path("site/data")

PARTNER_CODE = "BR"
H = 6                      # horizonte maximo de projecao (meses)
HISTORY_MONTHS = 36        # historico recente incluido no JSON (contexto do grafico)
LIMIAR_RELIABLE = 15.0     # MAPE (%) a h=3 abaixo do qual a serie e "reliable"

# Candidatos (ordem SARIMA) e inicios de treino avaliados por backtest.
CANDIDATE_ORDERS = [
    ((0, 1, 1), (0, 1, 1, 12)),   # airline classico
    ((1, 1, 1), (0, 1, 1, 12)),
]
CANDIDATE_STARTS = ["2019-01", "2021-01"]   # 2021 = tratamento do choque COVID

SERIES_LABELS = {
    "imports": "Imports (CA ← BR)",
    "exports": "Exports (CA → BR)",
    "total":   "Total trade",
}


# ── Carga da serie bilateral ───────────────────────────────────────────────────

def load_bilateral() -> dict[str, pd.Series]:
    """Retorna {'imports','exports','total'} como Series mensais (PeriodIndex)."""
    df = pd.read_parquet(PARQUET_PATH, columns=["date", "trade_type", "country_code", "value_cad"])
    br = df[df["country_code"] == PARTNER_CODE]
    g = (
        br.groupby(["date", "trade_type"])["value_cad"].sum()
        .unstack("trade_type", fill_value=0)
        .sort_index()
    )
    g.index = pd.PeriodIndex(g.index, freq="M")
    imp = g.get("Import", pd.Series(0, index=g.index)).astype(float)
    exp = g.get("Export", pd.Series(0, index=g.index)).astype(float)
    return {"imports": imp, "exports": exp, "total": (imp + exp).astype(float)}


def drop_partial_last(y: pd.Series) -> tuple[pd.Series, bool]:
    """Remove o ultimo mes se parecer parcial (outlier baixo vs. meses recentes)."""
    if len(y) < 6:
        return y, False
    last = y.iloc[-1]
    ref = np.median(y.iloc[-4:-1])
    if ref > 0 and last < 0.5 * ref:
        LOG.info("  Ultimo mes %s parece parcial (%.0f < 50%% da mediana recente %.0f) — excluido do treino.",
                 y.index[-1], last, ref)
        return y.iloc[:-1], True
    return y, False


# ── Modelos ─────────────────────────────────────────────────────────────────

def _fit_forecast(y_train: pd.Series, h: int, order, sorder):
    """Ajusta SARIMAX em log(y) e devolve (media, {lvl:(lower,upper)}) na escala original."""
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    res = SARIMAX(np.log(y_train.values), order=order, seasonal_order=sorder,
                  enforce_stationarity=False, enforce_invertibility=False).fit(disp=0)
    fc = res.get_forecast(h)
    mean = np.exp(fc.predicted_mean)
    ci = {}
    for lvl, alpha in [(80, 0.20), (95, 0.05)]:
        c = fc.conf_int(alpha=alpha)
        ci[lvl] = (np.exp(c[:, 0]), np.exp(c[:, 1]))
    return mean, ci


def _snaive_point(y: pd.Series, origin: int, h: int) -> float:
    """Ingenuo sazonal: valor 12 meses antes do alvo (origin+h)."""
    t = origin + h
    return float(y.iloc[t - 12]) if t - 12 >= 0 else np.nan


# ── Backtest de origem movel ────────────────────────────────────────────────

def backtest(y: pd.Series, start: str, order, sorder, min_train: int):
    """Rolling-origin. Retorna (mape_model, mae_model, mape_naive) por horizonte."""
    y2 = y[y.index >= pd.Period(start, "M")]
    n = len(y2)
    apeM = {h: [] for h in range(1, H + 1)}
    aeM  = {h: [] for h in range(1, H + 1)}
    apeN = {h: [] for h in range(1, H + 1)}
    for o in range(min_train, n - 1):
        hmax = min(H, n - 1 - o)
        try:
            mean, _ = _fit_forecast(y2.iloc[:o + 1], hmax, order, sorder)
        except Exception:
            continue
        for h in range(1, hmax + 1):
            a = float(y2.iloc[o + h])
            if a <= 0:
                continue
            apeM[h].append(abs(mean[h - 1] - a) / a)
            aeM[h].append(abs(mean[h - 1] - a))
            sn = _snaive_point(y2, o, h)
            if not np.isnan(sn):
                apeN[h].append(abs(sn - a) / a)
    mape = {h: (float(np.mean(v)) * 100 if v else None) for h, v in apeM.items()}
    mae  = {h: (float(np.mean(v)) if v else None) for h, v in aeM.items()}
    mapeN = {h: (float(np.mean(v)) * 100 if v else None) for h, v in apeN.items()}
    return mape, mae, mapeN


def _avg(d: dict) -> float:
    vals = [v for v in d.values() if v is not None]
    return float(np.mean(vals)) if vals else float("nan")


def select_model(y: pd.Series):
    """Escolhe (start, order, sorder) com menor MAPE medio no backtest; compara ao naive."""
    best = None
    for start in CANDIDATE_STARTS:
        min_train = 36 if start.startswith("2021") else 48
        if len(y[y.index >= pd.Period(start, "M")]) < min_train + 8:
            continue
        for order, sorder in CANDIDATE_ORDERS:
            try:
                mape, mae, mapeN = backtest(y, start, order, sorder, min_train)
            except Exception as e:
                LOG.warning("  backtest falhou (%s %s): %s", start, order, e)
                continue
            score = _avg(mape)
            if np.isnan(score):
                continue
            if best is None or score < best["score"]:
                best = dict(start=start, order=order, sorder=sorder, min_train=min_train,
                            mape=mape, mae=mae, mape_naive=mapeN, score=score,
                            score_naive=_avg(mapeN))
    return best


def classify(best) -> str:
    if best is None:
        return "low"
    beats = best["score"] < best["score_naive"]
    if not beats:
        return "low"
    m3 = best["mape"].get(3) or best["score"]
    return "reliable" if m3 <= LIMIAR_RELIABLE else "directional"


# ── Montagem da saida por serie ─────────────────────────────────────────────

def build_series(name: str, y_full: pd.Series) -> dict:
    y, partial = drop_partial_last(y_full)
    best = select_model(y)

    hist = [{"month": str(p), "actual": int(round(v))}
            for p, v in y.iloc[-HISTORY_MONTHS:].items()]

    forecast = []
    naive_fc = []
    if best is not None:
        y_tr = y[y.index >= pd.Period(best["start"], "M")]
        try:
            mean, ci = _fit_forecast(y_tr, H, best["order"], best["sorder"])
            last = y.index[-1]
            for i in range(H):
                p = last + (i + 1)
                lo80, up80 = ci[80][0][i], ci[80][1][i]
                lo95, up95 = ci[95][0][i], ci[95][1][i]
                # ingenuo sazonal para o mesmo mes-alvo (referencia honesta no grafico)
                sn_val = float(y.iloc[len(y) + i - 12]) if (len(y) + i - 12) >= 0 else None
                forecast.append({
                    "month": str(p),
                    "forecast": int(round(float(mean[i]))),
                    "lower80": int(round(float(lo80))), "upper80": int(round(float(up80))),
                    "lower95": int(round(float(lo95))), "upper95": int(round(float(up95))),
                    "naive": None if sn_val is None else int(round(sn_val)),
                })
        except Exception as e:
            LOG.warning("  forecast final falhou (%s): %s", name, e)
            best = None

    confidence = classify(best)
    meta = {
        "label": SERIES_LABELS.get(name, name),
        "confidence": confidence,
        "model": (f"SARIMAX{best['order']}x{best['sorder']} on log, train from {best['start'][:4]}"
                  if best else "n/a"),
        "trained_through": str(y.index[-1]),
        "last_month_excluded_partial": bool(partial),
        "backtest_mape_by_horizon": (best["mape"] if best else None),
        "backtest_mae_by_horizon": (best["mae"] if best else None),
        "baseline_mape_by_horizon": (best["mape_naive"] if best else None),
        "backtest_mape_avg": (round(best["score"], 1) if best else None),
        "baseline_mape_avg": (round(best["score_naive"], 1) if best else None),
        "beats_baseline": (bool(best["score"] < best["score_naive"]) if best else False),
        "mape_h3": (round(best["mape"].get(3), 1) if best and best["mape"].get(3) else None),
    }
    return {"history": hist, "forecast": forecast, "meta": meta}


def main() -> None:
    if not PARQUET_PATH.exists():
        LOG.warning("[forecast] Parquet nao encontrado em %s. Nada a gerar.", PARQUET_PATH)
        return
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    LOG.info("[forecast] Carregando serie bilateral CA-BR…")
    series = load_bilateral()

    out = {"series": {}, "metadata": {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "partner": "Brazil",
        "horizon_months": H,
        "method": "SARIMAX (log, airline-type) selected by rolling-origin backtest; baseline = seasonal naive",
        "assumptions": ("Short-term projection · assumes no price/FX/policy shocks · "
                        "recent months subject to revision by Statistics Canada"),
        "note": "Statistics Canada revises recent months; the most recent month may be updated in later releases.",
    }}
    for name, y in series.items():
        LOG.info("[forecast] Serie '%s' (%d meses)…", name, len(y))
        try:
            res = build_series(name, y)
        except Exception as e:
            # Nunca derruba o job mensal por causa de uma serie: degrada para "low".
            LOG.warning("  Falha ao projetar '%s' (%s) — marcada como low confidence.", name, e)
            res = {"history": [], "forecast": [],
                   "meta": {"label": SERIES_LABELS.get(name, name), "confidence": "low",
                            "model": "n/a", "trained_through": None,
                            "last_month_excluded_partial": False,
                            "backtest_mape_by_horizon": None, "backtest_mae_by_horizon": None,
                            "baseline_mape_by_horizon": None, "backtest_mape_avg": None,
                            "baseline_mape_avg": None, "beats_baseline": False, "mape_h3": None}}
        out["series"][name] = res
        m = res["meta"]
        LOG.info("  -> %s | model=%s | MAPE h3=%s%% avg=%s%% (naive avg=%s%%) | beats=%s",
                 m["confidence"], m["model"], m["mape_h3"], m["backtest_mape_avg"],
                 m["baseline_mape_avg"], m["beats_baseline"])

    path = JSON_DIR / "forecast.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    LOG.info("[forecast] Escrito %s", path)


if __name__ == "__main__":
    main()
