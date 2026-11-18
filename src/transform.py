"""Transform: data/raw/ → data/clean/ (tablas normalizadas).

Obligatorio en la Práctica 1 Big Data (ver enunciado):
  - clave canónica iso3 (+ mapeo iso2↔iso3 vía focus_countries)
  - perfil aplanado → countries
  - explosión de idiomas → languages (tabla larga)
  - indicadores en esquema común → indicators
  - tipado / nulos; join por iso3; informe de cobertura

Para una URL nueva: implementad una función y registradla en TRANSFORMERS
con el mismo nombre que `transformer:` en sources.yaml.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import pandas as pd

from .config import Config, Source, load_config
from .paths import CLEAN, RAW, ensure_dirs

Transformer = Callable[[Source, Config], dict[str, pd.DataFrame]]


def _read_json(path: Path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def transform_countries_profile(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    raw_path = RAW / f"{source.id}.json"
    payload = _read_json(raw_path)
    wanted = {c.iso3 for c in cfg.focus_countries}

    countries_rows = []
    language_rows = []
    for item in payload:
        iso3 = item.get("cca3")
        if iso3 not in wanted:
            continue
        name = item.get("name") or {}
        countries_rows.append(
            {
                "iso3": iso3,
                "iso2": item.get("cca2"),
                "name_common": name.get("common"),
                "name_official": name.get("official"),
                "region": item.get("region"),
                "subregion": item.get("subregion"),
            }
        )
        for code, lang_name in (item.get("languages") or {}).items():
            language_rows.append(
                {
                    "iso3": iso3,
                    "language_code": code,
                    "language_name": lang_name,
                }
            )

    return {
        "countries": pd.DataFrame(countries_rows),
        "languages": pd.DataFrame(language_rows),
    }


def transform_worldbank_indicator(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    rows = []
    meta = source.meta or {}
    country_by_iso3 = {country.iso3: country for country in cfg.focus_countries}
    raw_path = RAW / f"{source.id}.json"
    if raw_path.exists():
        payload = _read_json(raw_path)
        if isinstance(payload, list) and len(payload) > 1 and payload[1]:
            for rec in payload[1]:
                if rec is None:
                    continue
                iso3 = rec.get("countryiso3code")
                country = country_by_iso3.get(iso3)
                if country is None:
                    continue
                rows.append(
                    {
                        "iso3": iso3,
                        "iso2": country.iso2,
                        "indicator_id": meta.get("indicator_id", source.id),
                        "indicator_label": meta.get("indicator_label", (rec.get("indicator") or {}).get("value")),
                        "year": rec.get("date"),
                        "value": rec.get("value"),
                        "unit": meta.get("unit"),
                        "source_id": source.id,
                    }
                )
        return {"indicators": pd.DataFrame(rows)}

    for country in cfg.country_targets:
        path = RAW / f"{source.id}_{country.iso2}.json"
        if not path.exists():
            continue
        payload = _read_json(path)
        if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
            continue
        for rec in payload[1]:
            if rec is None:
                continue
            rows.append(
                {
                    "iso3": country.iso3,
                    "iso2": country.iso2,
                    "indicator_id": meta.get("indicator_id", source.id),
                    "indicator_label": meta.get(
                        "indicator_label",
                        (rec.get("indicator") or {}).get("value"),
                    ),
                    "year": rec.get("date"),
                    "value": rec.get("value"),
                    "unit": meta.get("unit"),
                    "source_id": source.id,
                }
            )
    return {"indicators": pd.DataFrame(rows)}


def transform_owid_life_expectancy(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    raw_path = RAW / f"{source.id}.csv"
    df = pd.read_csv(raw_path)
    meta = source.meta or {}
    year = str(meta["year"]) if "year" in meta else None
    wanted = {c.iso3 for c in cfg.focus_countries}
    # Columnas OWID: Entity, Code, Year, Life expectancy
    code_col = "Code" if "Code" in df.columns else "code"
    year_col = "Year" if "Year" in df.columns else "year"
    value_col = "Life expectancy" if "Life expectancy" in df.columns else df.columns[-1]
    iso2_by_iso3 = {c.iso3: c.iso2 for c in cfg.focus_countries}
    sub = df[df[code_col].isin(wanted)]
    if year is not None:
        sub = sub[sub[year_col].astype(str) == year]
    rows = []
    for _, r in sub.iterrows():
        iso3 = r[code_col]
        rows.append(
            {
                "iso3": iso3,
                "iso2": iso2_by_iso3.get(iso3),
                "indicator_id": meta.get("indicator_id", "life_expectancy"),
                "indicator_label": meta.get("indicator_label", value_col),
                "year": str(r[year_col]),
                "value": r[value_col],
                "unit": meta.get("unit", "years"),
                "source_id": source.id,
            }
        )
    return {"indicators": pd.DataFrame(rows)}


def transform_eurostat_demo_pjan(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    rows = []
    meta = source.meta or {}
    for country in cfg.focus_countries:
        path = RAW / f"{source.id}_{country.iso2}.json"
        if not path.exists():
            continue
        payload = _read_json(path)
        values = payload.get("value") or {}
        if not values:
            # País fuera de cobertura Eurostat: huérfano documentado vía ausencia
            continue
        # Con una sola celda filtrada, la clave suele ser "0"
        value = next(iter(values.values()))
        rows.append(
            {
                "iso3": country.iso3,
                "iso2": country.iso2,
                "indicator_id": meta.get("indicator_id", "demo_pjan"),
                "indicator_label": meta.get(
                    "indicator_label", payload.get("label")
                ),
                "year": meta.get("time"),
                "value": value,
                "unit": meta.get("unit", "persons"),
                "source_id": source.id,
            }
        )
    return {"indicators": pd.DataFrame(rows)}


def transform_generic_passthrough(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    """Plantilla: lee raw y deja un DataFrame mínimo para que lo adapteis."""
    paths = sorted(RAW.glob(f"{source.id}*"))
    rows = []
    for path in paths:
        rows.append(
            {
                "source_id": source.id,
                "filename": path.name,
                "bytes": path.stat().st_size,
                "note": "Reemplazad este transformer por uno específico",
            }
        )
    return {"custom_raw_index": pd.DataFrame(rows)}


TRANSFORMERS: dict[str, Transformer] = {
    "countries_profile": transform_countries_profile,
    "worldbank_indicator": transform_worldbank_indicator,
    "eurostat_demo_pjan": transform_eurostat_demo_pjan,
    "owid_life_expectancy": transform_owid_life_expectancy,
    "generic_passthrough": transform_generic_passthrough,
}


def run_transform(cfg: Config | None = None) -> dict[str, pd.DataFrame]:
    cfg = cfg or load_config()
    ensure_dirs()
    buckets: dict[str, list[pd.DataFrame]] = {}

    for source in cfg.sources:
        name = source.transformer
        if name not in TRANSFORMERS:
            raise KeyError(
                f"Transformer '{name}' no registrado. "
                f"Disponibles: {sorted(TRANSFORMERS)}"
            )
        print(f"[transform] {source.id} → {name}")
        out = TRANSFORMERS[name](source, cfg)
        for table, df in out.items():
            buckets.setdefault(table, []).append(df)

    merged: dict[str, pd.DataFrame] = {}
    for table, parts in buckets.items():
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        if table == "indicators" and "year" in df:
            df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
        dest = CLEAN / f"{table}.csv"
        df.to_csv(dest, index=False)
        print(f"[transform] {table}: {len(df)} filas → {dest}")
        merged[table] = df

    if "countries" in merged:
        profile = set(merged["countries"]["iso3"].dropna())
        indicators = merged.get("indicators", pd.DataFrame(columns=["iso3", "source_id", "indicator_id"]))
        have = set(indicators[["iso3", "source_id", "indicator_id"]].dropna().itertuples(index=False, name=None))
        orphans = [
            {"iso3": iso3, "source_id": source.id, "indicator_id": (source.meta or {}).get("indicator_id", source.id)}
            for source in cfg.sources if source.role == "indicator" and source.transformer not in ("generic_passthrough", "countries_profile")
            for iso3 in profile
            if (iso3, source.id, (source.meta or {}).get("indicator_id", source.id)) not in have
        ]
        report = pd.DataFrame(orphans, columns=["iso3", "source_id", "indicator_id"])
        report_path = CLEAN / "join_orphans.csv"
        report.to_csv(report_path, index=False)
        print(f"[transform] huérfanos del join: {len(orphans)} → {report_path}")

    return merged


if __name__ == "__main__":
    run_transform()
