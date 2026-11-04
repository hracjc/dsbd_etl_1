"""Extract: descarga intacta → data/raw/ + manifiesto.

No transforma. Añadir una URL nueva = entrada en sources.yaml.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import requests

from .config import Config, Source, load_config
from .paths import EXTRACT_STATE_PATH, MANIFEST, RAW, ensure_dirs

USER_AGENT = "UCJC-DSBD-practica1/1.2 (educational ETL framework)"
TIMEOUT = 60
FIELDNAMES = [
    "timestamp_utc",
    "source_id",
    "url",
    "http_status",
    "bytes",
    "filename",
    "iso2",
    "iso3",
    "status",
    "note",
]


def _raw_name(source: Source, iso2: str | None = None) -> str:
    if source.url_template and iso2:
        return f"{source.id}_{iso2}.{source.format}"
    return f"{source.id}.{source.format}"


def _state_key(row: dict) -> tuple[str, str, str]:
    return row["source_id"], row["url"], row["filename"]


def _load_state() -> dict[tuple[str, str, str], dict]:
    latest = {}
    if not EXTRACT_STATE_PATH.exists():
        return latest

    with EXTRACT_STATE_PATH.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        missing = set(FIELDNAMES) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"Estado de extracción inválido en {EXTRACT_STATE_PATH}: "
                f"faltan columnas {sorted(missing)}"
            )
        for row in reader:
            latest[_state_key(row)] = row
    return latest


def _record_state(row: dict) -> None:
    write_header = not EXTRACT_STATE_PATH.exists() or EXTRACT_STATE_PATH.stat().st_size == 0
    with EXTRACT_STATE_PATH.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def extract_source(
    source: Source,
    cfg: Config,
    state: dict[tuple[str, str, str], dict] | None = None,
    force: bool = False,
) -> list[dict]:
    """Descarga solo peticiones sin resultado exitoso previo."""
    rows: list[dict] = []
    latest = state if state is not None else _load_state()

    if source.url_template:
        targets = cfg.focus_countries
    else:
        targets = [None]

    for country in targets:
        iso2 = country.iso2 if country else ""
        iso3 = country.iso3 if country else ""
        url = source.resolve_url(iso2=iso2, iso3=iso3)
        filename = _raw_name(source, iso2 or None)
        dest = RAW / filename
        request = {
            "source_id": source.id,
            "url": url,
            "filename": filename,
        }
        previous = latest.get(_state_key(request))
        if not force and (
            previous
            and previous["status"] == "success"
            and (RAW / previous["filename"]).is_file()
        ):
            cached = dict(previous)
            cached["note"] = "Skipped: successful result already exists"
            rows.append(cached)
            print(f"[extract] {source.id} {iso3 or '-'}: cached")
            continue

        now = datetime.now(timezone.utc).isoformat()
        status = 0
        nbytes = 0
        result = "failure"
        note = ""
        try:
            response = requests.get(
                url,
                timeout=TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )
            status = response.status_code
            nbytes = len(response.content)
            if status >= 400:
                raise requests.HTTPError(f"HTTP {status}", response=response)
            dest.write_bytes(response.content)
            result = "success"
        except requests.RequestException as exc:
            if exc.response is not None:
                status = exc.response.status_code
                nbytes = len(exc.response.content)
            note = str(exc)
        except OSError as exc:
            note = str(exc)

        row = {
            "timestamp_utc": now,
            **request,
            "http_status": status,
            "bytes": nbytes,
            "iso2": iso2,
            "iso3": iso3,
            "status": result,
            "note": note,
        }
        _record_state(row)
        latest[_state_key(row)] = row
        rows.append(row)
        print(f"[extract] {source.id} {iso3 or '-'}: {result} ({status})")
    return rows


def run_extract(cfg: Config | None = None, force: bool = False) -> Path:
    cfg = cfg or load_config()
    ensure_dirs()
    all_rows: list[dict] = []
    state = _load_state()
    for source in cfg.sources:
        print(f"[extract] {source.id} — {source.name}")
        all_rows.extend(extract_source(source, cfg, state, force=force))

    with MANIFEST.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"[extract] manifiesto → {MANIFEST}")
    print(f"[extract] estado → {EXTRACT_STATE_PATH}")
    return MANIFEST


if __name__ == "__main__":
    run_extract()
