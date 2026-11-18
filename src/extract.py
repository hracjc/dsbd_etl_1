"""Extract: descarga intacta → data/raw/ + manifiesto.

No transforma. Añadir una URL nueva = entrada en sources.yaml.
"""

from __future__ import annotations

import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import requests

from .config import Config, Source, load_config
from .paths import EXTRACT_STATE_PATH, MANIFEST, RAW, ensure_dirs

USER_AGENT = "hracjc-UCJC-DSBD-practica1/1.2"
TIMEOUT = 60
REQUEST_INTERVAL = 0.2
_last_request_at = 0.0
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


def _request(url: str):
    global _last_request_at
    wait = REQUEST_INTERVAL - (time.monotonic() - _last_request_at)
    if wait > 0:
        time.sleep(wait)
    _last_request_at = time.monotonic()
    return requests.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})


def _download(url: str, dest: Path) -> tuple[int, int]:
    response = _request(url)
    status = response.status_code
    nbytes = len(response.content)
    if status != 200:
        return status, nbytes

    content = response.content
    if "api.worldbank.org" in url:
        payload = json.loads(content)
        if isinstance(payload, list) and isinstance(payload[0], dict) and isinstance(payload[1], list):
            pages = int(payload[0].get("pages", 1))
            records = payload[1]
            for page in range(2, pages + 1):
                page_url = f"{url}&page={page}" if "?" in url else f"{url}?page={page}"
                page_response = _request(page_url)
                if page_response.status_code != 200:
                    return page_response.status_code, len(page_response.content)
                page_payload = json.loads(page_response.content)
                records.extend(page_payload[1])
            if len(records) != int(payload[0]["total"]):
                raise ValueError("World Bank record count does not match total")
            content = json.dumps(payload).encode("utf-8")
            nbytes = len(content)

    dest.write_bytes(content)
    return status, nbytes


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
        targets = cfg.country_targets
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
        if not force and previous and (previous["http_status"] == "404" or previous["status"] == "success" and (RAW / previous["filename"]).is_file()):
            cached = dict(previous)
            if previous["http_status"] == "404":
                cached["note"] = "HTTP 404; not retrying"
            else:
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
            status, nbytes = _download(url, dest)
            if status == 200:
                result = "success"
            elif status == 404:
                note = "HTTP 404; not retrying"
            else:
                note = f"HTTP {status}"
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
    run_id = uuid4().hex
    all_rows: list[dict] = []
    state = _load_state()
    for source in cfg.sources:
        print(f"[extract] {source.id} — {source.name}")
        all_rows.extend(extract_source(source, cfg, state, force=force))

    manifest_fields = FIELDNAMES.copy()
    manifest_fields.append("run_id")
    with MANIFEST.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=manifest_fields)
        writer.writeheader()
        writer.writerows({"run_id": run_id, **row} for row in all_rows)

    print(f"[extract] manifiesto → {MANIFEST}")
    print(f"[extract] estado → {EXTRACT_STATE_PATH}")
    return MANIFEST


if __name__ == "__main__":
    run_extract()
