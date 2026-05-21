#!/usr/bin/env python3
"""
Completa el asistente inicial de Metabase (OSS) si aún no está hecho.
Idempotente: si ya hay instancia configurada, no hace nada.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("METABASE_URL", "http://metabase:3000").rstrip("/")
EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@blackfriday.local")
PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "BlackFridayLab1")
FIRST = os.environ.get("METABASE_ADMIN_FIRST_NAME", "Admin")
LAST = os.environ.get("METABASE_ADMIN_LAST_NAME", "Lab")
SITE = os.environ.get("METABASE_SITE_NAME", "Black Friday Lab")


def http_json(method: str, path: str, payload: dict | None = None) -> tuple[int, dict | list | str]:
    url = f"{BASE}{path}"
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            body = r.read().decode("utf-8", errors="replace")
            if not body.strip():
                return r.status, {}
            try:
                return r.status, json.loads(body)
            except json.JSONDecodeError:
                return r.status, body
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(err_body)
        except json.JSONDecodeError:
            parsed = err_body
        print(f"HTTP {e.code}: {parsed}", file=sys.stderr)
        raise SystemExit(1) from e


def wait_ready(max_wait_s: int = 240) -> None:
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"{BASE}/api/health")
            with urllib.request.urlopen(req, timeout=5) as r:
                if r.status == 200:
                    return
        except Exception:
            pass
        time.sleep(3)
    print("Metabase no respondió a /api/health a tiempo.", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    wait_ready()
    status, props = http_json("GET", "/api/session/properties")
    if status != 200 or not isinstance(props, dict):
        print("Respuesta inesperada de session/properties", file=sys.stderr)
        raise SystemExit(1)

    token = props.get("setup-token") or props.get("setup_token")
    if not token:
        print("Metabase ya configurado (no hay setup-token). OK.")
        return

    payload = {
        "token": token,
        "user": {
            "first_name": FIRST,
            "last_name": LAST,
            "email": EMAIL,
            "password": PASSWORD,
        },
        "prefs": {"site_name": SITE, "allow_tracking": False},
    }
    status, _ = http_json("POST", "/api/setup", payload)
    if status not in (200, 201):
        print(f"Setup inesperado: HTTP {status}", file=sys.stderr)
        raise SystemExit(1)
    print(f"Metabase: usuario admin creado ({EMAIL}).")


if __name__ == "__main__":
    main()
