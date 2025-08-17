# core/env.py
from __future__ import annotations
from pathlib import Path
import os

def _load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(path, override=True)
    except Exception:
        # fallback bem simples
        for raw in path.read_text(encoding="utf-8").splitlines():
            s = raw.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

def load_project_and_tenant_env(client_id: str | None) -> None:
    """
    1) Carrega ./.env (global do projeto)
    2) Se client_id for informado: tenants/<client_id>/config/.env (sobrescreve o global)
    """
    root = Path(__file__).resolve().parents[1]  # raiz do repo
    _load_dotenv_file(root / ".env")

    if client_id:
        _load_dotenv_file(root / "tenants" / client_id / "config" / ".env")
