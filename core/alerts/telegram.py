from __future__ import annotations
import os
import time
import logging
from typing import Optional
import requests

log = logging.getLogger("alerts.telegram")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(name)s - %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

TELEGRAM_API = "https://api.telegram.org"

def _get_env() -> tuple[str, str]:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError("Configure TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID no .env")
    return token, chat_id

def send_telegram_text(message: str, *, parse_mode: Optional[str] = None, disable_web_page_preview: bool = True) -> None:
    token, chat_id = _get_env()
    url = f"{TELEGRAM_API}/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    last_err = None
    for i in range(3):
        try:
            r = requests.post(url, json=payload, timeout=15)
            if r.ok:
                return
            last_err = RuntimeError(f"Telegram HTTP {r.status_code}: {r.text}")
        except Exception as e:
            last_err = e
        time.sleep(0.5 * (2 ** i))
    log.error("Falha ao enviar alerta no Telegram: %s", last_err)
