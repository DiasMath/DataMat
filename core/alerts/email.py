# core/alerts/email.py
from __future__ import annotations
import os
import smtplib
import ssl
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional, List

log = logging.getLogger(__name__)

def _get_env() -> dict:
    return {
        "host": os.getenv("SMTP_HOST", "").strip(),
        "port": int(os.getenv("SMTP_PORT", "587")),
        "user": os.getenv("SMTP_USER", "").strip(),
        "password": os.getenv("SMTP_PASS", "").strip(),
        "from_addr": os.getenv("SMTP_FROM", "").strip() or os.getenv("SMTP_USER", "").strip(),
        "to_addrs": [x.strip() for x in os.getenv("SMTP_TO", "").split(",") if x.strip()],
        "use_tls": os.getenv("SMTP_TLS", "1").strip() in ("1", "true", "on", "yes", "y"),
    }

def send_email(subject: str, body_text: str, body_html: Optional[str] = None) -> bool:
    """
    Envia e-mail via SMTP. Retorna True se sucesso, False se falha.
    Blindado contra exceções para não derrubar o pipeline.
    """
    try:
        cfg = _get_env()
        
        # Validação mínima silenciosa
        if not cfg["host"] or not cfg["to_addrs"]:
            log.warning("SMTP não configurado completamente. Pulei o envio de e-mail.")
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = cfg["from_addr"]
        msg["To"] = ", ".join(cfg["to_addrs"])

        msg.attach(MIMEText(body_text or "", "plain", "utf-8"))
        if body_html:
            msg.attach(MIMEText(body_html, "html", "utf-8"))

        context = ssl.create_default_context()
        
        with smtplib.SMTP(cfg["host"], cfg["port"], timeout=30) as server:
            if cfg["use_tls"]:
                server.starttls(context=context)
            
            if cfg["user"] and cfg["password"]:
                server.login(cfg["user"], cfg["password"])
                
            server.sendmail(cfg["from_addr"], cfg["to_addrs"], msg.as_string())
            
        return True

    except Exception as e:
        log.error(f"Falha ao enviar e-mail (SMTP): {e}")
        return False