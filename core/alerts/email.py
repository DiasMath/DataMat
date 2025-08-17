from __future__ import annotations
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional, List

def _get_env() -> dict:
    cfg = {
        "host": os.getenv("SMTP_HOST", "").strip(),
        "port": int(os.getenv("SMTP_PORT", "587")),
        "user": os.getenv("SMTP_USER", "").strip(),
        "password": os.getenv("SMTP_PASS", "").strip(),
        "from_addr": os.getenv("SMTP_FROM", "").strip() or os.getenv("SMTP_USER", "").strip(),
        "to_addrs": [x.strip() for x in os.getenv("SMTP_TO", "").split(",") if x.strip()],
        "use_tls": os.getenv("SMTP_TLS", "1").strip() in ("1", "true", "on", "yes", "y"),
    }
    # Campos mínimos
    if not cfg["host"] or not cfg["from_addr"] or not cfg["to_addrs"]:
        raise RuntimeError("Configure SMTP_HOST, SMTP_FROM, SMTP_TO (e usualmente SMTP_USER/SMTP_PASS/SMTP_TLS) no .env")
    return cfg

def send_email(subject: str, body_text: str, body_html: Optional[str] = None) -> None:
    """
    Envia e-mail via SMTP (STARTTLS por padrão).
    Requer .env: SMTP_HOST, SMTP_PORT(587), SMTP_USER, SMTP_PASS, SMTP_FROM, SMTP_TO, SMTP_TLS(1|0)
    """
    cfg = _get_env()

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg["from_addr"]
    msg["To"] = ", ".join(cfg["to_addrs"])

    part_text = MIMEText(body_text or "", "plain", "utf-8")
    msg.attach(part_text)
    if body_html:
        part_html = MIMEText(body_html, "html", "utf-8")
        msg.attach(part_html)

    context = ssl.create_default_context()
    with smtplib.SMTP(cfg["host"], cfg["port"], timeout=30) as server:
        if cfg["use_tls"]:
            server.starttls(context=context)
        if cfg["user"]:
            # Observação: para Microsoft 365/Outlook corporativo, prefira App Password
            # ou SMTP AUTH habilitado pela TI.
            server.login(cfg["user"], cfg["password"])
        server.sendmail(cfg["from_addr"], cfg["to_addrs"], msg.as_string())
