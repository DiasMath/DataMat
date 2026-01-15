from __future__ import annotations
import traceback
from datetime import datetime
from typing import Dict, Any, Optional

# Importa os drivers (carteiros) que acabamos de refatorar
from .email import send_email
from .telegram import send_telegram_text

class PipelineObserver:
    """
    Centraliza a lógica de notificação. 
    Recebe eventos do pipeline e formata as mensagens adequadas para cada canal (Email/Telegram).
    """

    def notify_failure(self, tenant: str, job_name: str, error: Exception, tb: str = None):
        """Notifica uma falha crítica via Telegram (rápido) e Email (detalhado)."""
        
        if not tb:
            tb = traceback.format_exc()

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_type = error.__class__.__name__
        error_msg = str(error)

        # 1. TELEGRAM (Mensagem Curta e Urgente)
        tg_msg = (
            f"🚨 *FALHA CRÍTICA: {tenant}*\n"
            f"*Job:* `{job_name}`\n"
            f"*Erro:* `{error_type}`\n"
            f"_{error_msg}_\n"
            f"🕒 {timestamp}"
        )
        send_telegram_text(tg_msg, parse_mode="Markdown")

        # 2. EMAIL (Relatório Completo com Traceback)
        subject = f"[FALHA ETL] {tenant} - {job_name}"
        
        body_txt = (
            f"FALHA NO PIPELINE\n"
            f"Tenant: {tenant}\n"
            f"Job: {job_name}\n"
            f"Data: {timestamp}\n\n"
            f"ERRO: {error_type}\n"
            f"Mensagem: {error_msg}\n\n"
            f"TRACEBACK:\n{tb}"
        )
        
        body_html = f"""
        <h2 style="color: #e74c3c;">🚨 Falha no Pipeline: {tenant}</h2>
        <p><strong>Job:</strong> {job_name}<br>
           <strong>Data:</strong> {timestamp}<br>
           <strong>Erro:</strong> {error_type}</p>
        <div style="background-color: #f8d7da; padding: 10px; border-radius: 5px; color: #721c24;">
            {error_msg}
        </div>
        <h3>Detalhes Técnicos (Traceback)</h3>
        <pre style="background-color: #f8f9fa; padding: 10px; border: 1px solid #ddd; overflow-x: auto;">
{tb}
        </pre>
        """
        send_email(subject, body_txt, body_html)

    def notify_success(self, tenant: str, stats: Dict[str, Any]):
        """Notifica sucesso (Geralmente só log ou Telegram resumo, para não spammar email)."""
        # Exemplo simples para Telegram
        msg = f"✅ *{tenant}*: Pipeline concluído com sucesso.\n"
        for k, v in stats.items():
            msg += f"- {k}: {v}\n"
        
        send_telegram_text(msg, parse_mode="Markdown")

# Instância Singleton para uso fácil
observer = PipelineObserver()