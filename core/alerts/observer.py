from __future__ import annotations
import traceback
from datetime import datetime
from typing import Dict, Any
from .telegram import TelegramAlert

class PipelineObserver:
    """
    Centraliza a lógica de notificação do DataMat. 
    Lida com eventos de ETL (Pipelines) e eventos de Infraestrutura (Sistema)
    usando exclusivamente o Telegram.
    """
    def __init__(self):
        # Instancia o 'carteiro' do Telegram uma única vez
        self.telegram = TelegramAlert()

    def notify_failure(self, tenant: str, job_name: str, error: Exception, tb: str = None):
        """Notifica uma falha crítica de pipeline via Telegram com detalhes técnicos."""
        if not tb:
            tb = traceback.format_exc()

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_type = error.__class__.__name__
        error_msg = str(error)

        # TELEGRAM (Mensagem Completa com Traceback)
        # Como a classe TelegramAlert fatia mensagens grandes, podemos mandar o Traceback por aqui!
        tg_msg = (
            f"🚨 *FALHA CRÍTICA: {tenant}*\n"
            f"*Job:* `{job_name}`\n"
            f"*Erro:* `{error_type}`\n"
            f"_{error_msg}_\n"
            f"🕒 {timestamp}\n\n"
            f"*Traceback Técnico:*\n"
            f"```python\n{tb}\n```"
        )
        
        # Envia usando parse_mode Markdown para a formatação de código (```) funcionar
        self.telegram.send(tg_msg, parse_mode="Markdown")

    def notify_success(self, tenant: str, stats: Dict[str, Any]):
        """Notifica sucesso do ETL."""
        msg = f"✅ *{tenant}*: Pipeline concluído com sucesso.\n"
        for k, v in stats.items():
            msg += f"- {k}: {v}\n"
        
        self.telegram.send(msg, parse_mode="Markdown")

    def notify_system_event(self, subject: str, message: str, is_error: bool = False, parse_mode: str = "HTML"):
        """
        Notifica eventos de infraestrutura (Ex: Backup, Limpeza de Logs, etc) via Telegram.
        """
        # Envia diretamente pelo Telegram (não há mais lógica paralela de e-mail)
        self.telegram.send(message, parse_mode=parse_mode)

# Instância Singleton para uso fácil no projeto inteiro
observer = PipelineObserver()
