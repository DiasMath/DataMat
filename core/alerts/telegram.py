# core/alerts/telegram.py
from __future__ import annotations
import os
import time
import logging
import requests
from typing import Optional, List

# Configura logger local
log = logging.getLogger("alerts.telegram")

TELEGRAM_API = "https://api.telegram.org"
MAX_MESSAGE_LENGTH = 4000  # Limite seguro (O oficial é 4096)

def _split_message(message: str) -> List[str]:
    """
    Divide a mensagem em blocos de até MAX_MESSAGE_LENGTH caracteres.
    Tenta cortar sempre na quebra de linha para não estragar a formatação.
    """
    if len(message) <= MAX_MESSAGE_LENGTH:
        return [message]

    parts = []
    while message:
        if len(message) <= MAX_MESSAGE_LENGTH:
            parts.append(message)
            break

        # Tenta achar a última quebra de linha dentro do limite seguro
        cut_point = message.rfind('\n', 0, MAX_MESSAGE_LENGTH)

        # Se não tiver quebra de linha (texto maciço), corta na força bruta
        if cut_point == -1:
            cut_point = MAX_MESSAGE_LENGTH

        # Adiciona o pedaço e avança
        parts.append(message[:cut_point])
        message = message[cut_point:].lstrip() # Remove quebra de linha sobrando no inicio
    
    return parts


class TelegramAlert:
    """
    Classe para envio de alertas via Telegram.
    Permite instanciar múltiplos bots ou chats diferentes no mesmo projeto.
    """
    def __init__(self, token: Optional[str] = None, chat_id: Optional[str] = None):
        # Se não passar os parâmetros, ele busca no .env como fallback
        self.token = token or os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID", "").strip()

    def _send_single_chunk(self, text: str, parse_mode: Optional[str], disable_preview: bool) -> bool:
        """Envia um único pedaço de texto."""
        url = f"{TELEGRAM_API}/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": disable_preview,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode

        last_err = None
        # Retry logic (3x)
        for i in range(3):
            try:
                r = requests.post(url, json=payload, timeout=15)
                if r.ok:
                    return True
                
                # Se for erro 429 (Too Many Requests), espera o tempo que o Telegram mandar
                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", 5))
                    time.sleep(wait)
                    continue
                    
                last_err = f"HTTP {r.status_code}: {r.text}"
                
            except Exception as e:
                last_err = str(e)
            
            time.sleep(0.5 * (2 ** i)) # Backoff: 0.5s, 1s, 2s

        log.error(f"Falha ao enviar chunk Telegram: {last_err}")
        return False

    def send(self, message: str, parse_mode: Optional[str] = "HTML", disable_web_page_preview: bool = True) -> bool:
        """
        Método principal de envio compatível com o padrão Observer.
        Verifica tamanho, fatia se necessário e envia os pedaços em ordem.
        """
        if not self.token or not self.chat_id:
            log.warning("Telegram ignorado: Variáveis de ambiente não configuradas.")
            return False

        # 1. Fatia a mensagem se for grande
        chunks = _split_message(message)
        
        # 2. Envia pedaço por pedaço
        all_sent = True
        for i, chunk in enumerate(chunks):
            # Se foi fatiado, adiciona um indicador visual (ex: [1/3]) para saber a ordem
            if len(chunks) > 1:
                footer = f"\n\n[Parte {i+1}/{len(chunks)}]"
                if len(chunk) + len(footer) > MAX_MESSAGE_LENGTH:
                    pass 
                else:
                    chunk += footer

            success = self._send_single_chunk(chunk, parse_mode, disable_web_page_preview)
            if not success:
                all_sent = False
        
        return all_sent


# =============================================================================
# FUNÇÃO LEGACY (RETROCOMPATIBILIDADE)
# =============================================================================
def send_telegram_text(message: str, *, parse_mode: Optional[str] = None, disable_web_page_preview: bool = True) -> bool:
    """
    Mantido para compatibilidade com scripts antigos do DataMat.
    Ele instancia a classe nova por baixo dos panos.
    """
    bot = TelegramAlert()
    return bot.send(message, parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview)
