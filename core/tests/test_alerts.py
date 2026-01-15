# core/tests/test_alerts.py
from __future__ import annotations
import os
import logging
from .test_utils import load_test_envs
from core.alerts import observer

def test_alerts_integration(client_id: str) -> bool:
    """
    Testa a integração com os canais de alerta (Telegram/Email).
    Dispara mensagens reais para validar credenciais e conectividade.
    """
    print(f"\n🔔 Testando Sistema de Alertas para: {client_id}")
    
    # 1. Carrega variáveis (precisa do TELEGRAM_TOKEN, CHAT_ID, etc)
    load_test_envs(client_id)
    
    # Validação prévia básica
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not tg_token:
        print("⚠️  TELEGRAM_BOT_TOKEN não encontrado no .env. O teste pode falhar.")
    
    print("   -> Disparando notificação de SUCESSO (Teste)...")
    try:
        # Simula um report de sucesso
        stats = {
            "Linhas Extraídas": 150,
            "Linhas Inseridas": 150,
            "Tempo Total": "00:00:02"
        }
        observer.notify_success(
            tenant=client_id, 
            stats=stats
        )
        print("      [OK] Comando de sucesso enviado.")
    except Exception as e:
        print(f"      [X] Erro ao enviar sucesso: {e}")
        return False

    print("   -> Disparando notificação de FALHA (Teste)...")
    try:
        # Força um erro real para testar a formatação do Traceback
        print("      (Simulando uma divisão por zero intencional...)")
        x = 1 / 0
    except ZeroDivisionError as e:
        try:
            observer.notify_failure(
                tenant=client_id, 
                job_name="TESTE_INTEGRACAO_ALERTS", 
                error=e
            )
            print("      [OK] Comando de falha enviado.")
        except Exception as send_err:
            print(f"      [X] Erro ao enviar falha: {send_err}")
            return False

    print("\n✅ Teste de envio concluído.")
    print("   VERIFIQUE SEU CELULAR/EMAIL AGORA.")
    return True