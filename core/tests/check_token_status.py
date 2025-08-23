#!/usr/bin/env python3
"""
Verifica status dos tokens OAuth2 do Bling.
"""
from __future__ import annotations
import os, sys, json, time
from pathlib import Path

def _load_envs(client_id: str):
    """Carrega .env global e do tenant."""
    try:
        from dotenv import load_dotenv
        load_dotenv(".env", override=True)
        load_dotenv(Path("tenants")/client_id/"config"/".env", override=True)
    except Exception as e:
        print(f"Aviso: erro ao carregar .env: {e}")

def check_token_status(client_id: str):
    """Verifica status dos tokens OAuth2."""
    print(f"🔍 Verificando status dos tokens para {client_id}")
    
    # Carregar variáveis de ambiente
    _load_envs(client_id)
    
    # Verificar cache de tokens
    cache_path = Path(f".secrets/bling_tokens_{client_id}.json")
    if not cache_path.exists():
        print("❌ Cache de tokens não encontrado")
        print("Execute: python scripts/oauth2_setup.py {client_id}")
        return
    
    try:
        tokens = json.loads(cache_path.read_text(encoding="utf-8"))
        
        # Informações do token
        access_token = tokens.get("access_token")
        refresh_token = tokens.get("refresh_token")
        expires_at = tokens.get("expires_at")
        now = int(time.time())
        
        print(f"📋 Status dos tokens:")
        print(f"  - Access Token: {'✅ Presente' if access_token else '❌ Ausente'}")
        print(f"  - Refresh Token: {'✅ Presente' if refresh_token else '❌ Ausente'}")
        
        if expires_at:
            remaining = expires_at - now
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            days = hours // 24
            
            if remaining > 0:
                print(f"  - Tempo restante: {days}d {hours%24}h {minutes}m ({remaining} segundos)")
                
                if remaining < 300:  # Menos de 5 minutos
                    print(f"⚠️  Token vai expirar em breve! Será renovado automaticamente.")
                elif remaining < 3600:  # Menos de 1 hora
                    print(f"⚠️  Token vai expirar em menos de 1 hora.")
                else:
                    print(f"✅ Token válido para execução diária.")
            else:
                print(f"❌ Token expirado! Será renovado automaticamente na próxima execução.")
        
        # Verificar configuração OAuth2
        print(f"\n⚙️  Configuração OAuth2:")
        required_vars = [
            "OAUTH_AUTH_URL",
            "OAUTH_TOKEN_URL", 
            "OAUTH_CLIENT_ID",
            "OAUTH_CLIENT_SECRET",
            "OAUTH_REDIRECT_URI"
        ]
        
        all_configured = True
        for var in required_vars:
            value = os.getenv(var, "").strip()
            if value:
                print(f"  ✅ {var}: {'*' * len(value)} (configurado)")
            else:
                print(f"  ❌ {var}: Não configurado")
                all_configured = False
        
        if all_configured:
            print(f"\n🎉 Configuração completa!")
            print(f"💡 O sistema está pronto para execução diária às 2h da manhã.")
            print(f"🔄 Tokens serão renovados automaticamente quando necessário.")
        else:
            print(f"\n❌ Configuração incompleta!")
            print(f"Configure as variáveis faltando no arquivo .env do tenant.")
        
    except Exception as e:
        print(f"❌ Erro ao verificar tokens: {e}")

def main():
    if len(sys.argv) != 2:
        print("Uso: python -m core.tests.check_token_status CLIENT_ID")
        print("Exemplo: python -m core.tests.check_token_status LOJAJUNTOS")
        sys.exit(1)
    
    client_id = sys.argv[1].strip()
    check_token_status(client_id)

if __name__ == "__main__":
    main()
