#!/usr/bin/env python3
"""
Simula execução diária para mostrar renovação automática de tokens.
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

def simulate_daily_execution(client_id: str):
    """Simula execução diária às 2h da manhã."""
    print(f"🌅 Simulando execução diária para {client_id}")
    
    # Carregar variáveis de ambiente
    _load_envs(client_id)
    
    # Verificar cache de tokens
    cache_path = Path(f".secrets/bling_tokens_{client_id}.json")
    if not cache_path.exists():
        print("❌ Cache de tokens não encontrado")
        return
    
    try:
        tokens = json.loads(cache_path.read_text(encoding="utf-8"))
        
        # Informações do token
        access_token = tokens.get("access_token")
        refresh_token = tokens.get("refresh_token")
        expires_at = tokens.get("expires_at")
        now = int(time.time())
        
        print(f"📋 Status do token:")
        print(f"  - Access Token: {'✅ Presente' if access_token else '❌ Ausente'}")
        print(f"  - Refresh Token: {'✅ Presente' if refresh_token else '❌ Ausente'}")
        print(f"  - Expira em: {expires_at}")
        print(f"  - Agora: {now}")
        
        if expires_at:
            remaining = expires_at - now
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            
            if remaining > 0:
                print(f"  - Tempo restante: {hours}h {minutes}m ({remaining} segundos)")
                
                if remaining < 300:  # Menos de 5 minutos
                    print(f"⚠️  Token vai expirar em breve! Será renovado automaticamente.")
                else:
                    print(f"✅ Token válido para esta execução.")
            else:
                print(f"❌ Token expirado! Será renovado automaticamente.")
        
        # Simular chamadas da API (versão simplificada)
        print(f"\n🌐 Simulando chamadas da API...")
        
        # Importar o adapter
        from core.adapters.api_adapter import APISourceAdapter
        
        # Configuração OAuth2
        auth_config = {
            "kind": "oauth2_generic",
            "token_cache": f".secrets/bling_tokens_{client_id}.json",
            "token_url_env": "OAUTH_TOKEN_URL",
            "client_id_env": "OAUTH_CLIENT_ID",
            "client_secret_env": "OAUTH_CLIENT_SECRET",
            "redirect_uri_env": "OAUTH_REDIRECT_URI",
            "scope_env": "OAUTH_SCOPE",
            "use_basic_auth": True
        }
        
        # Criar adapter com configuração que NÃO faz paginação infinita
        api = APISourceAdapter(
            endpoint="https://www.bling.com.br/Api/v3/produtos",
            auth=auth_config,
            paging={
                "mode": "page",
                "page_param": "pagina",
                "size_param": "limite",
                "size": 1,  # Apenas 1 produto por página
                "start_page": 1
            },
            default_headers={
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
        )
        
        # Simular apenas 1 chamada (sem loop infinito)
        print(f"  📡 Chamada única: ", end="")
        try:
            df = api.extract()
            print(f"✅ Sucesso - {len(df)} produtos")
            
            # Mostrar informações do primeiro produto (se houver)
            if len(df) > 0:
                first_product = df.iloc[0]
                print(f"     📦 Primeiro produto: {first_product.get('nome', 'N/A')}")
            
        except Exception as e:
            print(f"❌ Erro: {e}")
        
        # Verificar se tokens foram renovados
        print(f"\n🔄 Verificando se tokens foram renovados...")
        try:
            new_tokens = json.loads(cache_path.read_text(encoding="utf-8"))
            new_expires_at = new_tokens.get("expires_at")
            
            if new_expires_at != expires_at:
                new_remaining = new_expires_at - now
                new_hours = new_remaining // 3600
                new_minutes = (new_remaining % 3600) // 60
                print(f"✅ Tokens renovados! Novo tempo restante: {new_hours}h {new_minutes}m")
            else:
                print(f"ℹ️  Tokens não foram renovados (ainda válidos)")
                
        except Exception as e:
            print(f"❌ Erro ao verificar renovação: {e}")
        
        print(f"\n🎉 Simulação concluída!")
        print(f"💡 O sistema funciona automaticamente - você não precisa se preocupar com renovação de tokens!")
        
    except Exception as e:
        print(f"❌ Erro na simulação: {e}")

def main():
    if len(sys.argv) != 2:
        print("Uso: python -m core.tests.simulate_daily_execution CLIENT_ID")
        print("Exemplo: python -m core.tests.simulate_daily_execution LOJAJUNTOS")
        sys.exit(1)
    
    client_id = sys.argv[1].strip()
    simulate_daily_execution(client_id)

if __name__ == "__main__":
    main()
