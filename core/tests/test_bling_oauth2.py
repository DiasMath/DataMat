#!/usr/bin/env python3
"""
Teste para validar configuração OAuth2 do Bling.
Uso: python -m core.tests.test_bling_oauth2 LOJAJUNTOS
"""
from __future__ import annotations
import os, sys, json
from pathlib import Path

def _load_envs(client_id: str):
    """Carrega .env global e do tenant."""
    try:
        from dotenv import load_dotenv
        load_dotenv(".env", override=True)
        load_dotenv(Path("tenants")/client_id/"config"/".env", override=True)
    except Exception as e:
        print(f"Aviso: erro ao carregar .env: {e}")

def test_oauth2_config(client_id: str) -> bool:
    """Testa se a configuração OAuth2 está completa."""
    print(f"🧪 Testando configuração OAuth2 para {client_id}")
    
    # Carregar variáveis de ambiente
    _load_envs(client_id)
    
    # Verificar variáveis obrigatórias
    required_vars = [
        "OAUTH_AUTH_URL",
        "OAUTH_TOKEN_URL", 
        "OAUTH_CLIENT_ID",
        "OAUTH_CLIENT_SECRET",
        "OAUTH_REDIRECT_URI"
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var, "").strip()
        if not value:
            missing_vars.append(var)
        else:
            print(f"✅ {var}: {'*' * len(value)} (configurado)")
    
    if missing_vars:
        print(f"❌ Variáveis faltando: {', '.join(missing_vars)}")
        return False
    
    # Verificar cache de tokens
    cache_path = Path(f".secrets/bling_tokens_{client_id}.json")
    if not cache_path.exists():
        print(f"❌ Cache de tokens não encontrado: {cache_path}")
        print("Execute primeiro: python scripts/oauth2_setup.py {client_id}")
        return False
    
    try:
        tokens = json.loads(cache_path.read_text(encoding="utf-8"))
        print(f"✅ Cache de tokens encontrado: {cache_path}")
        
        # Verificar tokens
        access_token = tokens.get("access_token")
        refresh_token = tokens.get("refresh_token")
        expires_at = tokens.get("expires_at")
        
        if not access_token:
            print("❌ access_token não encontrado no cache")
            return False
        
        if not refresh_token:
            print("⚠️ refresh_token não encontrado - renovação automática não funcionará")
        
        if expires_at:
            import time
            now = int(time.time())
            if now >= expires_at:
                print("⚠️ Token expirado - será renovado automaticamente na próxima execução")
            else:
                remaining = expires_at - now
                print(f"✅ Token válido por mais {remaining} segundos")
        
        print(f"✅ access_token: {'*' * 20}... (presente)")
        if refresh_token:
            print(f"✅ refresh_token: {'*' * 20}... (presente)")
        
    except Exception as e:
        print(f"❌ Erro ao ler cache de tokens: {e}")
        return False
    
    return True

def test_bling_api_call(client_id: str) -> bool:
    """Testa uma chamada real para a API do Bling."""
    print(f"\n🌐 Testando chamada para API do Bling...")
    
    try:
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
        
        # Criar adapter para produtos (endpoint simples para teste)
        api = APISourceAdapter(
            endpoint="https://www.bling.com.br/Api/v3/produtos",
            auth=auth_config,
            paging={
                "mode": "page",
                "page_param": "pagina",
                "size_param": "limite",
                "size": 5,  # apenas 5 para teste
                "start_page": 1
            },
            default_headers={
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
        )
        
        # Fazer chamada de teste
        print("📡 Fazendo chamada de teste...")
        df = api.extract()
        
        print(f"✅ API funcionando! Retornou {len(df)} produtos")
        if len(df) > 0:
            print("📋 Primeiro produto:")
            first_product = df.iloc[0]
            for col in df.columns[:5]:  # mostrar apenas 5 colunas
                print(f"  - {col}: {first_product[col]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro na chamada da API: {e}")
        return False

def main():
    if len(sys.argv) != 2:
        print("Uso: python -m core.tests.test_bling_oauth2 CLIENT_ID")
        print("Exemplo: python -m core.tests.test_bling_oauth2 LOJAJUNTOS")
        sys.exit(1)
    
    client_id = sys.argv[1].strip()
    
    print(f"🔍 Testando configuração OAuth2 do Bling para {client_id}")
    print("=" * 60)
    
    # Teste 1: Configuração
    config_ok = test_oauth2_config(client_id)
    
    if not config_ok:
        print("\n❌ Configuração OAuth2 falhou!")
        print("Execute primeiro: python scripts/oauth2_setup.py {client_id}")
        sys.exit(1)
    
    # Teste 2: Chamada da API
    api_ok = test_bling_api_call(client_id)
    
    if api_ok:
        print(f"\n🎉 Todos os testes passaram para {client_id}!")
        print("A configuração OAuth2 está funcionando corretamente.")
    else:
        print(f"\n❌ Teste da API falhou para {client_id}")
        sys.exit(1)

if __name__ == "__main__":
    main()
