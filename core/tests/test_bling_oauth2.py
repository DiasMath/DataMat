#!/usr/bin/env python3
"""
Teste para validar configuração OAuth2 do Bling e fazer uma chamada real à API.
"""
from __future__ import annotations
import os, sys, json
from pathlib import Path
from .test_utils import load_test_envs # <-- Importa a função centralizada

def test_oauth2_config(client_id: str) -> bool:
    """Testa se a configuração OAuth2 está completa."""
    print(f"🧪 Testando configuração OAuth2 para {client_id}")
    load_test_envs(client_id)
    
    required_vars = ["OAUTH_TOKEN_URL", "OAUTH_CLIENT_ID", "OAUTH_CLIENT_SECRET"]
    missing_vars = [var for var in required_vars if not os.getenv(var, "").strip()]
    
    if missing_vars:
        print(f"❌ Variáveis faltando no .env: {', '.join(missing_vars)}")
        return False
    
    print("✅ Variáveis de ambiente OAuth2 encontradas.")
    return True

def test_bling_api_call(client_id: str) -> bool:
    """Testa uma chamada real para a API do Bling, usando a arquitetura atual."""
    print(f"\n🌐 Testando chamada para API do Bling com filtro (params)...")
    load_test_envs(client_id)
    
    try:
        from core.adapters.api_adapter import APISourceAdapter
        
        # Define a configuração de autenticação reutilizável
        auth_config = {
            "kind": "oauth2_generic",
            "token_cache": f".secrets/bling_tokens_{client_id}.json",
            "token_url_env": "OAUTH_TOKEN_URL",
            "client_id_env": "OAUTH_CLIENT_ID",
            "client_secret_env": "OAUTH_CLIENT_SECRET",
        }
        
        # Monta a URL base a partir do .env
        base_url = os.getenv("API_BASE_URL", "").rstrip('/')
        if not base_url:
            raise RuntimeError("API_BASE_URL não definida no .env do tenant.")
        
        # Cria o adapter para o endpoint de produtos
        api = APISourceAdapter(
            endpoint=f"{base_url}/produtos",
            auth=auth_config,
            paging={"mode": "page", "page_param": "pagina", "size_param": "limit", "size": 100},
            # Testa o filtro de produtos ativos usando o atributo 'params'
            params={}
        )
        
        print(f"📡 Fazendo chamada para '{api.endpoint}' com params={api.base_params}...")
        df = api.extract()
        
        print(f"✅ API funcionando! Retornou {len(df)} produtos ativos.")
        if len(df) > 0:
            print("📋 Exemplo de produto retornado:")
            print(df.iloc[0][['id', 'codigo', 'nome', 'situacao']].to_string())
        
        return True
        
    except Exception as e:
        print(f"❌ Erro na chamada da API: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    if len(sys.argv) != 2:
        print(f"Uso: python -m {__package__}.test_bling_oauth2 CLIENT_ID")
        sys.exit(1)
    
    client_id = sys.argv[1].strip()
    
    config_ok = test_oauth2_config(client_id)
    if not config_ok:
        sys.exit(1)
    
    api_ok = test_bling_api_call(client_id)
    if not api_ok:
        sys.exit(1)

    print(f"\n🎉 Testes de OAuth2 e API para {client_id} passaram com sucesso!")

if __name__ == "__main__":
    main()