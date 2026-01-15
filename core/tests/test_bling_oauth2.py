# core/tests/test_bling_oauth2.py
from __future__ import annotations
import os
import sys
import logging
import requests
from pathlib import Path
from .test_utils import load_test_envs

# Adiciona raiz ao path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from core.auth.oauth2_client import OAuth2Client

log = logging.getLogger("TestBlingOAuth")

def test_oauth2_config(client_id: str) -> bool:
    """Valida se as variáveis de ambiente obrigatórias existem."""
    print(f"🔍 [1/2] Verificando configurações para {client_id}...")
    load_test_envs(client_id)
    
    required = [
        "OAUTH_TOKEN_URL", "OAUTH_CLIENT_ID", 
        "OAUTH_CLIENT_SECRET", "OAUTH_REDIRECT_URI"
    ]
    
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        print(f"❌ Faltam variáveis no .env: {', '.join(missing)}")
        return False
        
    print("✅ Configuração de ambiente OK.")
    return True

def test_bling_api_call(client_id: str) -> bool:
    """
    Usa o token existente (gerado pelo setup) para fazer uma chamada real à API.
    """
    print("📡 [2/2] Testando chamada real de API (usando token do setup)...")
    
    # 1. Recupera credenciais do ambiente (Necessário para o Refresh funcionar)
    token_url = os.getenv("OAUTH_TOKEN_URL")
    # Tenta pegar OAUTH_CLIENT_ID, se não tiver, pega BLING_CLIENT_ID
    cid = os.getenv("OAUTH_CLIENT_ID") or os.getenv("BLING_CLIENT_ID")
    csec = os.getenv("OAUTH_CLIENT_SECRET") or os.getenv("BLING_CLIENT_SECRET")
    redir = os.getenv("OAUTH_REDIRECT_URI")
    scope = os.getenv("OAUTH_SCOPE") # Opcional

    # 2. Localiza o cache usando o ID (Hash)
    token_file = ROOT_DIR / ".secrets" / f"bling_tokens_{cid}.json"
    
    if not token_file.exists():
        print(f"❌ Token não encontrado em: {token_file}")
        print(f"💡 DICA: Rode 'python scripts/oauth2_setup.py {client_id}' primeiro!")
        return False

    try:
        # 3. Inicializa cliente COMPLETO (Correção do Erro)
        # Precisamos passar as credenciais para ele conseguir fazer o refresh se precisar
        oauth = OAuth2Client(
            token_url=token_url,
            client_id=cid,
            client_secret=csec,
            redirect_uri=redir,
            scope=scope,
            cache_path=token_file
        )
        
        # 4. Garante que o token está válido
        print("   -> Verificando validade do token (Refresh automático)...")
        token = oauth.ensure_access_token()
        
        # 5. Faz uma chamada leve
        base_url = os.getenv("API_BASE_URL", "https://www.bling.com.br/Api/v3")
        url = f"{base_url.rstrip('/')}/vendedores"
        
        print(f"   -> GET {url} ...")
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        resp = requests.get(url, headers=headers, params={"limit": 1}, timeout=10)
        
        if resp.status_code == 200:
            print("✅ SUCESSO! A API respondeu corretamente.")
            return True
        else:
            print(f"❌ A API retornou erro: {resp.status_code}")
            print(f"   Corpo: {resp.text}")
            return False

    except Exception as e:
        print(f"❌ Erro na execução do teste: {e}")
        # print(traceback.format_exc()) # Descomente para ver o erro completo se precisar
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python -m core.tests.test_bling_oauth2 CLIENTE")
        sys.exit(1)
    
    cli = sys.argv[1]
    if test_oauth2_config(cli):
        test_bling_api_call(cli)