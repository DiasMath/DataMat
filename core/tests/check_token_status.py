from __future__ import annotations
import os
import json
import time
from pathlib import Path
from .test_utils import load_test_envs

def check_token_status(client_id: str):
    print(f"🔍 Verificando status dos tokens para {client_id}")
    load_test_envs(client_id)
    
    real_client_id = os.getenv("OAUTH_CLIENT_ID")
    if not real_client_id:
        print("❌ Erro: OAUTH_CLIENT_ID não encontrado no .env")
        return

    # Caminho padrão do arquivo de tokens
    token_path = Path(f".secrets/bling_tokens_{real_client_id}.json")
    
    print(f"   -> Procurando arquivo: {token_path}")
    
    if not token_path.exists():
        print("❌ Arquivo de token NÃO ENCONTRADO.")
        print("   -> Execute o teste 'complete' para gerar um novo token.")
        return

    try:
        with open(token_path, 'r') as f:
            data = json.load(f)
            
        access_token = data.get("access_token")
        refresh_token = data.get("refresh_token")
        expires_at = data.get("expires_at", 0)
        
        print("\n📋 Dados do Token:")
        print(f"   -> Access Token: {'Presente ✅' if access_token else 'Ausente ❌'}")
        print(f"   -> Refresh Token: {'Presente ✅' if refresh_token else 'Ausente ❌'}")
        
        # Verifica expiração
        now = time.time()
        ttl = expires_at - now
        
        if ttl > 0:
            print(f"   -> Status: VÁLIDO ✅ (Expira em {ttl/60:.1f} minutos)")
        else:
            print(f"   -> Status: EXPIRADO ⚠️ (Expirou há {abs(ttl)/60:.1f} minutos)")
            print("      O sistema deve tentar o refresh automaticamente na próxima execução.")

    except Exception as e:
        print(f"❌ Erro ao ler arquivo de token: {e}")