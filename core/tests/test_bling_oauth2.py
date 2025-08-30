# core/tests/test_bling_oauth2.py

"""
Teste focado para validar o fluxo completo do OAuth2Client de forma isolada.
Este script é autocontido e guia o usuário pelo processo de autorização.
"""
from __future__ import annotations
import os
import sys
import webbrowser
import threading
import time
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode
from http.server import BaseHTTPRequestHandler, HTTPServer

# Adiciona o diretório raiz ao path para permitir imports de 'core'
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Função auxiliar para carregar os .env
def load_test_envs(client_id: str):
    """Carrega os ambientes de teste (global e do cliente)."""
    try:
        from dotenv import load_dotenv
        print(f"Carregando ambientes para o cliente: {client_id}")
        load_dotenv(".env", override=True)
        tenant_env = Path("tenants") / client_id / "config" / ".env"
        if tenant_env.exists():
            load_dotenv(tenant_env, override=True)
    except ImportError:
        print("Aviso: python-dotenv não instalado. Usando variáveis de ambiente existentes.")

# Lógica de autorização, agora dentro do próprio teste
def get_auth_code_from_user(client) -> str | None:
    """Guia o usuário pelo fluxo de autorização para obter o 'code'."""
    state = f"test-state-{int(time.time())}"
    
    # Monta a URL de autorização
    q = {"response_type": "code", "client_id": client.client_id, "state": state, "redirect_uri": client.redirect_uri}
    auth_url = f"{os.getenv('OAUTH_AUTH_URL')}?{urlencode(q)}"
    
    print("\nAbra/autorize nesta URL:")
    print(auth_url)
    webbrowser.open(auth_url)

    # Inicia um servidor local para capturar o callback
    code_holder = {}
    port = urlparse(client.redirect_uri).port

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            qs = parse_qs(urlparse(self.path).query)
            if qs.get("state", [None])[0] == state:
                code_holder["code"] = qs.get("code", [None])[0]
                self.send_response(200)
                self.wfile.write(b"<h1>Autorizado!</h1><p>Pode fechar esta janela e voltar ao terminal.</p>")
            else:
                self.send_response(400)
                self.wfile.write(b"<h1>Erro de State Mismatch</h1>")
        def log_message(self, format, *args):
            return

    httpd = HTTPServer(("127.0.0.1", port), CallbackHandler)
    server_thread = threading.Thread(target=httpd.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    print(f"\nAguardando callback em http://127.0.0.1:{port} ...")
    try:
        # Espera até 2 minutos pelo código
        for _ in range(240):
            if "code" in code_holder:
                print("✅ Código de autorização recebido via callback.")
                return code_holder["code"]
            time.sleep(0.5)
        raise TimeoutError("Tempo esgotado esperando pelo código de autorização.")
    finally:
        httpd.shutdown()

def test_oauth_client_flow(client_id: str) -> bool:
    """Testa o OAuth2Client de forma isolada, executando o ciclo completo."""
    print("\n" + "="*50)
    print("🧪 Testando o fluxo do OAuth2Client isoladamente...")
    print("="*50)
    
    try:
        from core.auth.oauth2_client import OAuth2Client
        
        load_test_envs(client_id)
        
        client_id_env = os.getenv("OAUTH_CLIENT_ID")
        token_cache_file = Path(f".secrets/bling_tokens_{client_id_env}.json")

        if token_cache_file.exists():
            print(f"🗑️  Deletando arquivo de token antigo: {token_cache_file}")
            token_cache_file.unlink()

        client = OAuth2Client(cache_path=token_cache_file)

        print("\n--- Etapa 1: Obter Código de Autorização ---")
        auth_code = get_auth_code_from_user(client)
        if not auth_code:
            return False
            
        print("\n--- Etapa 2: Trocar Código por Tokens ---")
        tokens = client.exchange_code(auth_code)
        print("✅ Troca de código realizada.")
        print("🔑 Tokens recebidos e salvos:")
        print(tokens)

        if "refresh_token" not in tokens or not tokens["refresh_token"]:
            print("\n❌ FALHA CRÍTICA: 'refresh_token' não foi encontrado nos tokens salvos!")
            return False
        
        print("\n--- Etapa 3: Renovar Token com Refresh Token ---")
        refreshed_tokens = client.refresh()
        print("✅ Refresh realizado com sucesso.")
        print("🔑 Tokens após o refresh:")
        print(refreshed_tokens)

        if "access_token" not in refreshed_tokens or not refreshed_tokens["access_token"]:
            print("\n❌ FALHA CRÍTICA: 'access_token' não foi encontrado após o refresh!")
            return False

        print("\n✅ Fluxo do OAuth2Client passou com sucesso.")
        return True

    except Exception as e:
        print(f"\n❌ Erro durante o teste do OAuth2Client: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    if len(sys.argv) != 2:
        print(f"Uso: python -m {__package__} CLIENT_ID")
        sys.exit(1)
    
    client_id = sys.argv[1].strip()
    
    client_ok = test_oauth_client_flow(client_id)
    if not client_ok:
        print("\n🛑 Teste do OAuth2Client falhou. A depuração deve focar no 'core/auth/oauth2_client.py'.")
        sys.exit(1)

    print(f"\n🎉 Todos os testes de autenticação para '{client_id}' passaram com sucesso!")

if __name__ == "__main__":
    main()