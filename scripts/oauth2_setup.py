# scripts/oauth2_setup.py
from __future__ import annotations
import os
import sys
import json
import time
import threading
import webbrowser
import argparse
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from dotenv import load_dotenv

# --- SETUP DE CAMINHOS ---
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from core.auth.oauth2_client import OAuth2Client

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger("OAuthSetup")

def build_authorize_url(base_auth_url: str, client_id: str, redirect_uri: str, state: str):
    from urllib.parse import urlencode
    q = {"response_type": "code", "client_id": client_id, "state": state, "redirect_uri": redirect_uri}
    return f"{base_auth_url}?{urlencode(q)}"

def _try_parse_port_from_redirect(redirect_uri: str) -> int | None:
    try:
        if redirect_uri.startswith(("http://127.0.0.1", "http://localhost")):
            hostport = redirect_uri.split("//",1)[1].split("/",1)[0]
            if ":" in hostport:
                return int(hostport.split(":")[1])
            return 80
    except Exception:
        pass
    return None

def run_local_server_for_code(expected_state: str, port: int, timeout_s: int = 120) -> str:
    code_holder = {"code": None, "state": None}
    
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.endswith("favicon.ico"):
                self.send_response(404); self.end_headers(); return

            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            new_code = (qs.get("code") or [None])[0]
            new_state = (qs.get("state") or [None])[0]

            if new_code:
                code_holder["code"] = new_code
                code_holder["state"] = new_state
            
            self.send_response(200)
            self.send_header("Content-Type","text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"<h1>Autorizado!</h1><p>Pode fechar esta janela e voltar ao terminal.</p>")
            
        def log_message(self, *args, **kwargs): return

    httpd = HTTPServer(("127.0.0.1", port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    
    print(f"⏳ Aguardando callback na porta {port} por {timeout_s}s...")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if code_holder["code"]: break
        time.sleep(0.5)
    
    httpd.shutdown()
    
    if not code_holder["code"]: raise TimeoutError("Tempo esgotado.")
    if code_holder["state"] != expected_state: raise RuntimeError(f"STATE inválido!")
    return code_holder["code"]

def main():
    parser = argparse.ArgumentParser(description="Gerador de Tokens OAuth2")
    parser.add_argument("client_id", help="Nome da pasta do tenant (Ex: LOJAJUNTOS)")
    args = parser.parse_args()
    
    tenant_folder = args.client_id
    print(f"🔧 Configurando OAuth2 para pasta: {tenant_folder}")

    global_env = ROOT_DIR / ".env"
    tenant_env = ROOT_DIR / "tenants" / tenant_folder / "config" / ".env"

    if not tenant_env.exists():
        log.error(f"❌ .env não encontrado em: {tenant_env}")
        return

    load_dotenv(global_env)
    load_dotenv(tenant_env, override=True)

    token_url = os.getenv("OAUTH_TOKEN_URL")
    cid = os.getenv("OAUTH_CLIENT_ID") or os.getenv("BLING_CLIENT_ID") # Hash Real
    csec = os.getenv("OAUTH_CLIENT_SECRET") or os.getenv("BLING_CLIENT_SECRET")
    redir = os.getenv("OAUTH_REDIRECT_URI")
    auth_url = os.getenv("OAUTH_AUTH_URL")
    scope = os.getenv("OAUTH_SCOPE")

    if not (auth_url and token_url and cid and csec and redir):
        log.error("❌ Configuração incompleta no .env")
        return

    # --- DEFINIÇÃO DO ARQUIVO (HASH) ---
    secrets_dir = ROOT_DIR / ".secrets"
    secrets_dir.mkdir(exist_ok=True)
    cache_path = secrets_dir / f"bling_tokens_{cid}.json"

    oauth = OAuth2Client(
        token_url=token_url, client_id=cid, client_secret=csec,
        redirect_uri=redir, scope=scope, cache_path=cache_path
    )

    state = f"init-{int(time.time())}"
    url = build_authorize_url(auth_url, cid, redir, state)
    
    print("\n" + "="*60)
    print("👉  Autorize no navegador:")
    print(url)
    print("="*60 + "\n")

    try: webbrowser.open(url)
    except: pass

    port = _try_parse_port_from_redirect(redir)
    code = None
    if port:
        try: code = run_local_server_for_code(state, port)
        except Exception as e: print(f"⚠️  Erro no servidor local: {e}")
    
    if not code:
        code = input("✍️  Cole o code aqui: ").strip()

    if code:
        print("🔄 Gerando tokens...")
        try:
            tokens = oauth.exchange_code(code)
            
            # --- O PULO DO GATO: INJETAR O NOME DO CLIENTE NO ARQUIVO ---
            # Reabre o arquivo salvo pelo client, adiciona o nome e salva de novo
            if cache_path.exists():
                with open(cache_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                # Adiciona etiqueta humana
                data["_tenant_label"] = tenant_folder
                data["_updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

                with open(cache_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
            # -------------------------------------------------------------

            print(f"\n✅ SUCESSO! Arquivo salvo em: {cache_path}")
            print(f"   (Etiquetado internamente como: {tenant_folder})")

        except Exception as e:
            log.error(f"❌ Erro: {e}")

if __name__ == "__main__":
    main()