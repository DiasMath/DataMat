# scripts/oauth2_setup.py
from __future__ import annotations
import os, sys, json, time, threading, webbrowser, argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path

def _load_envs(client_id: str | None):
    """Carrega .env global e do tenant (se client_id for fornecido)."""
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(".env", override=True)
        if client_id:
            load_dotenv(Path("tenants")/client_id/"config"/".env", override=True)
    except Exception:
        pass

class OAuth2Client:
    def __init__(self, token_url: str, client_id: str, client_secret: str,
                 redirect_uri: str, scope: str|None, cache_path: Path,
                 timeout: int = 30, use_basic_auth: bool = True):
        import requests, base64  # noqa
        self.requests = requests
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.scope = scope
        self.timeout = timeout
        self.use_basic_auth = use_basic_auth
        self.cache_path = cache_path
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.tokens = self._load()

    def _load(self):
        if self.cache_path.exists():
            try:
                return json.loads(self.cache_path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save(self):
        self.cache_path.write_text(json.dumps(self.tokens, indent=2, ensure_ascii=False), encoding="utf-8")

    def _headers(self):
        h = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
        if self.use_basic_auth:
            import base64
            b = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
            h["Authorization"] = f"Basic {b}"
        return h

    def _post(self, data: dict):
        r = self.requests.post(self.token_url, headers=self._headers(), data=data, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def exchange_code(self, code: str):
        data = {"grant_type":"authorization_code", "code":code, "redirect_uri": self.redirect_uri}
        if not self.use_basic_auth:
            data.update({"client_id": self.client_id, "client_secret": self.client_secret})
        if self.scope:
            data["scope"] = self.scope
        js = self._post(data)
        self.tokens.update(js)
        self._save()
        return js

    def refresh(self):
        rt = self.tokens.get("refresh_token")
        if not rt:
            raise RuntimeError("Sem refresh_token. Refazer authorize.")
        data = {"grant_type":"refresh_token", "refresh_token": rt}
        if not self.use_basic_auth:
            data.update({"client_id": self.client_id, "client_secret": self.client_secret})
        js = self._post(data)
        js.setdefault("refresh_token", rt)
        self.tokens.update(js)
        self._save()
        return js

def build_authorize_url(base_auth_url: str, client_id: str, redirect_uri: str, state: str):
    from urllib.parse import urlencode
    # Para Bling: response_type=code; redirect_uri/scope podem estar no app, mas enviar não atrapalha
    q = {"response_type":"code", "client_id":client_id, "state":state, "redirect_uri": redirect_uri}
    return f"{base_auth_url}?{urlencode(q)}"

def _try_parse_port_from_redirect(redirect_uri: str) -> int | None:
    try:
        if redirect_uri.startswith(("http://127.0.0.1", "http://localhost")):
            # exemplos: http://127.0.0.1:8910/callback ; http://localhost:3000/
            hostport = redirect_uri.split("//",1)[1].split("/",1)[0]
            if ":" in hostport:
                return int(hostport.split(":")[1])
            # sem porta explícita -> 80
            return 80
    except Exception:
        pass
    return None

def run_local_server_for_code(expected_state: str, port: int, timeout_s: int = 120) -> str:
    code_holder = {"code": None, "state": None}
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            code_holder["code"] = (qs.get("code") or [None])[0]
            code_holder["state"] = (qs.get("state") or [None])[0]
            self.send_response(200)
            self.send_header("Content-Type","text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"<h3>Autorizado! Pode voltar ao terminal.</h3>")
        def log_message(self, *args, **kwargs):
            return
    httpd = HTTPServer(("127.0.0.1", port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if code_holder["code"]:
            break
        time.sleep(0.25)
    httpd.shutdown()
    code = code_holder["code"]; st = code_holder["state"]
    if not code:
        raise TimeoutError("Não recebi o code via callback local (tempo esgotado).")
    if st != expected_state:
        raise RuntimeError("STATE não confere.")
    return code

def main():
    # ---- CLI ----
    p = argparse.ArgumentParser(description="Bootstrap OAuth2 (Bling v3 / genérico)")
    p.add_argument("client", nargs="?", help="CLIENT_ID (ex.: LOJAJUNTOS)")
    p.add_argument("--client", dest="client_flag", help="CLIENT_ID (flag alternativa)")
    args = p.parse_args()
    client_id = args.client or args.client_flag or os.getenv("CLIENT_ID", "")
    if not client_id:
        raise SystemExit("Informe o CLIENT_ID (arg posicional/--client) ou defina a var de ambiente CLIENT_ID")

    # disponibiliza para quem usa os.environ
    os.environ["CLIENT_ID"] = client_id

    # ---- .envs ----
    _load_envs(client_id)

    auth_url = os.getenv("OAUTH_AUTH_URL","").strip()
    token_url = os.getenv("OAUTH_TOKEN_URL","").strip()
    cid = os.getenv("OAUTH_CLIENT_ID","").strip()
    csec = os.getenv("OAUTH_CLIENT_SECRET","").strip()
    redir = os.getenv("OAUTH_REDIRECT_URI","").strip()
    scope = (os.getenv("OAUTH_SCOPE") or "").strip() or None
    if not (auth_url and token_url and cid and csec and redir):
        raise SystemExit("Preencha OAUTH_AUTH_URL, OAUTH_TOKEN_URL, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET e OAUTH_REDIRECT_URI no .env")

    cache = Path(f".secrets/bling_tokens_{client_id}.json")
    oauth = OAuth2Client(token_url, cid, csec, redir, scope, cache,
                         timeout=int(os.getenv("API_TIMEOUT","30")),
                         use_basic_auth=True)  # Bling: Basic no /oauth/token

    state = os.getenv("OAUTH_TEST_STATE") or f"state-{int(time.time())}"
    url = build_authorize_url(auth_url, cid, redir, state)
    print("\nAbra/autorize nesta URL:\n", url, "\n")

    # tenta abrir no browser
    try: webbrowser.open(url)
    except Exception: pass

    code = None
    port = _try_parse_port_from_redirect(redir)
    if port is not None:
        try:
            print(f"Aguardando callback em {redir} ...")
            code = run_local_server_for_code(state, port)
            print("Code recebido via callback.")
        except Exception as e:
            print("Callback local falhou:", e)

    if not code:
        manual = os.getenv("OAUTH_AUTH_CODE","").strip()
        if not manual:
            manual = input("Cole aqui o 'code' da URL de redirecionamento: ").strip()
        code = manual

    tokens = oauth.exchange_code(code)
    print("\n✅ Tokens salvos em:", cache)
    print(json.dumps({k: tokens.get(k) for k in ["token_type","expires_in","scope"]}, indent=2))

    # teste opcional de refresh
    try:
        rt = oauth.refresh()
        print("🔄 Refresh OK. expires_in:", rt.get("expires_in"))
    except Exception as e:
        print("Aviso: refresh não testado agora:", e)

if __name__ == "__main__":
    main()
