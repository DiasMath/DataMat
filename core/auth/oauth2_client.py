# core/auth/oauth2_client.py
from __future__ import annotations
import base64, json, os, time
from pathlib import Path
from typing import Dict, Optional
import requests

class OAuth2Client:
    """
    Cliente OAuth2 genérico (Authorization Code + Refresh).
    Lê URLs e credenciais do .env (ou via __init__).
    """

    def __init__(
        self,
        token_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        scope: Optional[str] = None,
        cache_path: str | Path = ".secrets/oauth_tokens.json",
        timeout: int = 30,
        use_basic_auth: bool = True,
        extra_token_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.token_url = token_url or os.getenv("OAUTH_TOKEN_URL", "")
        self.client_id = client_id or os.getenv("OAUTH_CLIENT_ID", "")
        self.client_secret = client_secret or os.getenv("OAUTH_CLIENT_SECRET", "")
        self.redirect_uri = redirect_uri or os.getenv("OAUTH_REDIRECT_URI", "")
        self.scope = scope or os.getenv("OAUTH_SCOPE", None)
        self.timeout = int(timeout)
        self.use_basic_auth = use_basic_auth
        self.extra_token_headers = dict(extra_token_headers or {})
        if not self.token_url or not self.client_id:
            raise RuntimeError("OAuth2Client: OAUTH_TOKEN_URL e OAUTH_CLIENT_ID são obrigatórios.")
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._tokens: Dict[str, str] = self._load_tokens()

    def _load_tokens(self) -> Dict[str, str]:
        if self.cache_path.exists():
            try:
                return json.loads(self.cache_path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_tokens(self) -> None:
        self.cache_path.write_text(json.dumps(self._tokens, ensure_ascii=False, indent=2), encoding="utf-8")

    def _basic_auth_header(self) -> str:
        raw = f"{self.client_id}:{self.client_secret}".encode()
        return base64.b64encode(raw).decode()

    def _token_headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        if self.use_basic_auth:
            headers["Authorization"] = f"Basic {self._basic_auth_header()}"
        headers.update(self.extra_token_headers)
        return headers

    def _post_token(self, data: Dict[str, str]) -> Dict:
        resp = requests.post(self.token_url, headers=self._token_headers(), data=data, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _set_exp(self, tokens: Dict) -> Dict:
        exp = int(tokens.get("expires_in", 3600))
        tokens["expires_at"] = int(time.time()) + max(0, exp - 60)
        return tokens

    def exchange_code(self, authorization_code: Optional[str] = None) -> Dict:
        """Troca o código de autorização por tokens, garantindo a preservação do refresh_token."""
        code = authorization_code or os.getenv("OAUTH_AUTH_CODE", "")
        if not code:
            raise RuntimeError("OAuth2Client: informe OAUTH_AUTH_CODE para a primeira troca de tokens.")
        
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }
        if not self.use_basic_auth:
            payload.update({"client_id": self.client_id, "client_secret": self.client_secret})
        if self.scope:
            payload["scope"] = self.scope
            
        new_tokens = self._set_exp(self._post_token(payload))

        # --- CORREÇÃO DEFINITIVA AQUI ---
        # A API do Bling SÓ ENVIA o refresh_token na primeira vez.
        # Esta lógica garante que, uma vez obtido, ele nunca mais seja perdido.
        if "refresh_token" not in new_tokens:
            old_refresh_token = self._tokens.get("refresh_token")
            if old_refresh_token:
                new_tokens["refresh_token"] = old_refresh_token
        
        self._tokens = new_tokens
        # --- FIM DA CORREÇÃO ---

        self._save_tokens()
        return self._tokens

    def refresh(self) -> Dict:
        """Renova o token de acesso, preservando o refresh_token."""
        rt = self._tokens.get("refresh_token")
        if not rt:
            raise RuntimeError("OAuth2Client: refresh_token ausente; refaça o authorize+code.")
            
        payload = {"grant_type": "refresh_token", "refresh_token": rt}
        if not self.use_basic_auth:
            payload.update({"client_id": self.client_id, "client_secret": self.client_secret})
            
        new_tokens = self._set_exp(self._post_token(payload))
        new_tokens.setdefault("refresh_token", rt)
        
        self._tokens.update(new_tokens)
        self._save_tokens()
        return self._tokens

    def ensure_access_token(self) -> str:
        """Garante um token de acesso válido, renovando se necessário."""
        at = self._tokens.get("access_token")
        exp = int(self._tokens.get("expires_at", 0))
        
        if not at or time.time() >= exp:
            self.refresh()
            at = self._tokens.get("access_token")
            
        if not at:
            raise RuntimeError("OAuth2Client: falha ao garantir access_token.")
        return at