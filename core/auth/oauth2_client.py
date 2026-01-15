from __future__ import annotations
import json
import time
import requests
import logging
from pathlib import Path
from typing import Dict, Any

# Importa as exceções padronizadas
from core.errors.exceptions import AuthenticationError, ConfigurationError

log = logging.getLogger(__name__)

class OAuth2Client:
    """
    Cliente genérico para fluxo Authorization Code (OAuth2).
    Gerencia troca de código, refresh automático e persistência de tokens em arquivo.
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        scope: str = "",
        cache_path: Path = Path(".secrets/tokens.json")
    ):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.scope = scope
        self.cache_path = Path(cache_path)
        
        # Buffer de segurança: Renova o token se faltar menos de X segundos para expirar
        # 300 segundos = 5 minutos.
        self.expiry_buffer = 300 

        # Validação básica
        if not self.client_id or not self.client_secret:
            raise ConfigurationError("OAuth2Client: client_id e client_secret são obrigatórios.")

    def ensure_access_token(self) -> str:
        """
        Retorna um Access Token válido.
        1. Tenta carregar do cache.
        2. Verifica se expirou (considerando o buffer).
        3. Se expirou, tenta Refresh.
        4. Se não tem refresh token, levanta erro pedindo login manual.
        """
        tokens = self._load_tokens()
        
        if not tokens:
            raise AuthenticationError(f"Nenhum token encontrado em {self.cache_path}. Realize o fluxo de autorização manual (run_tests.py --test complete).")

        access_token = tokens.get("access_token")
        refresh_token = tokens.get("refresh_token")
        expires_at = tokens.get("expires_at", 0)
        
        now = time.time()
        
        # Verifica se está vencido ou perto de vencer
        if now >= (expires_at - self.expiry_buffer):
            log.info("⌛ Token próximo da expiração ou vencido. Tentando Refresh...")
            
            if not refresh_token:
                raise AuthenticationError("Token expirado e sem refresh_token disponível. Necessário re-autenticar.")
            
            new_tokens = self.refresh(refresh_token)
            return new_tokens["access_token"]
        
        return access_token

    def exchange_code(self, code: str) -> Dict[str, Any]:
        """
        Passo 2 do OAuth: Troca o 'Authorization Code' (recebido no callback)
        pelos tokens definitivos (Access + Refresh).
        """
        log.info("🔄 Trocando Authorization Code por Tokens...")
        
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }
        
        auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        
        try:
            response = requests.post(self.token_url, data=payload, auth=auth, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            self._save_tokens(data)
            return data
            
        except requests.exceptions.RequestException as e:
            msg = f"Falha na troca de código: {e}"
            if e.response is not None:
                msg += f" | Resposta: {e.response.text}"
            raise AuthenticationError(msg) from e

    def refresh(self, refresh_token: str = None) -> Dict[str, Any]:
        """
        Usa o Refresh Token para obter um novo Access Token sem interação do usuário.
        """
        if not refresh_token:
            tokens = self._load_tokens()
            refresh_token = tokens.get("refresh_token")
            
        if not refresh_token:
            raise AuthenticationError("Impossível renovar: Refresh Token ausente.")

        log.info("🔄 Executando Refresh Token...")
        
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
        
        auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        
        try:
            response = requests.post(self.token_url, data=payload, auth=auth, timeout=15)
            
            if response.status_code in [400, 401]:
                log.error(f"❌ Refresh Token rejeitado: {response.text}")
                raise AuthenticationError("Refresh Token expirado ou inválido. Faça login novamente.")
            
            response.raise_for_status()
            
            data = response.json()
            self._save_tokens(data)
            log.info("✅ Token renovado com sucesso.")
            return data

        except requests.exceptions.RequestException as e:
            msg = f"Erro de conexão no Refresh: {e}"
            if e.response is not None:
                msg += f" | Resposta: {e.response.text}"
            raise AuthenticationError(msg) from e

    def _save_tokens(self, token_data: Dict[str, Any]):
        """
        Salva os tokens no arquivo JSON, calculando o tempo absoluto de expiração.
        """
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        expires_in = token_data.get("expires_in", 3600)
        token_data["expires_at"] = time.time() + int(expires_in)
        
        if "refresh_token" not in token_data:
            old = self._load_tokens()
            if old and "refresh_token" in old:
                token_data["refresh_token"] = old["refresh_token"]
                log.debug("Preservando refresh_token antigo (API não retornou um novo).")

        try:
            with open(self.cache_path, "w") as f:
                json.dump(token_data, f, indent=2)
            log.debug(f"Tokens salvos em {self.cache_path}")
        except Exception as e:
            log.error(f"Falha ao salvar cache de tokens: {e}")

    def _load_tokens(self) -> Dict[str, Any]:
        """Lê o JSON de tokens."""
        if not self.cache_path.exists():
            return {}
        
        try:
            with open(self.cache_path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            log.warning(f"Cache de token corrompido em {self.cache_path}. Ignorando.")
            return {}