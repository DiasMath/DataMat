import logging
from pathlib import Path
from dotenv import load_dotenv

# --- CONFIGURAÇÃO DE LOG ---
log = logging.getLogger("core.env")

# --- DEFINIÇÃO DA RAIZ ---
# Assume que este arquivo está em: /core/env.py
# .parent = /core
# .parent.parent = / (Raiz do Projeto)
ROOT_DIR = Path(__file__).resolve().parent.parent

def load_global_env():
    """
    Carrega apenas as variáveis globais (da raiz).
    Útil para scripts que não são de um cliente específico (ex: master_nightly).
    """
    global_path = ROOT_DIR / ".env"
    
    if global_path.exists():
        # Carrega sem sobrescrever o que já existe no sistema (prioridade para variáveis de sistema)
        load_dotenv(dotenv_path=global_path)
    else:
        log.warning(f"⚠️  Arquivo .env global não encontrado em: {global_path}")

def load_tenant_env(tenant_id: str) -> bool:
    """
    Carrega o ambiente completo para um cliente:
    1. Carrega o .env Global (Base)
    2. Carrega o .env do Tenant (Sobrescreve/Específico)
    
    Retorna True se o arquivo do cliente foi encontrado e carregado.
    """
    # 1. Garante que as globais (ex: Telegram Token) estejam carregadas
    load_global_env()

    # 2. Busca o arquivo específico do cliente
    tenant_env_path = ROOT_DIR / "tenants" / tenant_id / "config" / ".env"
    
    if not tenant_env_path.exists():
        log.error(f"❌ Configuração (.env) não encontrada para: {tenant_id}")
        return False

    # 3. Carrega o Tenant com override=True
    # Isso garante que se o cliente tiver uma DB_URL ou TELEGRAM_CHAT_ID próprios,
    # eles ganhem prioridade sobre o global.
    load_dotenv(dotenv_path=tenant_env_path, override=True)
    
    return True