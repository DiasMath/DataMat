# core/tests/test_utils.py
from __future__ import annotations
from pathlib import Path

def load_test_envs(client_id: str) -> None:
    """
    Carrega de forma robusta os arquivos .env para o ambiente de teste.
    1. Encontra o diretório raiz do projeto.
    2. Carrega o .env global.
    3. Carrega o .env específico do tenant (cliente) com prioridade.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        print("Aviso: python-dotenv não instalado. O carregamento de .env pode falhar.")
        return

    # Encontra o diretório raiz (assumindo que core/tests está dois níveis abaixo)
    project_root = Path(__file__).resolve().parents[2]

    # 1. Carrega o .env da raiz do projeto
    global_env = project_root / ".env"
    if global_env.exists():
        load_dotenv(dotenv_path=global_env, override=False)

    # 2. Carrega o .env do tenant com prioridade (override=True)
    tenant_env = project_root / "tenants" / client_id / "config" / ".env"
    if tenant_env.exists():
        load_dotenv(dotenv_path=tenant_env, override=True)
    else:
        print(f"Aviso: .env do tenant não encontrado em: {tenant_env}")