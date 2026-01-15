from __future__ import annotations
import os
import sys
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
        print("⚠️  Aviso: python-dotenv não instalado. O carregamento de .env pode falhar.")
        return

    # Encontra o diretório raiz (assumindo que core/tests está dois níveis abaixo da raiz)
    # core/tests -> core -> raiz
    project_root = Path(__file__).resolve().parents[2]
    
    # Adiciona a raiz ao sys.path se não estiver lá, para permitir imports absolutos
    if str(project_root) not in sys.path:
        sys.path.append(str(project_root))

    # 1. Carrega o .env da raiz do projeto
    global_env = project_root / ".env"
    if global_env.exists():
        # print(f"   -> Carregando .env global: {global_env}")
        load_dotenv(dotenv_path=global_env, override=False)

    # 2. Carrega o .env do tenant com prioridade (override=True)
    if client_id:
        tenant_env = project_root / "tenants" / client_id / "config" / ".env"
        if tenant_env.exists():
            print(f"   -> Carregando .env do tenant '{client_id}': {tenant_env}")
            load_dotenv(dotenv_path=tenant_env, override=True)
        else:
            print(f"⚠️  Aviso: .env do tenant não encontrado em: {tenant_env}")
    
    # Garante que variáveis críticas existam
    if not os.getenv("OAUTH_CLIENT_ID"):
        print("❌ Erro: Variável OAUTH_CLIENT_ID não definida após carregar envs.")