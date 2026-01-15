import sys
import os
import requests
import logging
from pathlib import Path
from dotenv import load_dotenv

# --- CONFIGURAÇÃO DE CAMINHO ---
# Calcula a raiz do projeto (subindo um nível a partir de 'scripts')
ROOT_DIR = Path(__file__).resolve().parent.parent

# Adiciona a raiz ao path do Python (caso precise importar módulos do core no futuro)
sys.path.append(str(ROOT_DIR))

# Configura logs básicos
logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger("TelegramIDFinder")

def get_updates():
    """Busca as últimas mensagens para descobrir o ID do grupo."""
    
    # 1. Carrega o .env da raiz
    env_path = ROOT_DIR / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        log.warning(f"⚠️  Arquivo .env não encontrado na raiz: {env_path}")

    # 2. Obtém o Token
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        log.error("❌ Erro: TELEGRAM_BOT_TOKEN não encontrado nas variáveis de ambiente.")
        log.error("   Verifique se o arquivo .env existe e possui a chave TELEGRAM_BOT_TOKEN.")
        return

    # 3. Faz a requisição
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    log.info("🔍 Consultando API do Telegram para o Bot...")

    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            log.error(f"❌ Erro HTTP {response.status_code}: {response.text}")
            return

        data = response.json()
        if not data.get("ok"):
            log.error(f"❌ Erro na API do Telegram: {data}")
            return
            
        results = data.get("result", [])
        
        if not results:
            log.warning("⚠️  Nenhuma mensagem recente encontrada.")
            log.warning("   DICA: Adicione o bot ao grupo, envie uma mensagem lá (ex: 'oi') e rode este script novamente.")
            return

        # 4. Exibe os resultados
        log.info("\n📬 Conversas Encontradas (Copie o ID abaixo):")
        log.info("-" * 60)
        
        processed_ids = set()
        
        # Itera do mais recente para o mais antigo
        for update in reversed(results):
            if "message" in update:
                chat = update["message"]["chat"]
            elif "my_chat_member" in update:
                chat = update["my_chat_member"]["chat"]
            else:
                continue

            c_id = chat["id"]
            c_type = chat["type"]
            c_title = chat.get("title", chat.get("username", "Sem Nome"))
            
            if c_id not in processed_ids:
                icon = "👥" if c_type in ["group", "supergroup"] else "👤"
                
                log.info(f"{icon} Tipo: {c_type.upper()}")
                log.info(f"   Nome: {c_title}")
                log.info(f"   🆔 ID: {c_id}")
                log.info("-" * 60)
                
                processed_ids.add(c_id)

    except requests.exceptions.RequestException as e:
        log.error(f"❌ Falha de Conexão: {e}")
    except Exception as e:
        log.error(f"❌ Erro Inesperado: {e}")

if __name__ == "__main__":
    get_updates()