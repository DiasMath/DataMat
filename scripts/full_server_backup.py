import os
import sys
import subprocess
import datetime
import shutil
from pathlib import Path
from dotenv import load_dotenv

# =============================================================================
# SETUP DE AMBIENTE E PATHS
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))
load_dotenv(BASE_DIR / ".env")

try:
    from core.alerts.observer import observer
except ImportError as e:
    print(f"[ERRO CRÍTICO] Falha ao importar core.alerts.observer: {e}")
    sys.exit(1)

# =============================================================================
# CONFIGURAÇÕES DE BANCO E DIRETÓRIOS
# =============================================================================
# Tratamento Sênior: Separa HOST e PORTA se vierem juntos no .env
db_host_raw = os.getenv("DB_HOST", "localhost")
if ":" in db_host_raw:
    DB_HOST, DB_PORT = db_host_raw.split(":", 1)
else:
    DB_HOST = db_host_raw
    DB_PORT = "3306"

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

BACKUP_ROOT_LOCAL = Path(os.getenv("BACKUP_PATH_LOCAL", r"D:\DATAMAT\BACKUPS_BANCO"))
BACKUP_ROOT_CLOUD = Path(os.getenv("BACKUP_PATH_CLOUD", r"G:\Meu Drive\Datamat_Backups"))
RETENCAO_DIAS_LOCAL = int(os.getenv("BACKUP_RETENTION_DAYS", 180))

SYSTEM_DBS = {'information_schema', 'performance_schema', 'mysql', 'sys', 'innodb', 'tmp'}

# =============================================================================
# GERENCIADOR DE BACKUP
# =============================================================================
class BackupManager:
    def __init__(self):
        self.log_buffer = []
        self.has_error = False
        self.start_time = datetime.datetime.now()

    def log(self, msg, status="INFO"):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        if status == "SUCCESS": 
            icon = "✅"
        elif status == "ERROR": 
            icon = "❌"
            self.has_error = True
        elif status == "WARNING":
            icon = "⚠️"
        else: 
            icon = "ℹ️"
        
        formatted_msg = f"{icon} {msg}"
        print(f"[{timestamp}] {formatted_msg}")
        self.log_buffer.append(formatted_msg)

    def run(self):
        self.log(f"Iniciando Backup Mensal no servidor {DB_HOST} (Porta: {DB_PORT})...", "INFO")
        
        if not DB_USER or not DB_PASS:
            self.log("DB_USER ou DB_PASS não definidos no .env", "ERROR")
            self.finalize()
            return

        if not shutil.which("mysqldump"):
            self.log("Executável 'mysqldump' não encontrado no PATH do sistema", "ERROR")
            self.finalize()
            return

        try:
            BACKUP_ROOT_LOCAL.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
            pasta_destino = BACKUP_ROOT_LOCAL / f"{timestamp}_FULL_SERVER"
            pasta_destino.mkdir(exist_ok=True)
        except Exception as e:
            self.log(f"Erro ao criar diretórios locais: {e}", "ERROR")
            self.finalize()
            return

        # 3. Listagem de Bancos (Agora com parâmetro --port)
        try:
            cmd_list = [
                "mysql", 
                f"--user={DB_USER}", 
                f"--password={DB_PASS}", 
                f"--host={DB_HOST}", 
                f"--port={DB_PORT}",
                "--default-character-set=utf8mb4", 
                "-e", "SHOW DATABASES;", 
                "-s", "--skip-column-names"
            ]
            output = subprocess.check_output(cmd_list, text=True, encoding='utf-8')
            all_databases = [db.strip() for db in output.splitlines() if db.strip()]
        except Exception as e:
            self.log("Falha ao conectar no MySQL para listar bancos.", "ERROR")
            self.finalize()
            return

        # 4. Execução do Dump (Agora com parâmetro --port)
        for db in all_databases:
            if db.lower() in SYSTEM_DBS:
                continue
            
            arquivo_sql = pasta_destino / f"{db}.sql"
            dump_cmd = [
                "mysqldump", 
                f"--user={DB_USER}", 
                f"--password={DB_PASS}", 
                f"--host={DB_HOST}", 
                f"--port={DB_PORT}",
                "--default-character-set=utf8mb4", 
                "--force", "--opt", "--routines", "--triggers", 
                "--events", "--hex-blob", "--single-transaction", "--set-gtid-purged=OFF",    
                f"--result-file={arquivo_sql}", db
            ]
            
            try:
                subprocess.run(dump_cmd, check=True)
                self.log(f"Banco salvo: {db}", "SUCCESS")
            except subprocess.CalledProcessError:
                self.log(f"Falha ao salvar banco: {db}", "ERROR")
                if arquivo_sql.exists(): 
                    arquivo_sql.unlink()

        # 5. Sincronização com Nuvem
        if BACKUP_ROOT_CLOUD.exists():
            try:
                shutil.copytree(pasta_destino, BACKUP_ROOT_CLOUD / pasta_destino.name)
                self.log("Cópia para Nuvem (Google Drive) concluída", "SUCCESS")
            except Exception as e:
                self.log(f"Erro no upload para nuvem: {e}", "ERROR")
        else:
            self.log(f"Caminho da nuvem não encontrado: {BACKUP_ROOT_CLOUD}", "WARNING")

        # 6. Manutenção
        self.limpar_backups_antigos()
        self.finalize()

    def limpar_backups_antigos(self):
        cutoff = datetime.datetime.now() - datetime.timedelta(days=RETENCAO_DIAS_LOCAL)
        for item in BACKUP_ROOT_LOCAL.iterdir():
            if item.is_dir():
                try:
                    data_str = item.name[:10] 
                    data_pasta = datetime.datetime.strptime(data_str, "%Y-%m-%d")
                    if data_pasta < cutoff:
                        shutil.rmtree(item)
                        self.log(f"Removido backup antigo: {item.name}", "INFO")
                except:
                    continue

    def finalize(self):
        duration = datetime.datetime.now() - self.start_time
        
        msg = "<b>🛡️ RELATÓRIO DE BACKUP MENSAL</b>\n"
        msg += f"📅 Data: {self.start_time.strftime('%d/%m/%Y %H:%M')}\n"
        msg += f"⏱️ Duração: {str(duration).split('.')[0]}\n"
        msg += "-" * 20 + "\n"
        
        body = "\n".join(self.log_buffer)
        msg += body
        
        msg += "\n" + ("-" * 20) + "\n"
        if self.has_error:
            msg += "🚨 <b>STATUS: FALHA</b>"
        else:
            msg += "✅ <b>STATUS: SUCESSO</b>"
            
        try:
            observer.notify_system_event(
                subject="Relatório de Backup", 
                message=msg, 
                is_error=self.has_error, 
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"[AVISO] Erro ao enviar notificação (Verifique seu provedor de email/telegram): {e}")
            
        print("[SISTEMA] Processo finalizado.")

if __name__ == "__main__":
    BackupManager().run()