#!/usr/bin/env python3
"""
Master Nightly - Orquestrador de Cargas Noturnas (Modo Detalhado)
Gera um relatório extenso com contagem de linhas por tabela.
"""
import sys
import time
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# --- SETUP DE CAMINHOS ---
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# --- IMPORTS ---
from core.main import run_tenant_pipeline
# Usamos send_telegram_text para enviar a mensagem formatada manualmente
from core.alerts import send_telegram_text, observer
from core.env import load_global_env

# --- CONFIGURAÇÃO DE LOG ---
log_dir = ROOT_DIR / "nightly_logs"
log_dir.mkdir(exist_ok=True)

root_logger = logging.getLogger()
# Limpa handlers anteriores para evitar duplicidade em re-execuções manuais
if root_logger.handlers:
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)

nightly_log_file = log_dir / f"nightly_{datetime.now().strftime('%Y%m%d')}.log"
file_handler = logging.FileHandler(nightly_log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Adiciona console handler também para ver rodando
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(message)s'))

root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

log = logging.getLogger("MasterNightly")

def get_target_tenants() -> list:
    load_global_env()
    nightly_var = os.getenv("NIGHTLY_CLIENTS", "").strip()
    valid_tenants = []
    
    if nightly_var:
        log.info(f"📋 Modo Manual (NIGHTLY_CLIENTS): {nightly_var}")
        requested_tenants = [t.strip() for t in nightly_var.split(',') if t.strip()]
        for t in requested_tenants:
            tenant_path = ROOT_DIR / "tenants" / t
            if tenant_path.exists() and (tenant_path / "config" / ".env").exists():
                valid_tenants.append(t)
            else:
                log.warning(f"⚠️  Tenant '{t}' ignorado (pasta ou .env ausente).")
    else:
        log.info("🔍 NIGHTLY_CLIENTS vazio. Escaneando tenants...")
        tenants_dir = ROOT_DIR / "tenants"
        if tenants_dir.exists():
            for item in tenants_dir.iterdir():
                if item.is_dir() and not item.name.startswith("_"):
                    if (item / "config" / ".env").exists():
                        valid_tenants.append(item.name)
    return sorted(valid_tenants)

def format_duration(seconds):
    return str(timedelta(seconds=int(seconds)))

def run_nightly_batch():
    start_time = time.time()
    batch_date = datetime.now().strftime("%d/%m/%Y")
    
    log.info(f"🚀 INICIANDO BATERIA NOTURNA DETALHADA - {batch_date}")
    
    tenants = get_target_tenants()
    if not tenants:
        log.warning("⛔ Nenhum tenant para rodar.")
        return

    # Estrutura para guardar os relatórios individuais
    report_blocks = []
    
    global_stats = {
        "success": 0,
        "failure": 0,
        "total_rows": 0
    }

    for tenant_id in tenants:
        log.info(f"▶️  Processando: {tenant_id}")
        tenant_start = time.time()
        
        try:
            # rows = total de linhas (int)
            # details = lista de tuplas [(job_name, inserted, updated), ...]
            # proc_names = lista de strings com os nomes das procedures executadas
            rows, details, proc_names = run_tenant_pipeline(tenant_id)
            duration = format_duration(time.time() - tenant_start)
            
            if rows == -1:
                # --- CASO DE ERRO ---
                global_stats["failure"] += 1
                error_msg = details.get("error", "Erro desconhecido")
                log.error(f"❌ Falha em {tenant_id}: {error_msg}")
                
                block = (
                    f"❌ *{tenant_id}* (Falha)\n"
                    f"⏱️ `{duration}`\n"
                    f"💀 _Erro: {error_msg}_"
                )
                report_blocks.append(block)

            else:
                # --- CASO DE SUCESSO ---
                global_stats["success"] += 1
                global_stats["total_rows"] += rows
                log.info(f"✅ Sucesso em {tenant_id}: {rows} linhas")

                # 1. TRATAMENTO DOS JOBS (STAGING) -- VEM PRIMEIRO
                total_ins = 0
                total_upd = 0
                jobs_str = ""

                # details é uma lista de tuplas: (job_name, inserted, updated)
                # Vamos ordenar por volume (quem teve mais movimento aparece primeiro)
                sorted_jobs = sorted(details, key=lambda x: x[1] + x[2], reverse=True)

                for job_name, ins, upd in sorted_jobs:
                    total_ins += ins
                    total_upd += upd
                    # Só mostra o job se teve alguma movimentação para economizar espaço
                    if ins > 0 or upd > 0:
                        jobs_str += f"   ├─ `{job_name}`: 🟢+{ins} | 🔵~{upd}\n"
                
                # Se nenhum job teve dados, mostra msg padrão
                if not jobs_str:
                    jobs_str = "   (Sem novos dados)\n"

                # 2. TRATAMENTO DAS PROCEDURES (DW)
                procs_str = ""
                if proc_names:
                    for proc in proc_names:
                        procs_str += f"   ⚙️ `{proc}`\n"
                else:
                    procs_str = "   ⚠️ _Nenhuma procedure executada_\n"
                    
                # === MONTAGEM DO BLOCO ===
                block = (
                    f"✅ *{tenant_id}*\n"
                    f"⏱️ Duração: `{duration}`\n"
                    f"📂 *Carga Staging:*\n"
                    f"{jobs_str}"
                    f"   ━━━━━━━━━━━━━━━━\n"
                    f"   ∑ *Total Staging*: 🟢+{total_ins} | 🔵~{total_upd}"
                    f"🏗️ *Procedures (DW):*\n"
                    f"{procs_str}"
                )
                report_blocks.append(block)

        except Exception as e:
            global_stats["failure"] += 1
            log.critical(f"💥 Crash em {tenant_id}: {e}")
            block = f"💥 *{tenant_id}* (CRASH)\n⚠️ _{str(e)}_"
            report_blocks.append(block)

    # --- MONTAGEM DA MENSAGEM FINAL ---
    total_duration = format_duration(time.time() - start_time)
    header_icon = "⚠️" if global_stats["failure"] > 0 else "📊"
    
    # Cabeçalho
    final_msg = (
        f"{header_icon} *RELATÓRIO DE CARGA: {batch_date}*\n"
        f"⏱️ Tempo Total: {total_duration}\n"
        f"🏁 Sucessos: {global_stats['success']} | ❌ Falhas: {global_stats['failure']}\n"
        f"📉 Linhas Totais: {global_stats['total_rows']}\n\n"
        f"━━━━━━━━━━━━━━━━\n"
    )

    # Corpo (Junta todos os blocos dos clientes)
    final_msg += "\n\n".join(report_blocks)

    log.info("Enviando relatório detalhado para o Telegram...")
    
    # Envia (com parse_mode Markdown para ficar bonito)
    # Se a mensagem for muito grande (>4096 chars), o Telegram corta.
    # Por segurança, imprimimos no log também.
    log.info(final_msg) 
    
    send_telegram_text(final_msg, parse_mode="Markdown")
    
    log.info("💤 Bateria Noturna Finalizada.")

if __name__ == "__main__":
    run_nightly_batch()