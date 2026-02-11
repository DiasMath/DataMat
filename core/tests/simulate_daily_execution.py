from __future__ import annotations
import sys
import importlib
from .test_utils import load_test_envs

def simulate_daily_execution(client_id: str):
    print(f"🌅 Simulando execução diária (Extração) para {client_id}")
    load_test_envs(client_id)
    
    # Tenta importar os jobs do cliente dinamicamente
    try:
        module_path = f"tenants.{client_id}.pipelines.jobs"
        if module_path not in sys.modules:
            jobs_module = importlib.import_module(module_path)
        else:
            jobs_module = sys.modules[module_path]
            
        jobs = getattr(jobs_module, "JOBS", [])
    except ImportError as e:
        print(f"❌ Falha ao importar jobs de {client_id}: {e}")
        return

    # Filtra apenas jobs de API para teste
    api_jobs = [j for j in jobs if j.type == 'api']
    
    if not api_jobs:
        print("⚠️ Nenhum job do tipo 'api' encontrado para testar.")
        return

    # Pega o primeiro job para teste
    target_job = api_jobs[0]
    print(f"🧪 Job selecionado para teste: {target_job.name}")
    print(f"   -> Endpoint: {target_job.endpoint}")
    print(f"   -> ID Key: {getattr(target_job, 'id_key', 'id')}")

    try:
        # Importa o Adapter Real
        from core.adapters.api_adapter import APISourceAdapter
        
        # Configura o adapter usando os dados do Job (simulando o main.py)
        adapter = APISourceAdapter(
            endpoint=target_job.endpoint,
            id_key=getattr(target_job, 'id_key', 'id'), # Usa o campo novo que criamos
            auth=target_job.auth,
            paging=target_job.paging,
            max_passes=1, # Para teste, apenas 1 passagem
            row_limit=5 # Baixar apenas 5 registros para ser rápido
        )
        
        print("\n🚀 Iniciando extração de teste (Limit=5)...")
        data = adapter.extract_raw()
        
        print("\n📊 Resultado da Extração:")
        print(f"   -> Registros baixados: {len(data)}")
        
        if len(data) > 0:
            print(f"   -> Exemplo de registro: {data[0]}")
            print("✅ Simulação de extração concluída com SUCESSO.")
        else:
            print("⚠️ Extração retornou lista vazia (verifique se há dados na origem).")

    except Exception as e:
        print(f"❌ Erro crítico na simulação: {e}")
        import traceback
        traceback.print_exc()