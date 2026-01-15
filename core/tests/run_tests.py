#!/usr/bin/env python3
"""
Orquestrador de testes DataMat.
Centraliza a execução de verificações de ambiente, autenticação e simulação.
"""
from __future__ import annotations
import sys
import argparse
from pathlib import Path

# Garante que a raiz do projeto esteja no sys.path para imports absolutos funcionarem
# mesmo se o script for chamado de dentro da pasta tests.
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

def run_test(test_name: str, client_id: str) -> bool:
    """Executa um teste específico baseado no nome."""
    print(f"\n{'='*60}")
    print(f"🧪 [RUNNER] Iniciando teste: {test_name.upper()}")
    print(f"{'='*60}")
    
    try:
        if test_name == "status":
            from core.tests.check_token_status import check_token_status
            # Esse teste é visual (prints). Se não quebrar, consideramos sucesso.
            check_token_status(client_id)
            return True
            
        elif test_name == "complete":
            from core.tests.test_bling_oauth2 import test_oauth2_config, test_bling_api_call
            
            print("   -> Passo 1: Verificando configurações...")
            if not test_oauth2_config(client_id):
                print("❌ Falha nas configurações.")
                return False
                
            print("   -> Passo 2: Testando chamada de API real...")
            return test_bling_api_call(client_id)
            
        elif test_name == "simulate":
            from core.tests.simulate_daily_execution import simulate_daily_execution
            # Simulação de extração. Se rodar até o fim sem exceção, passou.
            simulate_daily_execution(client_id)
            return True
        
        elif test_name == "alerts":
            from core.tests.test_alerts import test_alerts_integration
            return test_alerts_integration(client_id)
            
        else:
            print(f"❌ Teste desconhecido: {test_name}")
            return False
            
    except ImportError as e:
        print(f"❌ Erro de Importação (verifique se os arquivos existem): {e}")
        return False
    except Exception as e:
        print(f"❌ Erro CRÍTICO ao executar {test_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_all_tests(client_id: str) -> dict:
    """Executa a bateria completa de testes."""
    print(f"🚀 Executando BATERIA COMPLETA para: {client_id}")
    
    tests = [
        ("status", "Verificação de Status do Token"),
        ("alerts", "Verificação de Envio de Alertas"),
        ("complete", "Teste de Fluxo OAuth2 (Refresh/API)"),
        ("simulate", "Simulação de Extração (Adapter)")
    ]
    
    results = {}
    
    for test_key, description in tests:
        print(f"\n📋 {description}...")
        success = run_test(test_key, client_id)
        results[test_key] = success
        
        # Se um teste crítico falhar, talvez queiramos parar? 
        # Por enquanto, vamos rodar tudo para diagnóstico completo.
    
    return results

def print_summary(results: dict):
    """Imprime o relatório final."""
    print(f"\n{'='*60}")
    print("📊 RELATÓRIO FINAL DE EXECUÇÃO")
    print(f"{'='*60}")
    
    passed = sum(1 for success in results.values() if success)
    total = len(results)
    
    for test_name, success in results.items():
        status_icon = "✅ PASSOU" if success else "❌ FALHOU"
        print(f"  {test_name.ljust(15)}: {status_icon}")
    
    print(f"\n🎯 Placar: {passed}/{total}")
    
    if passed == total:
        print(f"🎉 SUCESSO TOTAL! O ambiente de {sys.argv[1] if len(sys.argv)>1 else 'teste'} está saudável.")
    else:
        print("⚠️  ATENÇÃO: Existem falhas. Revise os logs acima.")

def main():
    parser = argparse.ArgumentParser(description="Executor de Testes do DataMat")
    parser.add_argument("client_id", help="ID do cliente (ex: HASHTAG, LOJAJUNTOS)")
    parser.add_argument(
        "--test", 
        choices=["status", "complete", "simulate", "alerts", "all"],
        default="all",
        help="Qual teste executar (padrão: all)"
    )
    
    args = parser.parse_args()
    
    if args.test == "all":
        results = run_all_tests(args.client_id)
        print_summary(results)
        sys.exit(0 if all(results.values()) else 1)
    else:
        success = run_test(args.test, args.client_id)
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()