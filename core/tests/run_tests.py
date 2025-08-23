#!/usr/bin/env python3
"""
Orquestrador de testes OAuth2 do Bling.
Seguindo boas práticas de organização de testes.
"""
from __future__ import annotations
import os, sys, argparse
from pathlib import Path

def run_test(test_name: str, client_id: str) -> bool:
    """Executa um teste específico."""
    print(f"\n{'='*60}")
    print(f"🧪 Executando: {test_name}")
    print(f"{'='*60}")
    
    try:
        if test_name == "status":
            from .check_token_status import check_token_status
            check_token_status(client_id)
            return True
            
        elif test_name == "complete":
            from .test_bling_oauth2 import test_oauth2_config, test_bling_api_call
            config_ok = test_oauth2_config(client_id)
            if config_ok:
                api_ok = test_bling_api_call(client_id)
                return api_ok
            return False
            
        elif test_name == "simulate":
            from .simulate_daily_execution import simulate_daily_execution
            simulate_daily_execution(client_id)
            return True
            
        else:
            print(f"❌ Teste desconhecido: {test_name}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar {test_name}: {e}")
        return False

def run_all_tests(client_id: str) -> dict:
    """Executa todos os testes e retorna resultados."""
    print(f"🚀 Executando todos os testes para {client_id}")
    
    tests = [
        ("status", "Verificação de Status"),
        ("complete", "Teste Completo"),
        ("simulate", "Simulação Diária")
    ]
    
    results = {}
    
    for test_name, description in tests:
        print(f"\n📋 {description}")
        success = run_test(test_name, client_id)
        results[test_name] = success
        
        if success:
            print(f"✅ {description}: PASSOU")
        else:
            print(f"❌ {description}: FALHOU")
    
    return results

def print_summary(results: dict):
    """Imprime resumo dos resultados."""
    print(f"\n{'='*60}")
    print(f"📊 RESUMO DOS TESTES")
    print(f"{'='*60}")
    
    passed = sum(1 for success in results.values() if success)
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASSOU" if success else "❌ FALHOU"
        print(f"  {test_name}: {status}")
    
    print(f"\n🎯 Resultado: {passed}/{total} testes passaram")
    
    if passed == total:
        print(f"🎉 Todos os testes passaram! Sistema pronto para produção.")
    else:
        print(f"⚠️  Alguns testes falharam. Verifique a configuração.")

def main():
    parser = argparse.ArgumentParser(
        description="Orquestrador de testes OAuth2 do Bling",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Executar todos os testes
  python -m core.tests.run_tests LOJAJUNTOS

  # Executar apenas verificação de status
  python -m core.tests.run_tests LOJAJUNTOS --test status

  # Executar teste completo
  python -m core.tests.run_tests LOJAJUNTOS --test complete

  # Executar simulação diária
  python -m core.tests.run_tests LOJAJUNTOS --test simulate

  # Executar múltiplos testes específicos
  python -m core.tests.run_tests LOJAJUNTOS --test status,complete
        """
    )
    
    parser.add_argument("client_id", help="ID do cliente (ex: LOJAJUNTOS)")
    parser.add_argument(
        "--test", 
        choices=["status", "complete", "simulate", "all"],
        default="all",
        help="Teste específico a executar (padrão: all)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Modo verboso"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"🔍 Modo verboso ativado")
        print(f"📋 Cliente: {args.client_id}")
        print(f"🧪 Teste: {args.test}")
    
    # Executar testes
    if args.test == "all":
        results = run_all_tests(args.client_id)
        print_summary(results)
        
        # Retornar código de saída baseado nos resultados
        if all(results.values()):
            sys.exit(0)  # Sucesso
        else:
            sys.exit(1)  # Falha
            
    else:
        # Executar teste específico
        success = run_test(args.test, args.client_id)
        if success:
            print(f"\n✅ Teste '{args.test}' passou!")
            sys.exit(0)
        else:
            print(f"\n❌ Teste '{args.test}' falhou!")
            sys.exit(1)

if __name__ == "__main__":
    main()
