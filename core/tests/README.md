# Testes OAuth2 Bling

Esta pasta contém testes para validar a configuração OAuth2 do Bling, organizados seguindo boas práticas.

## 🎯 **Estrutura de Testes**

### **Orquestrador Principal**
- **`run_tests.py`** - Orquestrador que executa todos os testes ou testes específicos

### **Testes Individuais**
- **`check_token_status.py`** - Verificação rápida de status dos tokens
- **`test_bling_oauth2.py`** - Teste completo (configuração + API)
- **`simulate_daily_execution.py`** - Simulação de execução diária

## 🚀 **Como Usar**

### **1. Executar Todos os Testes (Recomendado)**
```bash
python -m core.tests.run_tests LOJAJUNTOS
```

### **2. Executar Teste Específico**
```bash
# Verificação rápida de status
python -m core.tests.run_tests LOJAJUNTOS --test status

# Teste completo (configuração + API)
python -m core.tests.run_tests LOJAJUNTOS --test complete

# Simulação de execução diária
python -m core.tests.run_tests LOJAJUNTOS --test simulate

# Health check completo
python -m core.tests.run_tests LOJAJUNTOS --test health
```

### **3. Modo Verboso**
```bash
python -m core.tests.run_tests LOJAJUNTOS --verbose
```

## 📋 **Descrição dos Testes**

### **`status` - Verificação de Status**
- ✅ **Rápido e seguro** (não faz chamadas à API)
- ✅ Verifica configuração OAuth2
- ✅ Mostra status dos tokens
- ✅ Tempo restante até expiração
- **Uso**: Verificação diária, troubleshooting

### **`complete` - Teste Completo**
- ✅ Verifica configuração OAuth2
- ✅ Valida cache de tokens
- ✅ Faz chamada real para API do Bling
- ✅ Testa renovação automática
- **Uso**: Validação completa, antes de produção

### **`simulate` - Simulação Diária**
- ✅ Simula execução às 2h da manhã
- ✅ Verifica renovação automática
- ✅ Faz chamada real à API
- ✅ Mostra como funciona o sistema
- **Uso**: Demonstração, validação de fluxo

### **`health` - Health Check Completo**
- ✅ Verifica saúde de todos os componentes
- ✅ OAuth2, resiliência, métricas, ambiente
- ✅ Logs estruturados em JSON
- ✅ Status detalhado de cada componente
- **Uso**: Monitoramento, troubleshooting avançado

## 📊 **Saída dos Testes**

### **Execução Individual**
```
🧪 Executando: status
============================================================
🔍 Verificando status dos tokens para LOJAJUNTOS
📋 Status dos tokens:
  - Access Token: ✅ Presente
  - Refresh Token: ✅ Presente
  - Tempo restante: 0d 5h 45m (20700 segundos)
✅ Token válido para execução diária.

✅ Teste 'status' passou!
```

### **Execução Completa**
```
🚀 Executando todos os testes para LOJAJUNTOS

📋 Verificação de Status
✅ Verificação de Status: PASSOU

📋 Teste Completo
✅ Teste Completo: PASSOU

📋 Simulação Diária
✅ Simulação Diária: PASSOU

============================================================
📊 RESUMO DOS TESTES
============================================================
  status: ✅ PASSOU
  complete: ✅ PASSOU
  simulate: ✅ PASSOU

🎯 Resultado: 3/3 testes passaram
🎉 Todos os testes passaram! Sistema pronto para produção.
```

## 🔧 **Códigos de Saída**

- **0**: Todos os testes passaram
- **1**: Pelo menos um teste falhou

## 📈 **Boas Práticas Implementadas**

### **1. Separação de Responsabilidades**
- Cada teste tem uma função específica
- Orquestrador gerencia execução
- Código reutilizável

### **2. Flexibilidade**
- Executar todos os testes
- Executar teste específico
- Modo verboso para debug

### **3. Relatórios Claros**
- Status individual de cada teste
- Resumo final
- Códigos de saída padronizados

### **4. Manutenibilidade**
- Estrutura modular
- Fácil adicionar novos testes
- Documentação clara

## 🛠️ **Troubleshooting**

### **Erro: "Cache de tokens não encontrado"**
```bash
# Execute o setup OAuth2 primeiro
python scripts/oauth2_setup.py LOJAJUNTOS
```

### **Erro: "No module named 'pandas'"**
```bash
# Instale as dependências
pip install -r requirements.txt
```

### **Erro: "Variáveis faltando"**
Verifique se o arquivo `.env` do tenant está configurado corretamente:
```bash
# tenants/LOJAJUNTOS/config/.env
OAUTH_AUTH_URL=https://www.bling.com.br/Api/v3/oauth/authorize
OAUTH_TOKEN_URL=https://www.bling.com.br/Api/v3/oauth/token
OAUTH_CLIENT_ID=seu_client_id
OAUTH_CLIENT_SECRET=seu_client_secret
OAUTH_REDIRECT_URI=http://127.0.0.1:8910/callback
OAUTH_SCOPE=read
```

## 💡 **Recomendações de Uso**

### **Desenvolvimento**
```bash
# Verificação rápida
python -m core.tests.run_tests LOJAJUNTOS --test status
```

### **Validação Completa**
```bash
# Todos os testes
python -m core.tests.run_tests LOJAJUNTOS
```

### **CI/CD Pipeline**
```bash
# Com código de saída
python -m core.tests.run_tests LOJAJUNTOS --test complete
```
