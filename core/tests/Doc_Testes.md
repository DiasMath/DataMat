# 🧪 DataMat - Suíte de Testes e Validação de Ambiente

Este diretório contém a bateria de testes automatizados para o ecossistema DataMat.
O objetivo desta suíte é garantir a integridade das conexões (OAuth2), validar credenciais e simular extrações de dados reais antes de executar pipelines em produção.

---

## 🚀 Como Executar

A execução é centralizada pelo orquestrador `run_tests.py`.

> **⚠️ Importante:** Todos os comandos devem ser executados a partir da **raiz do projeto** (pasta acima de `core`) para garantir que os imports do Python funcionem corretamente.

### Sintaxe Básica

```bash
  python -m core.tests.run_tests <CLIENT_ID> --test <MODO>
```

Exemplos de Uso
1. Bateria Completa (Recomendado)
Executa todos os testes sequencialmente: verifica token, testa fluxo OAuth2 completo e simula extração.

```bash 
  python -m core.tests.run_tests HASHTAG --test all
```

2. Verificar Status do Token
Verifica se o arquivo de token existe e calcula o tempo restante de vida (TTL) sem fazer chamadas de rede. Útil para checagens rápidas.

`python -m core.tests.run_tests HASHTAG --test status`

3. Teste de Autenticação (OAuth2)
Valida as variáveis de ambiente e força um ciclo completo de autenticação (pode abrir o navegador para login). Use isso para gerar um novo token do zero.

`python -m core.tests.run_tests HASHTAG --test complete`

4. Simulação de Extração (Adapter)
Carrega as configurações reais do cliente (jobs.py), instancia o APISourceAdapter com a configuração real e tenta baixar 5 registros da API. Valida se o parâmetro id_key e a conexão com a API estão funcionais.

  `python -m core.tests.run_tests HASHTAG --test simulate`

### 📋 Detalhamento dos Testes

Abaixo está a descrição técnica do que cada módulo de teste realiza:

Modo (--test)   Script Responsável              O que é testado?
`status`          `check_token_status.py`           Validação Passiva: • Localiza o arquivo de tokens em `.secrets/.`• Lê o JSON e verifica se `access_token` e `refresh_token` existem.• Calcula o tempo de expiração (expires_at) e alerta se estiver vencido.

`complete`        `test_bling_oauth2.py`            Validação de Autenticação:• Verifica se `OAUTH_CLIENT_ID`, `SECRET` e `REDIRECT_URI` estão no `.env`.• Inicia o fluxo OAuth2: Abre navegador → Captura Code → Troca por Token.• Testa o Refresh Token para garantir que a renovação automática funciona.

`simulate`        `simulate_daily_execution.py`     Validação de Integração:• Carrega dinamicamente os Jobs do cliente `(tenants.<ID>.pipelines.jobs)`.• Seleciona o primeiro Job do tipo 'api'.• Instancia o APISourceAdapter com a configuração real.• Executa `extract_raw()` com limite de 5 linhas.• Valida se o parâmetro id_key e a conexão com a API estão funcionais.

`alerts`	        `test_alerts.py`	                🔔 Validação de Notificações: Envia mensagens reais de teste (Sucesso e Falha Simulada) para o Telegram/Email configurados no .env, validando credenciais e formatação.


### ⚙️ Pré-requisitos e Configuração

Para que os testes funcionem, a estrutura de arquivos deve estar correta:

1. Estrutura de Pastas Esperada

projeto_raiz/
├── .env                  # Configurações Globais (DB, etc)
├── core/
│   ├── adapters/         # Código fonte dos adapters
│   └── tests/            # Esta pasta de testes
└── tenants/
    └── HASHTAG/          # ID do Cliente (exemplo)
        ├── config/
        │   └── .env      # Configurações Específicas (OAuth) [OBRIGATÓRIO]
        └── pipelines/
            └── jobs.py   # Definição dos Jobs

2. Variáveis de Ambiente Necessárias

No arquivo `tenants/<ID>/config/.env`, as seguintes variáveis são obrigatórias para testes de API (Ex: Bling/Tiny):

OAUTH_CLIENT_ID=seu_client_id
OAUTH_CLIENT_SECRET=seu_client_secret
OAUTH_REDIRECT_URI=http://localhost:8080/callback
OAUTH_AUTH_URL=[https://www.bling.com.br/Api/v3/oauth/authorize](https://www.bling.com.br/Api/v3/oauth/authorize)
OAUTH_TOKEN_URL=[https://www.bling.com.br/Api/v3/oauth/token](https://www.bling.com.br/Api/v3/oauth/token)
OAUTH_SCOPE=bling:nfe:read bling:vendas:read ... 
API_BASE_URL=[https://www.bling.com.br/Api/v3](https://www.bling.com.br/Api/v3)


### 🛠️ Solução de Problemas

🔴 Erro: ModuleNotFoundError: No module named 'core'

- Causa: Você tentou rodar o script de dentro da pasta `core/tests` ou usou `python run_tests.py` direto.Solução: Volte para a raiz do projeto e execute como módulo:`python -m core.tests.run_tests` ...

🔴 Erro: Arquivo de token NÃO ENCONTRADO 

- Causa: O teste `status` ou `simulate` foi rodado antes de haver um login válido.Solução: Execute primeiro o teste de autenticação para gerar as credenciais: `python -m core.tests.run_tests <CLIENT_ID> --test complete`

🔴 Erro: ConnectionRefusedError ou Browser não abre

- Causa: O script tenta abrir o navegador e subir um servidor local na porta 8080 (ou a porta definida no seu Redirect URI).Solução: Verifique se a `OAUTH_REDIRECT_URI` no `.env` bate com a porta disponível na sua máquina e se ela está cadastrada no aplicativo da API (Bling/Tiny).

🔴 Erro: Falha na coerção de tipos (Logs de Warning) 

- Causa: Durante a simulação (`simulate`), o APISourceAdapter pode trazer dados que não batem 100% com o esperado pelo banco, mas isso geralmente é tratado pelo `datamat.py` na etapa de carga.Solução: Se o teste terminar com "✅ Simulação de extração concluída", ignore os warnings. O teste foca na extração, não na carga.

Nota do Desenvolvedor: Esta suíte de testes deve ser executada sempre que houver alteração nas credenciais de um cliente ou atualização no código core.
