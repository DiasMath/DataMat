# 🔐 Módulo de Autenticação (OAuth2)

Este módulo gerencia a segurança e o ciclo de vida dos tokens de acesso para as APIs (Bling, Tiny, etc).

## 🧠 Como Funciona

O `OAuth2Client` implementa o fluxo **Authorization Code Grant** de forma automatizada para ambientes de backend/ETL.

1.  **Code Exchange:** Troca o código de autorização manual pelo primeiro par de tokens.
2.  **Auto Refresh:** Verifica a validade do token a cada chamada.
3.  **Safety Buffer:** Renova o token automaticamente **5 minutos antes** de expirar, para evitar falhas durante extrações longas.
4.  **Persistência:** Salva os tokens criptografados/serializados em `.secrets/tokens.json`.

## 📂 Estrutura de Arquivos

* **`oauth2_client.py`**: A classe principal (o "motor").
* **`.secrets/`**: Pasta onde os tokens JSON são salvos localmente.

## ⚠️ Dependências

Este módulo exige as seguintes variáveis no `.env` do tenant:

* `OAUTH_CLIENT_ID`
* `OAUTH_CLIENT_SECRET`
* `OAUTH_TOKEN_URL`
* `OAUTH_REDIRECT_URI`

## 🚀 Como gerar o Primeiro Token?

Este módulo **não** roda sozinho. Para gerar o token inicial (que exige abrir o navegador), utilize a suíte de testes:

```bash
# Na raiz do projeto:
python -m core.tests.run_tests <CLIENT_ID> --test complete