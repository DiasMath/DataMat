"""
Módulo central para a definição de exceções customizadas da aplicação.
Organiza os erros por categorias para facilitar o tratamento e logs.
"""

class DataMatError(Exception):
    """
    Exceção base para todos os erros controlados pela biblioteca DataMat.
    Permite capturar qualquer erro da aplicação com 'except DataMatError'.
    """
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(message)
        self.original_error = original_error

# --- Erros de Infraestrutura e Configuração ---

class ConfigurationError(DataMatError):
    """
    Levantado quando há problemas no .env, settings ou jobs.py.
    Ex: Variável OAUTH_CLIENT_ID faltando, ou merge_mode inválido.
    """
    pass

class AuthenticationError(DataMatError):
    """
    Levantado quando falha o processo de OAuth, Refresh Token ou Permissões.
    Diferencia erros de acesso (401/403) de erros de extração (500/404).
    """
    pass

# --- Erros de Processamento de Dados ---

class DataExtractionError(DataMatError):
    """
    Ocorre durante a fase de extração (API ou Banco de Origem).
    Ex: Timeout na API, JSON malformado, endpoint não encontrado.
    """
    pass

class DataValidationError(DataMatError):
    """
    Ocorre durante a fase de validação (ex: Pandera).
    Ex: Coluna obrigatória faltando, tipo de dado incompatível.
    """
    pass

class DataLoadError(DataMatError):
    """
    Ocorre durante a carga (Load) no banco de dados de destino.
    Ex: Violação de Unique Key, erro de conexão SQL, falha no IODKU.
    """
    pass