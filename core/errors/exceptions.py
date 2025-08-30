"""
Módulo central para a definição de exceções customizadas da aplicação.

Ter um módulo dedicado para exceções ajuda a evitar dependências circulares
e mantém o código organizado, seguindo o Princípio da Responsabilidade Única.
"""

class DataMatError(Exception):
    """Exceção base para todos os erros controlados pela biblioteca DataMat."""
    pass

class DataExtractionError(DataMatError):
    """Ocorre durante a fase de extração de dados por um adapter."""
    pass

class DataValidationError(DataMatError):
    """Ocorre durante a fase de validação dos dados com Pandera."""
    pass

class DataLoadError(DataMatError):
    """Ocorre durante a carga dos dados no banco de dados."""
    pass