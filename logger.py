import logging
import os
from config import Config

def setup_logger():
    """
    Configura o sistema de logging da aplicação
    """
    try:
        # Garante que o diretório de logs existe
        log_dir = os.path.dirname(Config.LOG_FILE)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # Configura o logger
        logger = logging.getLogger('auth')
        logger.setLevel(getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO))
        
        # Evita duplicação de handlers
        if logger.handlers:
            logger.handlers.clear()
        
        # Formato do log
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para arquivo
        file_handler = logging.FileHandler(Config.LOG_FILE, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
        
        # Adiciona os handlers ao logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
        
    except Exception as e:
        # Fallback para um logger básico se houver erro na configuração
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        fallback_logger = logging.getLogger('auth_fallback')
        fallback_logger.error(f"Erro ao configurar logger: {e}")
        return fallback_logger

# Inicializa o logger
logger = setup_logger()