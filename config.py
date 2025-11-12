import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Configurações do banco de dados principal (autenticação)
    DB1_SERVER = os.getenv("DB1_SERVER", "localhost")
    DB1_NAME = os.getenv("DB1_NAME", "depara")
    DB1_USER = os.getenv("DB1_USER", "sa")
    DB1_PASSWORD = os.getenv("DB1_PASSWORD", "sua_senha")
    DB1_TIMEOUT = int(os.getenv("DB1_TIMEOUT", "30"))
    
    # Configurações do banco de dados do cliente
    DB2_SERVER = os.getenv("DB2_SERVER", "localhost")
    DB2_USER = os.getenv("DB2_USER", "sa")
    DB2_PASSWORD = os.getenv("DB2_PASSWORD", "sua_senha")
    DB2_TIMEOUT = int(os.getenv("DB2_TIMEOUT", "30"))
    
    # Outras configurações
    SECRET_KEY = os.getenv("SECRET_KEY", "chave-secreta-padrao")
    UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "uploads")
    MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH", "16777216"))  # 16MB
    
    # Configurações de Log
    LOG_FILE = os.getenv("LOG_FILE", "logs/app.log")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")