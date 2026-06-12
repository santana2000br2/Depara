import os
from dotenv import load_dotenv

load_dotenv()


class WindowsConfig:
    SECRET_KEY = os.getenv("SECRET_KEY", "chave-secreta-padrao-windows")
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"

    # Configurações de Banco para Windows
    DB_SERVER = os.getenv("DB_SERVER", "localhost")
    DB_NAME = os.getenv("DB_NAME", "DeXPara")
    DB_USER = os.getenv("DB_USER", "sa")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "sua-senha")

    # Configurações específicas do Windows
    LOG_PATH = os.getenv("LOG_PATH", ".\\logs\\")
    UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", ".\\uploads\\")

    @property
    def SQLALCHEMY_DATABASE_URI(self):
        return f"mssql+pyodbc://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_SERVER}/{self.DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
