import pyodbc
from config import Config
from logger import logger
from flask import session

def conectar_banco():
    """Conecta ao banco de dados principal (autenticação)"""
    try:
        return pyodbc.connect(
            Driver="{ODBC Driver 17 for SQL Server}",
            Server=Config.DB1_SERVER,
            Database=Config.DB1_NAME,
            UID=Config.DB1_USER,
            PWD=Config.DB1_PASSWORD,
            timeout=Config.DB1_TIMEOUT,
        )
    except Exception as e:
        logger.error(f"Erro de conexão: {e}")
        return None

def conectar_usuario():
    """Conecta ao banco de dados do projeto selecionado"""
    if "empresa_selecionada" not in session:
        return None

    empresa = session["empresa_selecionada"]
    banco = empresa.get("DadosGX")

    if not banco:
        return None

    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={Config.DB2_SERVER};"  
            f"DATABASE={banco};"
            f"UID={Config.DB2_USER};"
            f"PWD={Config.DB2_PASSWORD}"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        logger.error(f"Erro na conexão com banco do usuário: {e}")
        return None

def conectar_segunda_base(banco_nome):
    """Conecta a um banco específico"""
    try:
        return pyodbc.connect(
            Driver="{ODBC Driver 17 for SQL Server}",
            Server=Config.DB2_SERVER,
            Database=banco_nome,
            UID=Config.DB2_USER,
            PWD=Config.DB2_PASSWORD,
            timeout=Config.DB2_TIMEOUT,
        )
    except Exception as e:
        logger.error(f"Erro ao conectar base {banco_nome}: {e}")
        return None