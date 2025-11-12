import bcrypt
from logger import logger
from db.connection import conectar_banco


def hash_senha(senha):
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(senha.encode("utf-8"), salt)


def verificar_senha(senha, hash_armazenado):
    try:
        if not hash_armazenado:
            return False
        if isinstance(hash_armazenado, str):
            hash_armazenado = hash_armazenado.strip().encode("utf-8")
        return bcrypt.checkpw(senha.encode("utf-8"), hash_armazenado)
    except Exception as e:
        logger.error(f"Erro ao verificar senha: {e}")
        return False


def migrar_para_hash(username, senha):
    conexao = conectar_banco()
    if not conexao:
        return
    try:
        cursor = conexao.cursor()
        cursor.execute(
            "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'Usuarios' AND COLUMN_NAME = 'SenhaHash') "
            "ALTER TABLE Usuarios ADD SenhaHash VARCHAR(255)"
        )
        senha_hash = hash_senha(senha)
        cursor.execute(
            "UPDATE Usuarios SET SenhaHash = ? "
            "WHERE Usuario = ? AND (SenhaHash IS NULL OR SenhaHash = '')",
            (senha_hash, username),
        )
        conexao.commit()
    except Exception as e:
        logger.error(f"Erro ao migrar senha: {e}")
    finally:
        conexao.close()
