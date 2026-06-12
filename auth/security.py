import bcrypt
from logger import logger
from db.connection import conectar_banco


def hash_senha(senha):
    try:
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(senha.encode("utf-8"), salt)
        # Garantir que retornamos string para armazenamento no banco
        return hashed.decode('utf-8')
    except Exception as e:
        logger.error(f"Erro ao gerar hash da senha: {e}")
        raise


def verificar_senha(senha, hash_armazenado):
    try:
        print(f"DEBUG SECURITY: Verificando senha: '{senha}'")
        print(f"DEBUG SECURITY: Hash armazenado: '{hash_armazenado}'")
        
        if not hash_armazenado:
            print("DEBUG SECURITY: Hash armazenado está vazio")
            return False

        if isinstance(hash_armazenado, str):
            hash_armazenado = hash_armazenado.strip()
            if not hash_armazenado:
                print("DEBUG SECURITY: Hash armazenado está vazio após trim")
                return False
            hash_armazenado = hash_armazenado.encode('utf-8')
        
        # A senha também precisa estar em bytes
        senha_bytes = senha.encode('utf-8')
        
        print(f"DEBUG SECURITY: Hash em bytes: {hash_armazenado}")
        print(f"DEBUG SECURITY: Senha em bytes: {senha_bytes}")
        
        resultado = bcrypt.checkpw(senha_bytes, hash_armazenado)
        print(f"DEBUG SECURITY: Resultado do checkpw: {resultado}")
        
        return resultado
    except Exception as e:
        logger.error(f"Erro ao verificar senha: {e}")
        print(f"DEBUG SECURITY: Exception: {e}")
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