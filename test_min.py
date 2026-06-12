import hashlib
from db.connection import conectar_banco
from auth.security import hash_senha
from logger import logger

def migrar_senhas_md5_para_bcrypt():
    """
    Migra todas as senhas de MD5 para Bcrypt
    """
    conn = conectar_banco()
    if not conn:
        print("ERRO: Não foi possível conectar ao banco")
        return
    
    cursor = conn.cursor()
    
    try:
        # Buscar todos os usuários com senha MD5
        cursor.execute("SELECT UsuarioID, UsuarioNome, SenhaHash FROM Usuarios")
        usuarios = cursor.fetchall()
        
        migrados = 0
        problemas = 0
        
        for usuario in usuarios:
            usuario_id, usuario_nome, senha_hash = usuario
            
            # Verificar se é MD5 (32 caracteres hexadecimais)
            if senha_hash and len(senha_hash) == 32 and all(c in '0123456789abcdef' for c in senha_hash.lower()):
                print(f"Migrando usuário: {usuario_nome} (ID: {usuario_id})")
                print(f"Hash MD5 atual: {senha_hash}")
                
                # NÃO podemos converter MD5 para bcrypt diretamente
                # Precisamos definir uma senha padrão temporária
                senha_temporaria = "temp123"  # Senha temporária
                
                # Gerar hash bcrypt
                novo_hash = hash_senha(senha_temporaria)
                
                # Atualizar no banco
                cursor.execute(
                    "UPDATE Usuarios SET SenhaHash = ? WHERE UsuarioID = ?",
                    (novo_hash, usuario_id)
                )
                
                print(f"Novo hash bcrypt: {novo_hash[:50]}...")
                print("---")
                migrados += 1
                
                # Registrar para notificar o usuário
                logger.info(f"Usuário {usuario_nome} migrado para senha temporária")
        
        conn.commit()
        print(f"\n✅ Migração concluída: {migrados} usuários migrados")
        
        if migrados > 0:
            print("\n⚠️  AVISOS IMPORTANTES:")
            print("1. Todos os usuários migrados receberam a senha temporária: 'temp123'")
            print("2. Os usuários devem alterar suas senhas no primeiro login")
            print("3. Notifique todos os usuários sobre a mudança")
        
    except Exception as e:
        logger.error(f"Erro na migração: {e}")
        conn.rollback()
        print(f"❌ Erro na migração: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("Iniciando migração de senhas MD5 para Bcrypt...")
    migrar_senhas_md5_para_bcrypt()