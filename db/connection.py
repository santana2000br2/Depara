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

def log_detalhes_conexao(server, database, user, password):
    """Loga detalhes da conexão (sem expor senha completa)"""
    logger.info("=" * 80)
    logger.info("DIAGNÓSTICO DE CONEXÃO")
    logger.info("=" * 80)
    logger.info(f"Servidor: {server}")
    logger.info(f"Banco: {database}")
    logger.info(f"Usuário: {user}")
    logger.info(f"Senha (primeiros 4 chars): {password[:4] if password else 'vazia'}...")
    logger.info(f"Tamanho da senha: {len(password) if password else 0}")
    logger.info("-" * 80)

def testar_conexao_pyodbc(server, database, user, password):
    """Testa conexão diretamente com pyodbc para diagnóstico"""
    conn = None
    cursor = None
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password}"
        logger.info(f"String de conexão: {conn_str.replace(password, '***')}")
        
        conn = pyodbc.connect(conn_str)
        logger.info("✅ Conexão bem-sucedida no teste direto")
        
        # Testar consulta simples
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()
        
        if version and version[0]:
            logger.info(f"Versão do SQL Server: {str(version[0])[:100]}...")
        else:
            logger.warning("⚠️ Consulta @@VERSION não retornou resultados")
        
        return True
    except Exception as e:
        logger.error(f"❌ Erro no teste direto: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False
    finally:
        try:
            if cursor:
                cursor.close()
        except:
            pass
        try:
            if conn:
                conn.close()
        except:
            pass

def conectar_segunda_base(banco_nome):
    """Conecta a um banco específico usando credenciais do projeto se disponíveis"""
    logger.info("=" * 80)
    logger.info(f"CHAMADA: conectar_segunda_base('{banco_nome}')")
    logger.info("=" * 80)
    
    # Verificar se há projeto na sessão
    if "projeto_selecionado" not in session:
        logger.warning("⚠️ NENHUM PROJETO NA SESSÃO!")
        logger.info(f"Session keys: {list(session.keys())}")
        server = Config.DB2_SERVER
        user = Config.DB2_USER
        password = Config.DB2_PASSWORD
        origem = "DEFAULT (sem projeto na sessão)"
    else:
        projeto = session["projeto_selecionado"]
        logger.info(f"📁 Projeto na sessão: {projeto.get('NomeProjeto')}")
        logger.info(f"📁 DadosGX do projeto: {projeto.get('DadosGX')}")
        logger.info(f"📁 Banco solicitado: {banco_nome}")
        
        # Log de TODOS os campos do projeto para diagnóstico
        logger.info("📋 CAMPOS DO PROJETO NA SESSÃO:")
        for key, value in projeto.items():
            if key.lower().find('senha') >= 0:
                logger.info(f"  {key}: {'*' * len(str(value)) if value else 'vazio'}")
            else:
                logger.info(f"  {key}: {value}")
        
        # Verificar se este banco é o DadosGX (banco de homologação)
        banco_homologacao = projeto.get("DadosGX")
        banco_producao = projeto.get("bancoProducao")
        
        if banco_nome == banco_homologacao:
            logger.info("🎯 Banco solicitado é o DadosGX (homologação) deste projeto")
            
            # Verificar credenciais de homologação
            servidor_homo = projeto.get("servidorhomologacao")
            usuario_homo = projeto.get("usuariohomologacao")
            senha_homo = projeto.get("senhahomologacao")
            
            logger.info(f"🔍 Credenciais de homologação:")
            logger.info(f"   - Servidor: '{servidor_homo}' (tipo: {type(servidor_homo)})")
            logger.info(f"   - Usuário: '{usuario_homo}' (tipo: {type(usuario_homo)})")
            logger.info(f"   - Senha configurada: {'SIM' if senha_homo else 'NÃO'}")
            
            # Verificar se TODOS os campos estão preenchidos
            campos_preenchidos = all([servidor_homo, usuario_homo, senha_homo])
            logger.info(f"   - Todos campos preenchidos: {'SIM' if campos_preenchidos else 'NÃO'}")
            
            if campos_preenchidos:
                # Usar credenciais específicas de homologação do projeto
                server = servidor_homo
                user = usuario_homo
                password = senha_homo
                origem = "CREDENCIAIS ESPECÍFICAS DO PROJETO (homologação)"
                logger.info("✅ Usando credenciais específicas do projeto")
            else:
                # Usar credenciais padrão do .env
                server = Config.DB2_SERVER
                user = Config.DB2_USER
                password = Config.DB2_PASSWORD
                origem = "DEFAULT (credenciais de homologação incompletas)"
                logger.warning("⚠️ Usando credenciais padrão - campos de homologação incompletos")
        
        elif banco_nome == banco_producao:
            logger.info("🎯 Banco solicitado é o de produção deste projeto")
            # Verificar credenciais de produção
            servidor_prod = projeto.get("servidorproducao")
            usuario_prod = projeto.get("usuarioProducao")
            senha_prod = projeto.get("senhaproducao")
            
            logger.info(f"🔍 Credenciais de produção:")
            logger.info(f"   - Servidor: '{servidor_prod}'")
            logger.info(f"   - Usuário: '{usuario_prod}'")
            logger.info(f"   - Senha configurada: {'SIM' if senha_prod else 'NÃO'}")
            
            campos_preenchidos = all([servidor_prod, usuario_prod, senha_prod])
            logger.info(f"   - Todos campos preenchidos: {'SIM' if campos_preenchidos else 'NÃO'}")
            
            if campos_preenchidos:
                # Usar credenciais específicas de produção do projeto
                server = servidor_prod
                user = usuario_prod
                password = senha_prod
                origem = "CREDENCIAIS ESPECÍFICAS DO PROJETO (produção)"
                logger.info("✅ Usando credenciais específicas do projeto (produção)")
            else:
                # Usar credenciais padrão do .env
                server = Config.DB2_SERVER
                user = Config.DB2_USER
                password = Config.DB2_PASSWORD
                origem = "DEFAULT (credenciais de produção incompletas)"
                logger.warning("⚠️ Usando credenciais padrão - campos de produção incompletos")
        
        else:
            logger.warning(f"⚠️ Banco '{banco_nome}' não identificado como DadosGX ou produção")
            server = Config.DB2_SERVER
            user = Config.DB2_USER
            password = Config.DB2_PASSWORD
            origem = "DEFAULT (banco não identificado)"
    
    # Logar detalhes da conexão
    log_detalhes_conexao(server, banco_nome, user, password)
    logger.info(f"Origem das credenciais: {origem}")
    
    # Testar conexão antes de retornar
    logger.info("🧪 TESTANDO CONEXÃO...")
    teste_ok = testar_conexao_pyodbc(server, banco_nome, user, password)
    
    if not teste_ok:
        logger.error("❌ TESTE DE CONEXÃO FALHOU!")
        return None
    
    # Se o teste passou, criar a conexão real
    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={banco_nome};UID={user};PWD={password}"
        logger.info(f"🔗 Criando conexão real...")
        conn = pyodbc.connect(conn_str)
        logger.info("✅ CONEXÃO ESTABELECIDA COM SUCESSO!")
        return conn
    except Exception as e:
        logger.error(f"❌ Erro ao criar conexão: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

def conectar_usuario():
    """Conecta ao banco de dados do projeto selecionado"""
    logger.info("=" * 80)
    logger.info("CHAMADA: conectar_usuario()")
    logger.info("=" * 80)
    
    if "projeto_selecionado" not in session:
        logger.error("❌ Nenhum projeto selecionado na sessão")
        return None

    projeto = session["projeto_selecionado"]
    banco = projeto.get("DadosGX")
    
    if not banco:
        logger.error("❌ Banco DadosGX não configurado no projeto")
        return None
    
    logger.info(f"📁 Conectar usuário ao banco: {banco}")
    
    # Usar a função conectar_segunda_base que já tem toda a lógica
    return conectar_segunda_base(banco)

def conectar_homologacao():
    """Conecta ao banco de dados de homologação (DadosGX) usando credenciais do projeto"""
    logger.info("=" * 80)
    logger.info("CHAMADA: conectar_homologacao()")
    logger.info("=" * 80)
    
    if "projeto_selecionado" not in session:
        logger.error("❌ Nenhum projeto selecionado na sessão")
        return None

    projeto = session["projeto_selecionado"]
    banco = projeto.get("DadosGX")
    
    if not banco:
        logger.error("❌ Banco DadosGX não configurado no projeto")
        return None
    
    logger.info(f"📁 Conectar homologação ao banco: {banco}")
    
    # Usar a função conectar_segunda_base que já tem toda a lógica
    return conectar_segunda_base(banco)

def conectar_producao():
    """Conecta ao banco de dados de produção usando credenciais do projeto"""
    logger.info("=" * 80)
    logger.info("CHAMADA: conectar_producao()")
    logger.info("=" * 80)
    
    if "projeto_selecionado" not in session:
        logger.error("❌ Nenhum projeto selecionado na sessão")
        return None

    projeto = session["projeto_selecionado"]
    banco = projeto.get("bancoProducao")
    
    if not banco:
        logger.error("❌ Banco de produção não configurado no projeto")
        return None
    
    logger.info(f"📁 Conectar produção ao banco: {banco}")
    
    # Usar a função conectar_segunda_base que já tem toda a lógica
    return conectar_segunda_base(banco)

def conectar_banco_por_nome(banco_nome, tipo="homologacao"):
    """
    Conecta a um banco específico usando credenciais apropriadas
    
    Args:
        banco_nome: Nome do banco de dados
        tipo: "homologacao" (padrão) ou "producao"
    """
    logger.info("=" * 80)
    logger.info(f"CHAMADA: conectar_banco_por_nome('{banco_nome}', '{tipo}')")
    logger.info("=" * 80)
    
    if "projeto_selecionado" not in session:
        logger.warning("⚠️ Nenhum projeto na sessão, usando credenciais padrão")
        server = Config.DB2_SERVER
        user = Config.DB2_USER
        password = Config.DB2_PASSWORD
    else:
        projeto = session["projeto_selecionado"]
        
        if tipo == "producao":
            # Usar credenciais de produção
            servidor = projeto.get("servidorproducao", Config.DB2_SERVER)
            usuario = projeto.get("usuarioProducao", Config.DB2_USER)
            senha = projeto.get("senhaproducao", Config.DB2_PASSWORD)
            origem = f"PRODUÇÃO do projeto {projeto.get('NomeProjeto')}"
        else:  # homologacao
            # Usar credenciais de homologação
            servidor = projeto.get("servidorhomologacao", Config.DB2_SERVER)
            usuario = projeto.get("usuariohomologacao", Config.DB2_USER)
            senha = projeto.get("senhahomologacao", Config.DB2_PASSWORD)
            origem = f"HOMOLOGAÇÃO do projeto {projeto.get('NomeProjeto')}"
        
        server = servidor
        user = usuario
        password = senha
    
    logger.info(f"Origem: {origem}")
    logger.info(f"Servidor: {server}")
    logger.info(f"Usuário: {user}")
    
    try:
        conn = pyodbc.connect(
            Driver="{ODBC Driver 17 for SQL Server}",
            Server=server,
            Database=banco_nome,
            UID=user,
            PWD=password,
            timeout=Config.DB2_TIMEOUT,
        )
        logger.info(f"✅ Conexão bem-sucedida com {banco_nome}")
        return conn
    except Exception as e:
        logger.error(f"❌ Erro ao conectar base {banco_nome} ({tipo}): {e}")
        return None