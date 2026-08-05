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


def buscar_credenciais_projeto(projeto_id):
    """Busca credenciais de conexão no banco (senhas descriptografadas em memória)."""
    if not projeto_id:
        return None
    from utils.credential_crypto import descriptografar_segredo

    conn = conectar_banco()
    if not conn:
        return None
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                ProjetoID,
                NomeProjeto,
                DadosGX,
                servidorproducao,
                usuarioProducao,
                senhaproducao,
                servidorhomologacao,
                usuariohomologacao,
                senhahomologacao,
                BancoHomo,
                bancoProducao
            FROM Projeto
            WHERE ProjetoID = ?
        """, (projeto_id,))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "ProjetoID": row.ProjetoID,
            "NomeProjeto": row.NomeProjeto,
            "DadosGX": row.DadosGX,
            "servidorproducao": row.servidorproducao,
            "usuarioProducao": descriptografar_segredo(row.usuarioProducao),
            "senhaproducao": descriptografar_segredo(row.senhaproducao),
            "servidorhomologacao": row.servidorhomologacao,
            "usuariohomologacao": descriptografar_segredo(row.usuariohomologacao),
            "senhahomologacao": descriptografar_segredo(row.senhahomologacao),
            "BancoHomo": row.BancoHomo,
            "bancoProducao": row.bancoProducao,
        }
    except Exception as e:
        logger.error(f"Erro ao buscar credenciais do projeto {projeto_id}: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def obter_credenciais_producao(projeto_id):
    """Credenciais de produção (senha já em texto claro para conexão)."""
    try:
        creds = buscar_credenciais_projeto(projeto_id)
        if not creds or not creds.get("bancoProducao"):
            return None
        return {
            "banco": creds.get("bancoProducao"),
            "usuario": creds.get("usuarioProducao"),
            "senha": creds.get("senhaproducao"),
            "servidor": creds.get("servidorproducao"),
        }
    except Exception as e:
        logger.error(f"Erro ao obter credenciais de produção: {e}")
        return None


def _projeto_com_credenciais():
    """Metadados da sessão + senhas carregadas sob demanda do banco."""
    if "projeto_selecionado" not in session:
        return None
    # Remove senhas/usuários DB legados do cookie (sessões antigas)
    for key in list(session["projeto_selecionado"].keys()):
        if "senha" in key.lower() or key in ("usuarioProducao", "usuariohomologacao"):
            session["projeto_selecionado"].pop(key, None)
            session.modified = True
    projeto = dict(session["projeto_selecionado"])
    creds = buscar_credenciais_projeto(projeto.get("ProjetoID"))
    if creds:
        projeto.update(creds)
    return projeto


def log_detalhes_conexao(server, database, user, password):
    """Loga detalhes da conexão sem expor a senha."""
    logger.info("=" * 80)
    logger.info("DIAGNÓSTICO DE CONEXÃO")
    logger.info("=" * 80)
    logger.info(f"Servidor: {server}")
    logger.info(f"Banco: {database}")
    logger.info(f"Usuário: {user}")
    logger.info(f"Senha configurada: {'sim' if password else 'não'}")
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
    
    projeto = _projeto_com_credenciais()
    if not projeto:
        logger.warning("Nenhum projeto na sessão (ou sem ProjetoID)")
        logger.info(f"Session keys: {list(session.keys())}")
        server = Config.DB2_SERVER
        user = Config.DB2_USER
        password = Config.DB2_PASSWORD
        origem = "DEFAULT (sem projeto na sessão)"
    else:
        logger.info(f"Projeto: {projeto.get('NomeProjeto')} (id={projeto.get('ProjetoID')})")
        logger.info(f"DadosGX: {projeto.get('DadosGX')} | banco solicitado: {banco_nome}")

        banco_homologacao = projeto.get("DadosGX")
        banco_producao = projeto.get("bancoProducao")
        banco_homo_wf = projeto.get("BancoHomo")

        if banco_nome == banco_homologacao:
            logger.info("Banco solicitado é o DadosGX (homologação) deste projeto")

            servidor_homo = projeto.get("servidorhomologacao")
            usuario_homo = projeto.get("usuariohomologacao")
            senha_homo = projeto.get("senhahomologacao")

            logger.info(f"Credenciais homologação — servidor: '{servidor_homo}', usuário: '{usuario_homo}', senha: {'sim' if senha_homo else 'não'}")

            campos_preenchidos = all([servidor_homo, usuario_homo, senha_homo])

            if campos_preenchidos:
                server = servidor_homo
                user = usuario_homo
                password = senha_homo
                origem = "CREDENCIAIS ESPECÍFICAS DO PROJETO (homologação)"
            else:
                server = Config.DB2_SERVER
                user = Config.DB2_USER
                password = Config.DB2_PASSWORD
                origem = "DEFAULT (credenciais de homologação incompletas)"
                logger.warning("Usando credenciais padrão - campos de homologação incompletos")

        elif banco_nome == banco_producao:
            logger.info("Banco solicitado é o de produção deste projeto")
            servidor_prod = projeto.get("servidorproducao")
            usuario_prod = projeto.get("usuarioProducao")
            senha_prod = projeto.get("senhaproducao")

            logger.info(f"Credenciais produção — servidor: '{servidor_prod}', usuário: '{usuario_prod}', senha: {'sim' if senha_prod else 'não'}")

            campos_preenchidos = all([servidor_prod, usuario_prod, senha_prod])

            if campos_preenchidos:
                server = servidor_prod
                user = usuario_prod
                password = senha_prod
                origem = "CREDENCIAIS ESPECÍFICAS DO PROJETO (produção)"
            else:
                server = Config.DB2_SERVER
                user = Config.DB2_USER
                password = Config.DB2_PASSWORD
                origem = "DEFAULT (credenciais de produção incompletas)"
                logger.warning("Usando credenciais padrão - campos de produção incompletos")

        elif banco_homo_wf and banco_nome == banco_homo_wf:
            logger.info("Banco solicitado é o BancoHomo (homólogo WF) deste projeto")
            servidor_homo = projeto.get("servidorhomologacao")
            usuario_homo = projeto.get("usuariohomologacao")
            senha_homo = projeto.get("senhahomologacao")

            server = Config.DB2_SERVER
            user = Config.DB2_USER
            password = Config.DB2_PASSWORD
            origem = "DEFAULT (BancoHomo no servidor padrão)"

            if not testar_conexao_pyodbc(server, banco_nome, user, password):
                if all([servidor_homo, usuario_homo, senha_homo]):
                    logger.warning(
                        "BancoHomo inacessível no servidor padrão; "
                        "tentando servidor de homologação do projeto"
                    )
                    server = servidor_homo
                    user = usuario_homo
                    password = senha_homo
                    origem = "CREDENCIAIS ESPECÍFICAS DO PROJETO (homologação p/ BancoHomo)"
                else:
                    logger.warning("BancoHomo inacessível e credenciais de homologação incompletas")

        else:
            logger.warning(f"Banco '{banco_nome}' não identificado como DadosGX ou produção")
            server = Config.DB2_SERVER
            user = Config.DB2_USER
            password = Config.DB2_PASSWORD
            origem = "DEFAULT (banco não identificado)"

    log_detalhes_conexao(server, banco_nome, user, password)
    logger.info(f"Origem das credenciais: {origem}")

    logger.info("Testando conexão...")
    teste_ok = testar_conexao_pyodbc(server, banco_nome, user, password)

    if not teste_ok:
        logger.error("Teste de conexão falhou")
        return None

    try:
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={banco_nome};UID={user};PWD={password}"
        logger.info("Criando conexão real...")
        conn = pyodbc.connect(conn_str)
        logger.info("Conexão estabelecida com sucesso")
        return conn
    except Exception as e:
        logger.error(f"Erro ao criar conexão: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

def conectar_usuario():
    """Conecta ao banco de dados do projeto selecionado"""
    logger.info("=" * 80)
    logger.info("CHAMADA: conectar_usuario()")
    logger.info("=" * 80)
    
    projeto = _projeto_com_credenciais()
    if not projeto:
        logger.error("Nenhum projeto selecionado na sessão")
        return None

    banco = projeto.get("DadosGX")

    if not banco:
        logger.error("Banco DadosGX não configurado no projeto")
        return None

    logger.info(f"Conectar usuário ao banco: {banco}")
    return conectar_segunda_base(banco)

def conectar_homologacao():
    """Conecta ao banco de dados de homologação (DadosGX) usando credenciais do projeto"""
    logger.info("=" * 80)
    logger.info("CHAMADA: conectar_homologacao()")
    logger.info("=" * 80)

    projeto = _projeto_com_credenciais()
    if not projeto:
        logger.error("Nenhum projeto selecionado na sessão")
        return None

    banco = projeto.get("DadosGX")

    if not banco:
        logger.error("Banco DadosGX não configurado no projeto")
        return None

    logger.info(f"Conectar homologação ao banco: {banco}")
    return conectar_segunda_base(banco)

def conectar_producao():
    """Conecta ao banco de dados de produção usando credenciais do projeto"""
    logger.info("=" * 80)
    logger.info("CHAMADA: conectar_producao()")
    logger.info("=" * 80)

    projeto = _projeto_com_credenciais()
    if not projeto:
        logger.error("Nenhum projeto selecionado na sessão")
        return None

    banco = projeto.get("bancoProducao")

    if not banco:
        logger.error("Banco de produção não configurado no projeto")
        return None

    logger.info(f"Conectar produção ao banco: {banco}")
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
    
    projeto = _projeto_com_credenciais()
    origem = "DEFAULT (sem projeto na sessão)"
    if not projeto:
        logger.warning("Nenhum projeto na sessão, usando credenciais padrão")
        server = Config.DB2_SERVER
        user = Config.DB2_USER
        password = Config.DB2_PASSWORD
    else:
        if tipo == "producao":
            servidor = projeto.get("servidorproducao") or Config.DB2_SERVER
            usuario = projeto.get("usuarioProducao") or Config.DB2_USER
            senha = projeto.get("senhaproducao") or Config.DB2_PASSWORD
            origem = f"PRODUÇÃO do projeto {projeto.get('NomeProjeto')}"
        else:
            servidor = projeto.get("servidorhomologacao") or Config.DB2_SERVER
            usuario = projeto.get("usuariohomologacao") or Config.DB2_USER
            senha = projeto.get("senhahomologacao") or Config.DB2_PASSWORD
            origem = f"HOMOLOGAÇÃO do projeto {projeto.get('NomeProjeto')}"

        server = servidor
        user = usuario
        password = senha

    logger.info(f"Origem: {origem}")
    logger.info(f"Servidor: {server}")
    logger.info(f"Usuário: {user}")
    logger.info(f"Senha configurada: {'sim' if password else 'não'}")

    try:
        conn = pyodbc.connect(
            Driver="{ODBC Driver 17 for SQL Server}",
            Server=server,
            Database=banco_nome,
            UID=user,
            PWD=password,
            timeout=Config.DB2_TIMEOUT,
        )
        logger.info(f"Conexão bem-sucedida com {banco_nome}")
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar base {banco_nome} ({tipo}): {e}")
        return None