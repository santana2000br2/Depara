import pyodbc
import contextvars
from config import Config
from logger import logger
from flask import session, has_request_context

_projeto_id_conexao = contextvars.ContextVar("projeto_id_conexao", default=None)

_COLUNAS_DADOSGX = (
    ("servidordadosgx", "NVARCHAR(255) NULL"),
    ("usuariodadosgx", "NVARCHAR(500) NULL"),
    ("senhadadosgx", "NVARCHAR(500) NULL"),
)


def definir_projeto_conexao(projeto_id):
    """Define o projeto para conexões fora da sessão Flask (jobs em background)."""
    return _projeto_id_conexao.set(projeto_id)


def obter_projeto_id_conexao():
    """Projeto ativo na sessão ou definido para o job em background."""
    pid = _projeto_id_conexao.get()
    if pid:
        return pid
    if has_request_context() and "projeto_selecionado" in session:
        return (session.get("projeto_selecionado") or {}).get("ProjetoID")
    return None


def garantir_colunas_dadosgx(cursor):
    """Cria servidor/usuário/senha do DadosGX se ainda não existirem na tabela Projeto."""
    for coluna, ddl in _COLUNAS_DADOSGX:
        try:
            cursor.execute(
                """
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'Projeto' AND COLUMN_NAME = ?
                """,
                (coluna,),
            )
            if not cursor.fetchone():
                cursor.execute(f"ALTER TABLE Projeto ADD {coluna} {ddl}")
                logger.info("Coluna Projeto.%s criada", coluna)
        except Exception as exc:
            logger.warning("Não foi possível garantir coluna Projeto.%s: %s", coluna, exc)


def _creds_completas(servidor, usuario, senha):
    return bool((servidor or "").strip() and (usuario or "").strip() and senha)

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
        garantir_colunas_dadosgx(cursor)
        conn.commit()
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
                servidordadosgx,
                usuariodadosgx,
                senhadadosgx,
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
            "servidordadosgx": getattr(row, "servidordadosgx", None),
            "usuariodadosgx": descriptografar_segredo(getattr(row, "usuariodadosgx", None)),
            "senhadadosgx": descriptografar_segredo(getattr(row, "senhadadosgx", None)),
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


def _projeto_com_credenciais(projeto_id=None):
    """Metadados da sessão + senhas carregadas sob demanda do banco."""
    pid = projeto_id or _projeto_id_conexao.get()
    if not pid and has_request_context() and "projeto_selecionado" in session:
        # Remove senhas/usuários DB legados do cookie (sessões antigas)
        for key in list(session["projeto_selecionado"].keys()):
            if "senha" in key.lower() or key in (
                "usuarioProducao", "usuariohomologacao", "usuariodadosgx",
            ):
                session["projeto_selecionado"].pop(key, None)
                session.modified = True
        projeto = dict(session["projeto_selecionado"])
        pid = projeto.get("ProjetoID")
        creds = buscar_credenciais_projeto(pid)
        if creds:
            projeto.update(creds)
        return projeto
    if pid:
        return buscar_credenciais_projeto(pid)
    return None


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

def conectar_segunda_base(banco_nome, projeto_id=None):
    """Conecta a um banco específico usando credenciais do projeto se disponíveis"""
    logger.info("=" * 80)
    logger.info(f"CHAMADA: conectar_segunda_base('{banco_nome}')")
    logger.info("=" * 80)
    
    projeto = _projeto_com_credenciais(projeto_id)
    banco_homo_wf = ""
    if not projeto:
        logger.warning("Nenhum projeto na sessão (ou sem ProjetoID)")
        if has_request_context():
            logger.info(f"Session keys: {list(session.keys())}")
        server = Config.DB2_SERVER
        user = Config.DB2_USER
        password = Config.DB2_PASSWORD
        origem = "DEFAULT (sem projeto na sessão)"
    else:
        logger.info(f"Projeto: {projeto.get('NomeProjeto')} (id={projeto.get('ProjetoID')})")
        logger.info(f"DadosGX: {projeto.get('DadosGX')} | banco solicitado: {banco_nome}")

        banco_gx = (projeto.get("DadosGX") or "").strip()
        banco_producao = (projeto.get("bancoProducao") or "").strip()
        banco_homo_wf = (projeto.get("BancoHomo") or "").strip()
        banco_solicitado = (banco_nome or "").strip()

        if banco_solicitado and banco_solicitado == banco_gx:
            logger.info("Banco solicitado é o DadosGX deste projeto")

            servidor_gx = projeto.get("servidordadosgx")
            usuario_gx = projeto.get("usuariodadosgx")
            senha_gx = projeto.get("senhadadosgx")
            servidor_homo = projeto.get("servidorhomologacao")
            usuario_homo = projeto.get("usuariohomologacao")
            senha_homo = projeto.get("senhahomologacao")

            logger.info(
                "Credenciais DadosGX — servidor: '%s', usuário: '%s', senha: %s",
                servidor_gx, usuario_gx, 'sim' if senha_gx else 'não',
            )

            if _creds_completas(servidor_gx, usuario_gx, senha_gx):
                server, user, password = servidor_gx, usuario_gx, senha_gx
                origem = "CREDENCIAIS ESPECÍFICAS DO PROJETO (DadosGX)"
            elif _creds_completas(servidor_homo, usuario_homo, senha_homo):
                server, user, password = servidor_homo, usuario_homo, senha_homo
                origem = "CREDENCIAIS DE HOMOLOGAÇÃO DO PROJETO (fallback DadosGX)"
                logger.warning(
                    "DadosGX sem servidor/usuário/senha próprios — usando homologação do projeto"
                )
            else:
                server = Config.DB2_SERVER
                user = Config.DB2_USER
                password = Config.DB2_PASSWORD
                origem = "DEFAULT (credenciais de DadosGX incompletas)"
                logger.warning("Usando credenciais padrão - campos de DadosGX incompletos")

        elif banco_solicitado and banco_solicitado == banco_producao:
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

        elif banco_homo_wf and banco_solicitado == banco_homo_wf:
            logger.info("Banco solicitado é o BancoHomo (homólogo WF) deste projeto")
            server = Config.DB2_SERVER
            user = Config.DB2_USER
            password = Config.DB2_PASSWORD
            origem = "DEFAULT (BancoHomo no servidor padrão)"

        else:
            logger.warning(f"Banco '{banco_nome}' não identificado como DadosGX ou produção")
            server = Config.DB2_SERVER
            user = Config.DB2_USER
            password = Config.DB2_PASSWORD
            origem = "DEFAULT (banco não identificado)"

    logger.info(
        "Conectando %s em %s (usuário %s, origem %s)",
        banco_nome, server, user, origem,
    )

    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={banco_nome};UID={user};PWD={password}"
        )
        conn = pyodbc.connect(conn_str)
        logger.info("Conexão estabelecida com %s", banco_nome)
        return conn
    except Exception as e:
        if (
            projeto
            and banco_homo_wf
            and (banco_nome or "").strip() == banco_homo_wf
            and origem.startswith("DEFAULT")
            and _creds_completas(
                projeto.get("servidorhomologacao"),
                projeto.get("usuariohomologacao"),
                projeto.get("senhahomologacao"),
            )
        ):
            logger.warning(
                "BancoHomo inacessível no servidor padrão (%s); tentando homologação do projeto",
                e,
            )
            try:
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={projeto.get('servidorhomologacao')};"
                    f"DATABASE={banco_nome};"
                    f"UID={projeto.get('usuariohomologacao')};"
                    f"PWD={projeto.get('senhahomologacao')}"
                )
                conn = pyodbc.connect(conn_str)
                logger.info("Conexão BancoHomo via homologação do projeto")
                return conn
            except Exception as e2:
                logger.error("Erro ao criar conexão BancoHomo: %s", e2)
                return None
        logger.error("Erro ao criar conexão: %s", e)
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
    """Conecta ao banco DadosGX usando credenciais do projeto (servidor/usuário/senha próprios)."""
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
        elif banco_nome and (banco_nome or "").strip() == (projeto.get("DadosGX") or "").strip() and _creds_completas(
            projeto.get("servidordadosgx"),
            projeto.get("usuariodadosgx"),
            projeto.get("senhadadosgx"),
        ):
            servidor = projeto.get("servidordadosgx")
            usuario = projeto.get("usuariodadosgx")
            senha = projeto.get("senhadadosgx")
            origem = f"DADOS GX do projeto {projeto.get('NomeProjeto')}"
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