"""Persistência da configuração de e-mail por projeto."""

import re

from db.connection import conectar_banco
from logger import logger
from utils.credential_crypto import criptografar_segredo, descriptografar_segredo
from utils.depara_escopos import BLOCOS_NOTIFICACAO, CATEGORIAS_NOMES

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
CONFIGURACAO_GLOBAL_ID = 0


def validar_lista_emails(texto):
    """Valida lista de e-mails separados por vírgula, ponto-e-vírgula ou quebra de linha."""
    if not texto or not str(texto).strip():
        return True, []

    emails = []
    for parte in re.split(r"[,;\n\r]+", str(texto)):
        email = parte.strip()
        if not email:
            continue
        if not _EMAIL_RE.match(email):
            return False, [email]
        emails.append(email)

    return True, emails


def normalizar_destinatarios(texto):
    ok, invalidos = validar_lista_emails(texto)
    if not ok:
        raise ValueError(f"E-mail inválido: {invalidos[0]}")
    _, emails = validar_lista_emails(texto)
    return ", ".join(emails)


def _tabela_existe(cursor, nome_tabela):
    cursor.execute(
        """
        SELECT 1 FROM sys.tables
        WHERE name = ? AND schema_id = SCHEMA_ID('dbo')
        """,
        (nome_tabela,),
    )
    return cursor.fetchone() is not None


def _garantir_tabelas(conn, cursor):
    """Cria as tabelas de e-mail no banco principal caso ainda não existam."""
    criou = False

    if not _tabela_existe(cursor, "EmailSmtpConfig"):
        cursor.execute(
            """
            CREATE TABLE dbo.EmailSmtpConfig (
                EmailSmtpConfigID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                ProjetoID INT NOT NULL,
                SmtpHost NVARCHAR(255) NOT NULL,
                SmtpPort INT NOT NULL CONSTRAINT DF_EmailSmtpConfig_Port DEFAULT (587),
                SmtpUsuario NVARCHAR(255) NULL,
                SmtpSenha NVARCHAR(500) NULL,
                EmailRemetente NVARCHAR(255) NOT NULL,
                Destinatarios NVARCHAR(MAX) NOT NULL
                    CONSTRAINT DF_EmailSmtpConfig_Destinatarios DEFAULT (''),
                UsarTls BIT NOT NULL CONSTRAINT DF_EmailSmtpConfig_Tls DEFAULT (1),
                Ativo BIT NOT NULL CONSTRAINT DF_EmailSmtpConfig_Ativo DEFAULT (1),
                CONSTRAINT UQ_EmailSmtpConfig_Projeto UNIQUE (ProjetoID)
            )
            """
        )
        criou = True

    if not _tabela_existe(cursor, "EmailNotificacaoEnviada"):
        cursor.execute(
            """
            CREATE TABLE dbo.EmailNotificacaoEnviada (
                EmailNotificacaoEnviadaID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                ProjetoID INT NOT NULL,
                BlocoEscopo NVARCHAR(50) NOT NULL,
                DataEnvio DATETIME NOT NULL
                    CONSTRAINT DF_EmailNotificacaoEnviada_Data DEFAULT (GETDATE()),
                Destinatarios NVARCHAR(MAX) NULL,
                CONSTRAINT UQ_EmailNotificacaoEnviada UNIQUE (ProjetoID, BlocoEscopo)
            )
            """
        )
        criou = True

    if criou:
        conn.commit()
        logger.info("Tabelas de configuração de e-mail criadas automaticamente")


def obter_smtp_config(projeto_id):
    """Obtém a única configuração global; projeto_id é mantido por compatibilidade."""
    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return None

        cursor = conn.cursor()
        if not _tabela_existe(cursor, "EmailSmtpConfig"):
            return None

        cursor.execute(
            """
            SELECT SmtpHost, SmtpPort, SmtpUsuario, SmtpSenha, EmailRemetente,
                   Destinatarios, UsarTls, Ativo
            FROM EmailSmtpConfig
            WHERE ProjetoID = ?
            """,
            (CONFIGURACAO_GLOBAL_ID,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        return {
            "smtp_host": row.SmtpHost or "",
            "smtp_port": int(row.SmtpPort or 587),
            "smtp_usuario": row.SmtpUsuario or "",
            "smtp_senha": descriptografar_segredo(row.SmtpSenha) or "",
            "smtp_senha_configurada": bool(row.SmtpSenha),
            "email_remetente": row.EmailRemetente or "",
            "destinatarios": row.Destinatarios or "",
            "usar_tls": bool(row.UsarTls),
            "ativo": True,
        }
    except Exception as exc:
        logger.error(f"Erro ao obter SMTP config: {exc}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def salvar_smtp_config(projeto_id, dados):
    """Salva a única configuração global; projeto_id é mantido por compatibilidade."""
    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            raise RuntimeError("Falha na conexão com o banco principal")

        cursor = conn.cursor()
        _garantir_tabelas(conn, cursor)

        cursor.execute(
            "SELECT 1 FROM EmailSmtpConfig WHERE ProjetoID = ?",
            (CONFIGURACAO_GLOBAL_ID,),
        )
        existe = cursor.fetchone() is not None

        destinatarios = normalizar_destinatarios(dados.get("destinatarios", ""))
        senha_input = (dados.get("smtp_senha") or "").strip()
        senha_gravar = criptografar_segredo(senha_input) if senha_input else None

        params = (
            dados.get("smtp_host", "").strip(),
            int(dados.get("smtp_port") or 587),
            dados.get("smtp_usuario", "").strip() or None,
            senha_gravar,
            dados.get("email_remetente", "").strip(),
            destinatarios,
            1 if dados.get("usar_tls", True) else 0,
            1 if dados.get("ativo", True) else 0,
            CONFIGURACAO_GLOBAL_ID,
        )

        if existe:
            cursor.execute(
                """
                UPDATE EmailSmtpConfig
                SET SmtpHost = ?, SmtpPort = ?, SmtpUsuario = ?,
                    SmtpSenha = COALESCE(?, SmtpSenha), EmailRemetente = ?,
                    Destinatarios = ?, UsarTls = ?, Ativo = ?
                WHERE ProjetoID = ?
                """,
                params,
            )
        else:
            cursor.execute(
                """
                INSERT INTO EmailSmtpConfig
                    (SmtpHost, SmtpPort, SmtpUsuario, SmtpSenha, EmailRemetente,
                     Destinatarios, UsarTls, Ativo, ProjetoID)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )

        conn.commit()
        return True
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def obter_notificacoes_bloco(projeto_id):
    """Retorna apenas o histórico de envio dos blocos monitorados."""
    configs = {
        bloco: {
            "bloco_escopo": bloco,
            "nome": CATEGORIAS_NOMES.get(bloco, bloco),
            "enviado": False,
            "data_envio": None,
        }
        for bloco in BLOCOS_NOTIFICACAO
    }

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return list(configs.values())

        cursor = conn.cursor()
        if _tabela_existe(cursor, "EmailNotificacaoEnviada"):
            cursor.execute(
                """
                SELECT BlocoEscopo, DataEnvio
                FROM EmailNotificacaoEnviada
                WHERE ProjetoID = ?
                """,
                (projeto_id,),
            )
            for row in cursor.fetchall():
                bloco = row.BlocoEscopo
                if bloco in configs:
                    configs[bloco]["enviado"] = True
                    data_envio = row.DataEnvio
                    configs[bloco]["data_envio"] = (
                        data_envio.strftime("%d/%m/%Y %H:%M") if data_envio else None
                    )

        return list(configs.values())
    except Exception as exc:
        logger.error(f"Erro ao obter notificações por bloco: {exc}")
        return list(configs.values())
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def bloco_ja_notificado(projeto_id, bloco_escopo):
    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return False

        cursor = conn.cursor()
        if not _tabela_existe(cursor, "EmailNotificacaoEnviada"):
            return False

        cursor.execute(
            "SELECT 1 FROM EmailNotificacaoEnviada WHERE ProjetoID = ? AND BlocoEscopo = ?",
            (projeto_id, bloco_escopo),
        )
        return cursor.fetchone() is not None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def registrar_notificacao_enviada(projeto_id, bloco_escopo, destinatarios):
    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return False

        cursor = conn.cursor()
        _garantir_tabelas(conn, cursor)

        cursor.execute(
            "SELECT 1 FROM EmailNotificacaoEnviada WHERE ProjetoID = ? AND BlocoEscopo = ?",
            (projeto_id, bloco_escopo),
        )
        if cursor.fetchone():
            cursor.execute(
                """
                UPDATE EmailNotificacaoEnviada
                SET DataEnvio = GETDATE(), Destinatarios = ?
                WHERE ProjetoID = ? AND BlocoEscopo = ?
                """,
                (destinatarios, projeto_id, bloco_escopo),
            )
        else:
            cursor.execute(
                """
                INSERT INTO EmailNotificacaoEnviada (ProjetoID, BlocoEscopo, Destinatarios)
                VALUES (?, ?, ?)
                """,
                (projeto_id, bloco_escopo, destinatarios),
            )

        conn.commit()
        return True
    except Exception as exc:
        logger.error(f"Erro ao registrar notificação enviada: {exc}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def redefinir_notificacao_bloco(projeto_id, bloco_escopo):
    """Remove registro de envio para permitir novo disparo (ex.: após reabrir pendências)."""
    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return False

        cursor = conn.cursor()
        if not _tabela_existe(cursor, "EmailNotificacaoEnviada"):
            return False

        cursor.execute(
            "DELETE FROM EmailNotificacaoEnviada WHERE ProjetoID = ? AND BlocoEscopo = ?",
            (projeto_id, bloco_escopo),
        )
        conn.commit()
        return True
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
