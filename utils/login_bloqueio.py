"""Bloqueio de conta após tentativas inválidas de senha no login."""

from __future__ import annotations

from logger import logger

MAX_TENTATIVAS_SENHA = 5


def garantir_colunas_bloqueio_login(cursor, conn=None):
    """Garante TentativasLoginFalha, ContaBloqueada e DataBloqueio em Usuarios."""
    try:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'Usuarios'
              AND COLUMN_NAME IN ('TentativasLoginFalha', 'ContaBloqueada', 'DataBloqueio')
            """
        )
        existentes = {row[0] for row in cursor.fetchall()}

        if "TentativasLoginFalha" not in existentes:
            cursor.execute(
                "ALTER TABLE Usuarios ADD TentativasLoginFalha INT NOT NULL CONSTRAINT DF_Usuarios_TentativasLoginFalha DEFAULT 0"
            )
            logger.info("Coluna Usuarios.TentativasLoginFalha criada")

        if "ContaBloqueada" not in existentes:
            cursor.execute(
                "ALTER TABLE Usuarios ADD ContaBloqueada BIT NOT NULL CONSTRAINT DF_Usuarios_ContaBloqueada DEFAULT 0"
            )
            logger.info("Coluna Usuarios.ContaBloqueada criada")

        if "DataBloqueio" not in existentes:
            cursor.execute("ALTER TABLE Usuarios ADD DataBloqueio DATETIME NULL")
            logger.info("Coluna Usuarios.DataBloqueio criada")

        if conn and (
            "TentativasLoginFalha" not in existentes
            or "ContaBloqueada" not in existentes
            or "DataBloqueio" not in existentes
        ):
            conn.commit()
    except Exception as exc:
        logger.warning("Não foi possível criar colunas de bloqueio de login: %s", exc)


def conta_esta_bloqueada(valor) -> bool:
    return bool(valor)


def resetar_tentativas_login(cursor, conn, usuario_id: int):
    """Zera contador após login bem-sucedido."""
    cursor.execute(
        """
        UPDATE Usuarios
        SET TentativasLoginFalha = 0
        WHERE UsuarioID = ? AND TentativasLoginFalha <> 0
        """,
        (int(usuario_id),),
    )
    if conn:
        conn.commit()


def desbloquear_conta(cursor, conn, usuario_id: int):
    """Admin desbloqueia a senha/conta."""
    cursor.execute(
        """
        UPDATE Usuarios
        SET ContaBloqueada = 0,
            TentativasLoginFalha = 0,
            DataBloqueio = NULL
        WHERE UsuarioID = ?
        """,
        (int(usuario_id),),
    )
    if conn:
        conn.commit()


def registrar_falha_senha(cursor, conn, usuario_id: int) -> dict:
    """
    Incrementa falhas; bloqueia ao atingir o limite.
    Retorna: {tentativas, bloqueada, bloqueou_agora}
    """
    cursor.execute(
        """
        SELECT TentativasLoginFalha, ContaBloqueada
        FROM Usuarios
        WHERE UsuarioID = ?
        """,
        (int(usuario_id),),
    )
    row = cursor.fetchone()
    if not row:
        return {"tentativas": 0, "bloqueada": False, "bloqueou_agora": False}

    tentativas = int(row.TentativasLoginFalha or 0)
    ja_bloqueada = bool(row.ContaBloqueada)

    if ja_bloqueada:
        return {
            "tentativas": tentativas,
            "bloqueada": True,
            "bloqueou_agora": False,
        }

    tentativas += 1
    bloqueou_agora = tentativas >= MAX_TENTATIVAS_SENHA

    if bloqueou_agora:
        cursor.execute(
            """
            UPDATE Usuarios
            SET TentativasLoginFalha = ?,
                ContaBloqueada = 1,
                DataBloqueio = GETDATE()
            WHERE UsuarioID = ?
            """,
            (tentativas, int(usuario_id)),
        )
    else:
        cursor.execute(
            """
            UPDATE Usuarios
            SET TentativasLoginFalha = ?
            WHERE UsuarioID = ?
            """,
            (tentativas, int(usuario_id)),
        )

    if conn:
        conn.commit()

    return {
        "tentativas": tentativas,
        "bloqueada": bloqueou_agora,
        "bloqueou_agora": bloqueou_agora,
    }


def listar_emails_admins(cursor) -> list[str]:
    """E-mails de administradores ativos com e-mail cadastrado."""
    try:
        cursor.execute(
            """
            SELECT Email
            FROM Usuarios
            WHERE Adm = 1
              AND Ativo = 1
              AND Email IS NOT NULL
              AND LTRIM(RTRIM(Email)) <> ''
            """
        )
        emails = []
        vistos = set()
        for row in cursor.fetchall():
            email = (row.Email or "").strip().lower()
            if email and email not in vistos:
                vistos.add(email)
                emails.append((row.Email or "").strip())
        return emails
    except Exception as exc:
        logger.warning("Não foi possível listar e-mails de admins: %s", exc)
        return []


def notificar_admins_bloqueio(cursor, usuario_nome: str, usuario_id: int, tentativas: int):
    """Envia e-mail aos admins avisando do bloqueio por senha."""
    from utils.email_config import obter_smtp_config
    from utils.email_service import resolver_config_smtp, enviar_email

    emails = listar_emails_admins(cursor)
    if not emails:
        logger.warning(
            "Conta bloqueada (usuario_id=%s) mas nenhum admin com e-mail cadastrado",
            usuario_id,
        )
        return False

    smtp_cfg = resolver_config_smtp(obter_smtp_config(None))
    if not smtp_cfg:
        logger.error(
            "Conta bloqueada (usuario_id=%s) mas SMTP não configurado",
            usuario_id,
        )
        return False

    assunto = "Conta bloqueada por tentativas de senha — DE X PARA"
    texto = (
        f"A conta do usuário '{usuario_nome}' (ID {usuario_id}) "
        f"foi bloqueada após {tentativas} tentativas inválidas de senha.\n\n"
        "Desbloqueie o usuário em Gerenciar Usuários, se for o caso.\n"
    )
    html = (
        f"<p>A conta do usuário <strong>{usuario_nome}</strong> "
        f"(ID {usuario_id}) foi bloqueada após "
        f"<strong>{tentativas}</strong> tentativas inválidas de senha.</p>"
        "<p>Desbloqueie o usuário em <strong>Gerenciar Usuários</strong>, se for o caso.</p>"
    )

    try:
        enviar_email(smtp_cfg, emails, assunto, texto, html)
        logger.info(
            "E-mail de bloqueio enviado aos admins para usuario_id=%s (dest=%s)",
            usuario_id,
            len(emails),
        )
        return True
    except Exception as exc:
        logger.error("Falha ao enviar e-mail de bloqueio de conta: %s", exc)
        return False
