"""Schema e helpers para e-mail do usuário e tokens de redefinição de senha."""

from __future__ import annotations

import hashlib
import re
import secrets
from datetime import datetime, timedelta

from logger import logger

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
TOKEN_VALIDADE_HORAS = 2


def email_valido(email: str | None) -> bool:
    email = (email or "").strip()
    return bool(email) and bool(_EMAIL_RE.match(email)) and len(email) <= 255


def garantir_coluna_email(cursor, conn=None):
    """Garante coluna Usuarios.Email."""
    try:
        cursor.execute(
            """
            SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'Usuarios' AND COLUMN_NAME = 'Email'
            """
        )
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE Usuarios ADD Email NVARCHAR(255) NULL")
            logger.info("Coluna Usuarios.Email criada")
            if conn:
                conn.commit()
    except Exception as exc:
        logger.warning("Não foi possível criar Usuarios.Email: %s", exc)


def garantir_tabela_reset_senha(cursor, conn=None):
    """Garante tabela PasswordResetToken."""
    try:
        cursor.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = 'PasswordResetToken'
            )
            BEGIN
                CREATE TABLE PasswordResetToken (
                    TokenID INT IDENTITY(1,1) PRIMARY KEY,
                    UsuarioID INT NOT NULL,
                    TokenHash NVARCHAR(128) NOT NULL,
                    ExpiraEm DATETIME NOT NULL,
                    Usado BIT NOT NULL DEFAULT 0,
                    CriadoEm DATETIME NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT FK_PasswordReset_Usuario
                        FOREIGN KEY (UsuarioID) REFERENCES Usuarios(UsuarioID)
                );
                CREATE INDEX IX_PasswordReset_TokenHash
                    ON PasswordResetToken (TokenHash);
            END
            """
        )
        if conn:
            conn.commit()
    except Exception as exc:
        logger.warning("Não foi possível criar PasswordResetToken: %s", exc)


def hash_token_reset(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def criar_token_reset(cursor, conn, usuario_id: int) -> str:
    """Invalida tokens anteriores e cria um novo. Retorna o token em claro."""
    garantir_tabela_reset_senha(cursor, conn)
    cursor.execute(
        """
        UPDATE PasswordResetToken
        SET Usado = 1
        WHERE UsuarioID = ? AND Usado = 0
        """,
        (usuario_id,),
    )
    token = secrets.token_urlsafe(32)
    token_hash = hash_token_reset(token)
    expira = datetime.now() + timedelta(hours=TOKEN_VALIDADE_HORAS)
    cursor.execute(
        """
        INSERT INTO PasswordResetToken (UsuarioID, TokenHash, ExpiraEm, Usado)
        VALUES (?, ?, ?, 0)
        """,
        (usuario_id, token_hash, expira),
    )
    if conn:
        conn.commit()
    return token


def buscar_token_valido(cursor, token: str):
    """Retorna (TokenID, UsuarioID) se o token for válido; senão None."""
    if not token or len(token) > 200:
        return None
    garantir_tabela_reset_senha(cursor)
    token_hash = hash_token_reset(token)
    cursor.execute(
        """
        SELECT TOP 1 TokenID, UsuarioID, ExpiraEm, Usado
        FROM PasswordResetToken
        WHERE TokenHash = ?
        ORDER BY TokenID DESC
        """,
        (token_hash,),
    )
    row = cursor.fetchone()
    if not row:
        return None
    if row.Usado:
        return None
    if row.ExpiraEm < datetime.now():
        return None
    return row.TokenID, row.UsuarioID


def marcar_token_usado(cursor, conn, token_id: int):
    cursor.execute(
        "UPDATE PasswordResetToken SET Usado = 1 WHERE TokenID = ?",
        (token_id,),
    )
    if conn:
        conn.commit()
