"""Helpers de schema/persistência para login cifrado (UsuarioNome + UsuarioNomeHash)."""

from logger import logger


def garantir_coluna_login_hash(cursor, conn=None):
    """Garante UsuarioNomeHash e largura suficiente em UsuarioNome."""
    try:
        cursor.execute(
            """
            SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'Usuarios' AND COLUMN_NAME = 'UsuarioNomeHash'
            """
        )
        if not cursor.fetchone():
            cursor.execute(
                "ALTER TABLE Usuarios ADD UsuarioNomeHash NVARCHAR(128) NULL"
            )
            logger.info("Coluna Usuarios.UsuarioNomeHash criada")
            if conn:
                conn.commit()

        cursor.execute(
            """
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'Usuarios' AND COLUMN_NAME = 'UsuarioNome'
            """
        )
        row = cursor.fetchone()
        if row and row[0] is not None and 0 < row[0] < 500:
            cursor.execute(
                "ALTER TABLE Usuarios ALTER COLUMN UsuarioNome NVARCHAR(500) NOT NULL"
            )
            logger.info("Coluna Usuarios.UsuarioNome ampliada para NVARCHAR(500)")
            if conn:
                conn.commit()
    except Exception as exc:
        logger.warning("Não foi possível ajustar colunas de login: %s", exc)
