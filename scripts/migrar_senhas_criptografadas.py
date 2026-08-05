"""
Migra senhas em texto claro → Fernet (Projeto + EmailSmtpConfig).

Uso (na pasta do projeto, com .env carregado):
  python scripts/migrar_senhas_criptografadas.py

Requer cryptography instalado e, de preferência, CREDENTIALS_KEY no .env.
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(ROOT, ".env"))

from db.connection import conectar_banco
from utils.credential_crypto import (
    criptografar_segredo,
    esta_criptografado,
    gerar_chave_credentials,
)


def _ampliar_colunas(cursor):
    for coluna in (
        "senhaproducao",
        "senhahomologacao",
        "usuarioProducao",
        "usuariohomologacao",
    ):
        cursor.execute(
            """
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'Projeto' AND COLUMN_NAME = ?
            """,
            (coluna,),
        )
        row = cursor.fetchone()
        if row and row[0] is not None and 0 < row[0] < 500:
            cursor.execute(f"ALTER TABLE Projeto ALTER COLUMN {coluna} NVARCHAR(500) NULL")
            print(f"  ampliada Projeto.{coluna} -> NVARCHAR(500)")


def migrar_projetos(cursor):
    cursor.execute(
        """
        SELECT ProjetoID, senhaproducao, senhahomologacao,
               usuarioProducao, usuariohomologacao
        FROM Projeto
        """
    )
    rows = cursor.fetchall()
    atualizados = 0
    for row in rows:
        pid = row.ProjetoID
        sp = row.senhaproducao
        sh = row.senhahomologacao
        up = row.usuarioProducao
        uh = row.usuariohomologacao
        nova_sp, nova_sh, nova_up, nova_uh = sp, sh, up, uh
        mudou = False
        if sp and not esta_criptografado(str(sp)):
            nova_sp = criptografar_segredo(sp)
            mudou = True
        if sh and not esta_criptografado(str(sh)):
            nova_sh = criptografar_segredo(sh)
            mudou = True
        if up and not esta_criptografado(str(up)):
            nova_up = criptografar_segredo(up)
            mudou = True
        if uh and not esta_criptografado(str(uh)):
            nova_uh = criptografar_segredo(uh)
            mudou = True
        if mudou:
            cursor.execute(
                """
                UPDATE Projeto
                SET senhaproducao = ?, senhahomologacao = ?,
                    usuarioProducao = ?, usuariohomologacao = ?
                WHERE ProjetoID = ?
                """,
                (nova_sp, nova_sh, nova_up, nova_uh, pid),
            )
            atualizados += 1
            print(f"  ProjetoID {pid}: credenciais cifradas")
    return atualizados


def migrar_smtp(cursor):
    cursor.execute(
        """
        SELECT 1 FROM sys.tables
        WHERE name = 'EmailSmtpConfig' AND schema_id = SCHEMA_ID('dbo')
        """
    )
    if not cursor.fetchone():
        print("  EmailSmtpConfig inexistente — pulando")
        return 0

    cursor.execute("SELECT EmailSmtpConfigID, SmtpSenha FROM EmailSmtpConfig")
    rows = cursor.fetchall()
    atualizados = 0
    for row in rows:
        senha = row.SmtpSenha
        if senha and not esta_criptografado(str(senha)):
            cursor.execute(
                "UPDATE EmailSmtpConfig SET SmtpSenha = ? WHERE EmailSmtpConfigID = ?",
                (criptografar_segredo(senha), row.EmailSmtpConfigID),
            )
            atualizados += 1
            print(f"  EmailSmtpConfigID {row.EmailSmtpConfigID}: SmtpSenha cifrada")
    return atualizados


def main():
    if not os.getenv("CREDENTIALS_KEY"):
        print("AVISO: CREDENTIALS_KEY não está no .env.")
        print("Sugestão — adicione:")
        print(f"  CREDENTIALS_KEY={gerar_chave_credentials()}")
        print("Sem ela, a chave será derivada de SECRET_KEY (menos ideal).\n")

    conn = conectar_banco()
    if not conn:
        print("Falha ao conectar no banco principal.")
        sys.exit(1)

    cursor = conn.cursor()
    try:
        print("Ampliando colunas se necessário...")
        _ampliar_colunas(cursor)
        print("Migrando Projeto...")
        n1 = migrar_projetos(cursor)
        print("Migrando EmailSmtpConfig...")
        n2 = migrar_smtp(cursor)
        conn.commit()
        print(f"\nConcluído: {n1} projeto(s), {n2} SMTP.")
    except Exception as exc:
        conn.rollback()
        print(f"Erro: {exc}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
