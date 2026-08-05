"""
Migra UsuarioNome (login) em texto claro → cifra Fernet + UsuarioNomeHash.

Uso:
  python scripts/migrar_logins_criptografados.py
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
    esta_criptografado,
    preparar_login_para_banco,
    revelar_login_banco,
)
from utils.usuario_login_db import garantir_coluna_login_hash


def main():
    conn = conectar_banco()
    if not conn:
        print("Falha ao conectar no banco principal.")
        sys.exit(1)

    cursor = conn.cursor()
    try:
        garantir_coluna_login_hash(cursor, conn)
        cursor.execute("SELECT UsuarioID, UsuarioNome, UsuarioNomeHash FROM Usuarios")
        rows = cursor.fetchall()
        atualizados = 0
        for row in rows:
            uid = row.UsuarioID
            nome_banco = row.UsuarioNome
            hash_atual = getattr(row, "UsuarioNomeHash", None)
            claro = revelar_login_banco(nome_banco)
            if not claro:
                continue
            precisa = (not esta_criptografado(str(nome_banco or ""))) or (not hash_atual)
            if not precisa:
                continue
            cif, h = preparar_login_para_banco(claro)
            cursor.execute(
                """
                UPDATE Usuarios
                SET UsuarioNome = ?, UsuarioNomeHash = ?
                WHERE UsuarioID = ?
                """,
                (cif, h, uid),
            )
            atualizados += 1
            print(f"  UsuarioID {uid}: login cifrado")
        conn.commit()
        print(f"\nConcluído: {atualizados} usuário(s).")
    except Exception as exc:
        conn.rollback()
        print(f"Erro: {exc}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
