"""Registra quando cada bloco De/Para ficou disponível após importação do layout."""

from flask import has_request_context, session

from db.connection import conectar_banco
from logger import logger
from utils.depara_escopos import blocos_do_layout


def _tabela_existe(cursor, nome_tabela):
    cursor.execute(
        "SELECT 1 FROM sys.tables WHERE name = ? AND schema_id = SCHEMA_ID('dbo')",
        (nome_tabela,),
    )
    return cursor.fetchone() is not None


def garantir_tabela(cursor):
    """Cria a tabela BlocoDeParaDisponivel no banco principal se ainda não existir."""
    if _tabela_existe(cursor, "BlocoDeParaDisponivel"):
        return False

    cursor.execute(
        """
        CREATE TABLE dbo.BlocoDeParaDisponivel (
            BlocoDeParaDisponivelID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            ProjetoID INT NOT NULL,
            BlocoEscopo NVARCHAR(50) NOT NULL,
            DataDisponivel DATETIME NOT NULL
                CONSTRAINT DF_BlocoDeParaDisponivel_Data DEFAULT (GETDATE()),
            DataUltimaImportacao DATETIME NOT NULL
                CONSTRAINT DF_BlocoDeParaDisponivel_Ultima DEFAULT (GETDATE()),
            TipoLayout NVARCHAR(80) NULL,
            NomeLayout NVARCHAR(100) NULL,
            UsuarioID INT NULL,
            CONSTRAINT UQ_BlocoDeParaDisponivel UNIQUE (ProjetoID, BlocoEscopo)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IX_BlocoDeParaDisponivel_Projeto "
        "ON dbo.BlocoDeParaDisponivel (ProjetoID)"
    )
    logger.info("Tabela BlocoDeParaDisponivel criada automaticamente.")
    return True


def registrar_blocos_apos_importacao(
    projeto_id,
    tipo_layout,
    layout_nome=None,
    usuario_id=None,
):
    """Marca os blocos afetados pelo layout como disponíveis.

    Na primeira vez grava DataDisponivel; nas reimportações só atualiza
    DataUltimaImportacao / layout / usuário.
    """
    if not projeto_id:
        return []

    blocos = blocos_do_layout(tipo_layout)
    if not blocos:
        return []

    conn = conectar_banco()
    if not conn:
        logger.warning(
            "Não foi possível registrar disponibilidade dos blocos: falha de conexão."
        )
        return []

    registrados = []
    try:
        cursor = conn.cursor()
        garantir_tabela(cursor)

        for escopo in blocos:
            cursor.execute(
                """
                MERGE dbo.BlocoDeParaDisponivel AS alvo
                USING (SELECT ? AS ProjetoID, ? AS BlocoEscopo) AS origem
                    ON alvo.ProjetoID = origem.ProjetoID
                   AND alvo.BlocoEscopo = origem.BlocoEscopo
                WHEN MATCHED THEN
                    UPDATE SET
                        DataUltimaImportacao = GETDATE(),
                        TipoLayout = ?,
                        NomeLayout = ?,
                        UsuarioID = ?
                WHEN NOT MATCHED THEN
                    INSERT (ProjetoID, BlocoEscopo, DataDisponivel, DataUltimaImportacao,
                            TipoLayout, NomeLayout, UsuarioID)
                    VALUES (?, ?, GETDATE(), GETDATE(), ?, ?, ?);
                """,
                (
                    projeto_id,
                    escopo,
                    tipo_layout,
                    layout_nome,
                    usuario_id,
                    projeto_id,
                    escopo,
                    tipo_layout,
                    layout_nome,
                    usuario_id,
                ),
            )
            registrados.append(escopo)

        conn.commit()
        logger.info(
            "Blocos disponíveis após importação do layout %s (projeto %s): %s",
            tipo_layout,
            projeto_id,
            ", ".join(registrados),
        )
        return registrados
    except Exception as exc:
        conn.rollback()
        logger.error(
            "Erro ao registrar disponibilidade dos blocos (projeto %s, layout %s): %s",
            projeto_id,
            tipo_layout,
            exc,
        )
        return []
    finally:
        conn.close()


def registrar_apos_depara(tipo_layout, layout_nome=None):
    """Conveniência: lê projeto/usuário da sessão e registra os blocos."""
    if not has_request_context():
        return []

    projeto = session.get("projeto_selecionado") or {}
    projeto_id = projeto.get("ProjetoID")
    usuario = session.get("usuario") or {}
    usuario_id = usuario.get("usuario_id") or usuario.get("UsuarioID")

    return registrar_blocos_apos_importacao(
        projeto_id,
        tipo_layout,
        layout_nome=layout_nome,
        usuario_id=usuario_id,
    )


def obter_datas_blocos(projeto_id):
    """Retorna dict escopo → {DataDisponivel, DataUltimaImportacao, TipoLayout, NomeLayout}."""
    if not projeto_id:
        return {}

    conn = conectar_banco()
    if not conn:
        return {}

    try:
        cursor = conn.cursor()
        garantir_tabela(cursor)
        conn.commit()

        cursor.execute(
            """
            SELECT BlocoEscopo, DataDisponivel, DataUltimaImportacao, TipoLayout, NomeLayout
            FROM dbo.BlocoDeParaDisponivel
            WHERE ProjetoID = ?
            """,
            (projeto_id,),
        )
        resultado = {}
        for row in cursor.fetchall():
            resultado[row[0]] = {
                "DataDisponivel": row[1],
                "DataUltimaImportacao": row[2],
                "TipoLayout": row[3],
                "NomeLayout": row[4],
            }
        return resultado
    except Exception as exc:
        logger.error(f"Erro ao obter datas dos blocos do projeto {projeto_id}: {exc}")
        return {}
    finally:
        conn.close()
