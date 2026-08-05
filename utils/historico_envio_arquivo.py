"""Histórico de envios de arquivo por projeto (cada upload gera um registro)."""

from __future__ import annotations

from db.connection import conectar_banco
from logger import logger


def _tabela_existe(cursor, nome):
    cursor.execute(
        "SELECT 1 FROM sys.tables WHERE name = ? AND schema_id = SCHEMA_ID('dbo')",
        (nome,),
    )
    return cursor.fetchone() is not None


def garantir_tabela_historico_envio(cursor, conn=None):
    if _tabela_existe(cursor, "HistoricoEnvioArquivo"):
        return
    cursor.execute(
        """
        CREATE TABLE dbo.HistoricoEnvioArquivo (
            HistoricoEnvioArquivoID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            ProjetoID INT NOT NULL,
            LayoutID INT NULL,
            NomeLayout NVARCHAR(200) NULL,
            NomeArquivo NVARCHAR(260) NULL,
            Modo NVARCHAR(30) NOT NULL,
            TotalLinhas INT NOT NULL CONSTRAINT DF_HistoricoEnvio_Linhas DEFAULT (0),
            TotalErros INT NOT NULL CONSTRAINT DF_HistoricoEnvio_Erros DEFAULT (0),
            TotalAvisos INT NOT NULL CONSTRAINT DF_HistoricoEnvio_Avisos DEFAULT (0),
            ImportacaoRealizada BIT NOT NULL CONSTRAINT DF_HistoricoEnvio_Imp DEFAULT (0),
            ProcessId NVARCHAR(64) NULL,
            UsuarioID INT NULL,
            UsuarioNome NVARCHAR(200) NULL,
            DataEnvio DATETIME NOT NULL
                CONSTRAINT DF_HistoricoEnvio_Data DEFAULT (GETDATE())
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IX_HistoricoEnvioArquivo_Projeto_Data "
        "ON dbo.HistoricoEnvioArquivo (ProjetoID, DataEnvio DESC)"
    )
    logger.info("Tabela HistoricoEnvioArquivo criada")
    if conn:
        conn.commit()


def registrar_envio_arquivo(
    projeto_id,
    *,
    layout_id=None,
    nome_layout=None,
    nome_arquivo=None,
    modo="importacao",
    total_linhas=0,
    total_erros=0,
    total_avisos=0,
    importacao_realizada=False,
    process_id=None,
    usuario_id=None,
    usuario_nome=None,
):
    """Grava um envio (sempre INSERT — reenvio cria novo registro)."""
    if not projeto_id:
        return None

    conn = conectar_banco()
    if not conn:
        logger.warning("Não foi possível gravar histórico de envio: sem conexão")
        return None

    cursor = conn.cursor()
    try:
        garantir_tabela_historico_envio(cursor, conn)
        cursor.execute(
            """
            INSERT INTO HistoricoEnvioArquivo (
                ProjetoID, LayoutID, NomeLayout, NomeArquivo, Modo,
                TotalLinhas, TotalErros, TotalAvisos, ImportacaoRealizada,
                ProcessId, UsuarioID, UsuarioNome, DataEnvio
            )
            OUTPUT INSERTED.HistoricoEnvioArquivoID
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            """,
            (
                int(projeto_id),
                int(layout_id) if layout_id else None,
                (nome_layout or "")[:200] or None,
                (nome_arquivo or "")[:260] or None,
                (modo or "importacao")[:30],
                int(total_linhas or 0),
                int(total_erros or 0),
                int(total_avisos or 0),
                1 if importacao_realizada else 0,
                (str(process_id) if process_id else None),
                int(usuario_id) if usuario_id else None,
                (usuario_nome or "")[:200] or None,
            ),
        )
        row = cursor.fetchone()
        conn.commit()
        historico_id = int(row[0]) if row else None
        logger.info(
            "Histórico de envio gravado: id=%s projeto=%s layout=%s arquivo=%s",
            historico_id,
            projeto_id,
            nome_layout,
            nome_arquivo,
        )
        return historico_id
    except Exception as exc:
        logger.error("Erro ao gravar histórico de envio: %s", exc)
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        cursor.close()
        conn.close()


def listar_historico_envio(projeto_id, limite=200):
    """Lista envios do projeto (mais recentes primeiro)."""
    if not projeto_id:
        return []

    conn = conectar_banco()
    if not conn:
        return []

    cursor = conn.cursor()
    try:
        garantir_tabela_historico_envio(cursor, conn)
        limite = max(1, min(int(limite or 200), 1000))
        cursor.execute(
            f"""
            SELECT TOP ({limite})
                HistoricoEnvioArquivoID,
                ProjetoID,
                LayoutID,
                NomeLayout,
                NomeArquivo,
                Modo,
                TotalLinhas,
                TotalErros,
                TotalAvisos,
                ImportacaoRealizada,
                ProcessId,
                UsuarioID,
                UsuarioNome,
                DataEnvio
            FROM HistoricoEnvioArquivo
            WHERE ProjetoID = ?
            ORDER BY DataEnvio DESC, HistoricoEnvioArquivoID DESC
            """,
            (int(projeto_id),),
        )
        rows = []
        for r in cursor.fetchall():
            data = r.DataEnvio
            rows.append({
                "HistoricoEnvioArquivoID": r.HistoricoEnvioArquivoID,
                "ProjetoID": r.ProjetoID,
                "LayoutID": r.LayoutID,
                "NomeLayout": r.NomeLayout or "",
                "NomeArquivo": r.NomeArquivo or "",
                "Modo": r.Modo or "",
                "TotalLinhas": int(r.TotalLinhas or 0),
                "TotalErros": int(r.TotalErros or 0),
                "TotalAvisos": int(r.TotalAvisos or 0),
                "ImportacaoRealizada": bool(r.ImportacaoRealizada),
                "ProcessId": r.ProcessId or "",
                "UsuarioID": r.UsuarioID,
                "UsuarioNome": r.UsuarioNome or "",
                "DataEnvio": data,
                "DataEnvioFmt": (
                    data.strftime("%d/%m/%Y %H:%M:%S")
                    if data and hasattr(data, "strftime")
                    else (str(data) if data else "")
                ),
            })
        return rows
    except Exception as exc:
        logger.error("Erro ao listar histórico de envio: %s", exc)
        return []
    finally:
        cursor.close()
        conn.close()
