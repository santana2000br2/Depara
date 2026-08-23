"""Histórico de envios de arquivo por projeto (cada upload gera um registro)."""

from __future__ import annotations

import math

import pandas as pd

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
        garantir_tabela_historico_envio_erro(cursor, conn)
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
    garantir_tabela_historico_envio_erro(cursor, conn)
    if conn:
        conn.commit()


def garantir_tabela_historico_envio_erro(cursor, conn=None):
    """Tabela filha: erros/avisos de validação por envio (banco da aplicação)."""
    if _tabela_existe(cursor, "HistoricoEnvioArquivoErro"):
        return
    cursor.execute(
        """
        CREATE TABLE dbo.HistoricoEnvioArquivoErro (
            HistoricoEnvioArquivoErroID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            HistoricoEnvioArquivoID INT NOT NULL,
            ProjetoID INT NOT NULL,
            Tipo NVARCHAR(20) NOT NULL,
            Linha INT NULL,
            Coluna NVARCHAR(200) NULL,
            Mensagem NVARCHAR(1000) NOT NULL,
            DataRegistro DATETIME NOT NULL
                CONSTRAINT DF_HistEnvioErro_Data DEFAULT (GETDATE()),
            CONSTRAINT FK_HistEnvioErro_Envio
                FOREIGN KEY (HistoricoEnvioArquivoID)
                REFERENCES dbo.HistoricoEnvioArquivo (HistoricoEnvioArquivoID)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IX_HistEnvioErro_Envio "
        "ON dbo.HistoricoEnvioArquivoErro (HistoricoEnvioArquivoID)"
    )
    cursor.execute(
        "CREATE INDEX IX_HistEnvioErro_Projeto_Data "
        "ON dbo.HistoricoEnvioArquivoErro (ProjetoID, DataRegistro DESC)"
    )
    logger.info("Tabela HistoricoEnvioArquivoErro criada")
    if conn:
        conn.commit()


MAX_DETALHES_ERRO_HISTORICO = 10000


def _salvar_detalhes_erro_historico(cursor, historico_id, projeto_id, df_erros=None, df_avisos=None):
    """Insere linhas de erros/avisos vinculadas ao histórico do envio."""
    if not historico_id or not projeto_id:
        return 0

    garantir_tabela_historico_envio_erro(cursor)
    registros = []

    def _linha_int(valor):
        try:
            if valor is None or (isinstance(valor, float) and math.isnan(valor)):
                return None
            if isinstance(valor, str) and not valor.strip():
                return None
            return int(float(valor))
        except (TypeError, ValueError):
            return None

    def _texto(valor, limite):
        if valor is None:
            return None
        try:
            if pd.isna(valor):
                return None
        except (TypeError, ValueError):
            pass
        txt = str(valor).strip()
        return txt[:limite] if txt else None

    if df_erros is not None and not getattr(df_erros, "empty", True):
        for _, row in df_erros.iterrows():
            msg = _texto(row.get("Erro"), 1000)
            if not msg:
                continue
            registros.append((
                int(historico_id),
                int(projeto_id),
                "erro",
                _linha_int(row.get("Linha")),
                _texto(row.get("Coluna"), 200),
                msg,
            ))
            if len(registros) >= MAX_DETALHES_ERRO_HISTORICO:
                break

    if df_avisos is not None and not getattr(df_avisos, "empty", True):
        for _, row in df_avisos.iterrows():
            if len(registros) >= MAX_DETALHES_ERRO_HISTORICO:
                break
            msg = _texto(row.get("Aviso"), 1000)
            if not msg:
                continue
            registros.append((
                int(historico_id),
                int(projeto_id),
                "aviso",
                _linha_int(row.get("Linha")),
                _texto(row.get("Coluna"), 200),
                msg,
            ))

    if not registros:
        return 0

    if hasattr(cursor, "fast_executemany"):
        cursor.fast_executemany = True
    cursor.executemany(
        """
        INSERT INTO HistoricoEnvioArquivoErro (
            HistoricoEnvioArquivoID, ProjetoID, Tipo, Linha, Coluna, Mensagem, DataRegistro
        ) VALUES (?, ?, ?, ?, ?, ?, GETDATE())
        """,
        registros,
    )
    logger.info(
        "Histórico envio: %s detalhe(s) de erro/aviso gravados (historico_id=%s)",
        len(registros),
        historico_id,
    )
    return len(registros)


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
    df_erros=None,
    df_avisos=None,
):
    """Grava um envio (sempre INSERT — reenvio cria novo registro).

    Quando df_erros/df_avisos são informados, grava o detalhe em HistoricoEnvioArquivoErro.
    """
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
        historico_id = int(row[0]) if row else None
        if historico_id and (df_erros is not None or df_avisos is not None):
            _salvar_detalhes_erro_historico(
                cursor, historico_id, projeto_id, df_erros=df_erros, df_avisos=df_avisos,
            )
        conn.commit()
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
                h.HistoricoEnvioArquivoID,
                h.ProjetoID,
                h.LayoutID,
                h.NomeLayout,
                h.NomeArquivo,
                h.Modo,
                h.TotalLinhas,
                h.TotalErros,
                h.TotalAvisos,
                h.ImportacaoRealizada,
                h.ProcessId,
                h.UsuarioID,
                h.UsuarioNome,
                h.DataEnvio,
                (
                    SELECT COUNT(1)
                    FROM HistoricoEnvioArquivoErro e
                    WHERE e.HistoricoEnvioArquivoID = h.HistoricoEnvioArquivoID
                ) AS TotalDetalhes
            FROM HistoricoEnvioArquivo h
            WHERE h.ProjetoID = ?
            ORDER BY h.DataEnvio DESC, h.HistoricoEnvioArquivoID DESC
            """,
            (int(projeto_id),),
        )
        rows = []
        for r in cursor.fetchall():
            data = r.DataEnvio
            total_detalhes = int(getattr(r, "TotalDetalhes", 0) or 0)
            total_erros = int(r.TotalErros or 0)
            total_avisos = int(r.TotalAvisos or 0)
            rows.append({
                "HistoricoEnvioArquivoID": r.HistoricoEnvioArquivoID,
                "ProjetoID": r.ProjetoID,
                "LayoutID": r.LayoutID,
                "NomeLayout": r.NomeLayout or "",
                "NomeArquivo": r.NomeArquivo or "",
                "Modo": r.Modo or "",
                "TotalLinhas": int(r.TotalLinhas or 0),
                "TotalErros": total_erros,
                "TotalAvisos": total_avisos,
                "TotalDetalhes": total_detalhes,
                "TemDetalhesErro": total_detalhes > 0,
                "PodeVerErros": total_detalhes > 0 or total_erros > 0 or total_avisos > 0,
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


def obter_envio_historico(historico_id, projeto_id):
    """Retorna o cabeçalho de um envio do projeto, ou None."""
    if not historico_id or not projeto_id:
        return None

    conn = conectar_banco()
    if not conn:
        return None

    cursor = conn.cursor()
    try:
        garantir_tabela_historico_envio(cursor, conn)
        cursor.execute(
            """
            SELECT
                HistoricoEnvioArquivoID,
                ProjetoID,
                NomeLayout,
                NomeArquivo,
                Modo,
                TotalLinhas,
                TotalErros,
                TotalAvisos,
                ImportacaoRealizada,
                DataEnvio
            FROM HistoricoEnvioArquivo
            WHERE HistoricoEnvioArquivoID = ? AND ProjetoID = ?
            """,
            (int(historico_id), int(projeto_id)),
        )
        r = cursor.fetchone()
        if not r:
            return None
        data = r.DataEnvio
        return {
            "HistoricoEnvioArquivoID": r.HistoricoEnvioArquivoID,
            "ProjetoID": r.ProjetoID,
            "NomeLayout": r.NomeLayout or "",
            "NomeArquivo": r.NomeArquivo or "",
            "Modo": r.Modo or "",
            "TotalLinhas": int(r.TotalLinhas or 0),
            "TotalErros": int(r.TotalErros or 0),
            "TotalAvisos": int(r.TotalAvisos or 0),
            "ImportacaoRealizada": bool(r.ImportacaoRealizada),
            "DataEnvioFmt": (
                data.strftime("%d/%m/%Y %H:%M:%S")
                if data and hasattr(data, "strftime")
                else (str(data) if data else "")
            ),
        }
    except Exception as exc:
        logger.error("Erro ao obter histórico de envio %s: %s", historico_id, exc)
        return None
    finally:
        cursor.close()
        conn.close()


def listar_erros_historico_envio(historico_id, projeto_id, tipo=None, limite=1000):
    """Lista detalhes de erro/aviso de um envio (escopo do projeto).

    tipo: None|'erro'|'aviso'
    """
    if not historico_id or not projeto_id:
        return []

    conn = conectar_banco()
    if not conn:
        return []

    cursor = conn.cursor()
    try:
        garantir_tabela_historico_envio(cursor, conn)
        cursor.execute(
            """
            SELECT 1 FROM HistoricoEnvioArquivo
            WHERE HistoricoEnvioArquivoID = ? AND ProjetoID = ?
            """,
            (int(historico_id), int(projeto_id)),
        )
        if not cursor.fetchone():
            return []

        limite = max(1, min(int(limite or 1000), 10000))
        tipo_norm = (tipo or "").strip().lower()
        if tipo_norm in ("erro", "aviso"):
            cursor.execute(
                f"""
                SELECT TOP ({limite})
                    HistoricoEnvioArquivoErroID,
                    Tipo,
                    Linha,
                    Coluna,
                    Mensagem
                FROM HistoricoEnvioArquivoErro
                WHERE HistoricoEnvioArquivoID = ? AND ProjetoID = ? AND Tipo = ?
                ORDER BY
                    CASE WHEN Tipo = 'erro' THEN 0 ELSE 1 END,
                    Linha ASC,
                    HistoricoEnvioArquivoErroID ASC
                """,
                (int(historico_id), int(projeto_id), tipo_norm),
            )
        else:
            cursor.execute(
                f"""
                SELECT TOP ({limite})
                    HistoricoEnvioArquivoErroID,
                    Tipo,
                    Linha,
                    Coluna,
                    Mensagem
                FROM HistoricoEnvioArquivoErro
                WHERE HistoricoEnvioArquivoID = ? AND ProjetoID = ?
                ORDER BY
                    CASE WHEN Tipo = 'erro' THEN 0 ELSE 1 END,
                    Linha ASC,
                    HistoricoEnvioArquivoErroID ASC
                """,
                (int(historico_id), int(projeto_id)),
            )

        itens = []
        for r in cursor.fetchall():
            itens.append({
                "id": r.HistoricoEnvioArquivoErroID,
                "Tipo": (r.Tipo or "").strip().lower(),
                "Linha": r.Linha,
                "Coluna": r.Coluna or "",
                "Mensagem": r.Mensagem or "",
            })
        return itens
    except Exception as exc:
        logger.error("Erro ao listar erros do histórico %s: %s", historico_id, exc)
        return []
    finally:
        cursor.close()
        conn.close()


def contar_erros_historico_envio(historico_id, projeto_id):
    """Conta erros e avisos detalhados de um envio."""
    if not historico_id or not projeto_id:
        return {"erro": 0, "aviso": 0, "total": 0}

    conn = conectar_banco()
    if not conn:
        return {"erro": 0, "aviso": 0, "total": 0}

    cursor = conn.cursor()
    try:
        garantir_tabela_historico_envio(cursor, conn)
        cursor.execute(
            """
            SELECT Tipo, COUNT(1) AS Qtd
            FROM HistoricoEnvioArquivoErro
            WHERE HistoricoEnvioArquivoID = ? AND ProjetoID = ?
            GROUP BY Tipo
            """,
            (int(historico_id), int(projeto_id)),
        )
        contagem = {"erro": 0, "aviso": 0, "total": 0}
        for r in cursor.fetchall():
            tipo = (r.Tipo or "").strip().lower()
            qtd = int(r.Qtd or 0)
            if tipo in contagem:
                contagem[tipo] = qtd
            contagem["total"] += qtd
        return contagem
    except Exception as exc:
        logger.error("Erro ao contar erros do histórico %s: %s", historico_id, exc)
        return {"erro": 0, "aviso": 0, "total": 0}
    finally:
        cursor.close()
        conn.close()
