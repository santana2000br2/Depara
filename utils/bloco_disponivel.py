"""Registra quando cada bloco De/Para ficou disponível após importação do layout."""

from datetime import datetime

from flask import has_request_context, session

from db.connection import conectar_banco
from logger import logger
from utils.depara_escopos import CATEGORIAS_NOMES, blocos_do_layout

BLOCOS_VALIDOS = frozenset(CATEGORIAS_NOMES.keys())
ORIGEM_IMPORTACAO = "importacao"
ORIGEM_MANUAL = "manual"


def _tabela_existe(cursor, nome_tabela):
    cursor.execute(
        "SELECT 1 FROM sys.tables WHERE name = ? AND schema_id = SCHEMA_ID('dbo')",
        (nome_tabela,),
    )
    return cursor.fetchone() is not None


def _garantir_coluna_origem(cursor):
    cursor.execute(
        """
        IF COL_LENGTH('dbo.BlocoDeParaDisponivel', 'OrigemData') IS NULL
            ALTER TABLE dbo.BlocoDeParaDisponivel ADD OrigemData NVARCHAR(20) NULL
        """
    )


def garantir_tabela(cursor):
    """Cria a tabela BlocoDeParaDisponivel no banco principal se ainda não existir."""
    criada = False
    if not _tabela_existe(cursor, "BlocoDeParaDisponivel"):
        criada = True
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
                OrigemData NVARCHAR(20) NULL,
                CONSTRAINT UQ_BlocoDeParaDisponivel UNIQUE (ProjetoID, BlocoEscopo)
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IX_BlocoDeParaDisponivel_Projeto "
            "ON dbo.BlocoDeParaDisponivel (ProjetoID)"
        )
        logger.info("Tabela BlocoDeParaDisponivel criada automaticamente.")
    _garantir_coluna_origem(cursor)
    return criada


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
                            TipoLayout, NomeLayout, UsuarioID, OrigemData)
                    VALUES (?, ?, GETDATE(), GETDATE(), ?, ?, ?, ?);
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
                    ORIGEM_IMPORTACAO,
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
            SELECT BlocoEscopo, DataDisponivel, DataUltimaImportacao,
                   TipoLayout, NomeLayout, OrigemData
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
                "OrigemData": row[5] or ORIGEM_IMPORTACAO,
            }
        return resultado
    except Exception as exc:
        logger.error(f"Erro ao obter datas dos blocos do projeto {projeto_id}: {exc}")
        return {}
    finally:
        conn.close()


def parse_data_disponivel(valor):
    """Aceita formatos ISO e BR, com ou sem hora."""
    if not valor:
        return None
    texto = str(valor).strip()
    # Normaliza separadores comuns para facilitar o parse.
    texto_norm = texto.replace(".", "/").replace("-", "/")

    # Tenta primeiro os formatos explícitos com hora e ISO.
    for fmt in (
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%y %H:%M",
    ):
        try:
            return datetime.strptime(texto, fmt)
        except ValueError:
            pass
        try:
            return datetime.strptime(texto_norm, fmt)
        except ValueError:
            continue

    # Suporte a dia/mês com 1 dígito (ex: 5/8/2026).
    br = texto_norm.split(" ")
    data_part = br[0] if br else ""
    hora_part = br[1] if len(br) > 1 else None
    partes = data_part.split("/")
    if len(partes) == 3 and all(p.isdigit() for p in partes):
        dia, mes, ano = partes
        if len(ano) == 2:
            ano = f"20{ano}"
        try:
            d = int(dia)
            m = int(mes)
            y = int(ano)
            if hora_part:
                hhmmss = hora_part.split(":")
                hh = int(hhmmss[0]) if len(hhmmss) > 0 else 0
                mm = int(hhmmss[1]) if len(hhmmss) > 1 else 0
                ss = int(hhmmss[2]) if len(hhmmss) > 2 else 0
                return datetime(y, m, d, hh, mm, ss)
            return datetime(y, m, d)
        except Exception:
            pass
    return None


def salvar_data_manual(projeto_id, escopo, data_disponivel, usuario_id=None):
    """Define DataDisponivel de um bloco (projetos que não são Arquivo X Workflow)."""
    if not projeto_id or not data_disponivel:
        return False
    escopo = (escopo or "").strip().upper()
    if escopo not in BLOCOS_VALIDOS:
        return False

    conn = conectar_banco()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        garantir_tabela(cursor)
        cursor.execute(
            """
            MERGE dbo.BlocoDeParaDisponivel AS alvo
            USING (SELECT ? AS ProjetoID, ? AS BlocoEscopo) AS origem
                ON alvo.ProjetoID = origem.ProjetoID
               AND alvo.BlocoEscopo = origem.BlocoEscopo
            WHEN MATCHED THEN
                UPDATE SET
                    DataDisponivel = ?,
                    OrigemData = ?,
                    UsuarioID = ?
            WHEN NOT MATCHED THEN
                INSERT (ProjetoID, BlocoEscopo, DataDisponivel, DataUltimaImportacao,
                        TipoLayout, NomeLayout, UsuarioID, OrigemData)
                VALUES (?, ?, ?, ?, N'manual', NULL, ?, ?);
            """,
            (
                projeto_id,
                escopo,
                data_disponivel,
                ORIGEM_MANUAL,
                usuario_id,
                projeto_id,
                escopo,
                data_disponivel,
                data_disponivel,
                usuario_id,
                ORIGEM_MANUAL,
            ),
        )
        conn.commit()
        return True
    except Exception as exc:
        conn.rollback()
        logger.error(
            "Erro ao salvar data manual do bloco %s (projeto %s): %s",
            escopo,
            projeto_id,
            exc,
        )
        return False
    finally:
        conn.close()
