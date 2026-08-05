"""Versionamento de layouts.

Cada alteração de um layout gera um snapshot (versão) do estado anterior,
permitindo consultar o histórico e restaurar uma versão anterior quando preciso.

Modelo:
- O estado "ao vivo" fica nas tabelas ``Layouts`` / ``LayoutColunas``.
- Antes de aplicar uma alteração (edição ou restauração), o estado atual é
  copiado para ``LayoutVersoes`` / ``LayoutVersaoColunas`` como uma nova versão.
- Restaurar uma versão também guarda o estado atual antes de sobrescrever, de
  forma que a operação seja sempre reversível.
"""

from db.connection import conectar_banco
from logger import logger

TIPO_CRIACAO = "criacao"
TIPO_EDICAO = "edicao"
TIPO_RESTAURACAO = "restauracao"

TIPO_ACAO_LABEL = {
    TIPO_CRIACAO: "Criação",
    TIPO_EDICAO: "Edição",
    TIPO_RESTAURACAO: "Restauração",
}


def _tabela_existe(cursor, nome_tabela):
    cursor.execute(
        "SELECT 1 FROM sys.tables WHERE name = ? AND schema_id = SCHEMA_ID('dbo')",
        (nome_tabela,),
    )
    return cursor.fetchone() is not None


def garantir_tabelas(cursor):
    """Cria as tabelas de versionamento no banco principal caso não existam."""
    if not _tabela_existe(cursor, "LayoutVersoes"):
        cursor.execute(
            """
            CREATE TABLE dbo.LayoutVersoes (
                LayoutVersaoID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                LayoutID INT NOT NULL,
                Versao INT NOT NULL,
                NomeLayout NVARCHAR(100) NOT NULL,
                Descricao NVARCHAR(500) NULL,
                TipoAcao NVARCHAR(20) NOT NULL
                    CONSTRAINT DF_LayoutVersoes_TipoAcao DEFAULT ('edicao'),
                Observacao NVARCHAR(255) NULL,
                UsuarioVersao INT NULL,
                DataVersao DATETIME NOT NULL
                    CONSTRAINT DF_LayoutVersoes_Data DEFAULT (GETDATE()),
                CONSTRAINT UQ_LayoutVersoes_Layout_Versao UNIQUE (LayoutID, Versao)
            )
            """
        )
        logger.info("Tabela LayoutVersoes criada automaticamente.")

    if not _tabela_existe(cursor, "LayoutVersaoColunas"):
        cursor.execute(
            """
            CREATE TABLE dbo.LayoutVersaoColunas (
                LayoutVersaoColunaID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                LayoutVersaoID INT NOT NULL,
                Posicao INT NOT NULL,
                Descricao NVARCHAR(100) NOT NULL,
                Obrigatorio BIT NOT NULL
                    CONSTRAINT DF_LayoutVersaoColunas_Obrigatorio DEFAULT (0),
                Validacao NVARCHAR(100) NULL,
                TipoDado NVARCHAR(50) NOT NULL
                    CONSTRAINT DF_LayoutVersaoColunas_TipoDado DEFAULT ('texto'),
                CONSTRAINT FK_LayoutVersaoColunas_Versao
                    FOREIGN KEY (LayoutVersaoID) REFERENCES dbo.LayoutVersoes (LayoutVersaoID)
                    ON DELETE CASCADE
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IX_LayoutVersaoColunas_Versao "
            "ON dbo.LayoutVersaoColunas (LayoutVersaoID)"
        )
        logger.info("Tabela LayoutVersaoColunas criada automaticamente.")


def _proxima_versao(cursor, layout_id):
    cursor.execute(
        "SELECT ISNULL(MAX(Versao), 0) + 1 FROM dbo.LayoutVersoes WHERE LayoutID = ?",
        (layout_id,),
    )
    return int(cursor.fetchone()[0])


def registrar_snapshot(cursor, layout_id, tipo_acao=TIPO_EDICAO,
                       observacao=None, usuario_id=None):
    """Grava o estado atual do layout como uma nova versão.

    Deve ser chamado ANTES de aplicar a alteração, usando o mesmo cursor/transação
    da operação para garantir atomicidade. Retorna o número da versão criada ou
    ``None`` caso o layout não tenha colunas/estado a preservar.
    """
    garantir_tabelas(cursor)

    cursor.execute(
        "SELECT NomeLayout, Descricao FROM dbo.Layouts WHERE LayoutID = ?",
        (layout_id,),
    )
    layout = cursor.fetchone()
    if not layout:
        return None

    nome_layout, descricao = layout[0], layout[1]

    cursor.execute(
        """
        SELECT Posicao, Descricao, Obrigatorio, Validacao, TipoDado
        FROM dbo.LayoutColunas
        WHERE LayoutID = ?
        ORDER BY Posicao
        """,
        (layout_id,),
    )
    colunas = cursor.fetchall()

    versao = _proxima_versao(cursor, layout_id)

    cursor.execute(
        """
        INSERT INTO dbo.LayoutVersoes
            (LayoutID, Versao, NomeLayout, Descricao, TipoAcao, Observacao, UsuarioVersao, DataVersao)
        OUTPUT INSERTED.LayoutVersaoID
        VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
        """,
        (layout_id, versao, nome_layout, descricao, tipo_acao, observacao, usuario_id),
    )
    layout_versao_id = cursor.fetchone()[0]

    for coluna in colunas:
        cursor.execute(
            """
            INSERT INTO dbo.LayoutVersaoColunas
                (LayoutVersaoID, Posicao, Descricao, Obrigatorio, Validacao, TipoDado)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (layout_versao_id, coluna[0], coluna[1], coluna[2], coluna[3], coluna[4]),
        )

    logger.info(
        f"Snapshot do layout {layout_id} salvo como versão {versao} "
        f"({tipo_acao}, {len(colunas)} coluna(s))."
    )
    return versao


def listar_versoes(layout_id):
    """Retorna as versões de um layout, da mais recente para a mais antiga."""
    conn = conectar_banco()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        garantir_tabelas(cursor)
        conn.commit()
        cursor.execute(
            """
            SELECT v.LayoutVersaoID, v.Versao, v.NomeLayout, v.Descricao,
                   v.TipoAcao, v.Observacao, v.UsuarioVersao, v.DataVersao,
                   (SELECT COUNT(*) FROM dbo.LayoutVersaoColunas c
                     WHERE c.LayoutVersaoID = v.LayoutVersaoID) AS TotalColunas
            FROM dbo.LayoutVersoes v
            WHERE v.LayoutID = ?
            ORDER BY v.Versao DESC
            """,
            (layout_id,),
        )
        versoes = []
        for row in cursor.fetchall():
            versoes.append(
                {
                    "LayoutVersaoID": row[0],
                    "Versao": row[1],
                    "NomeLayout": row[2],
                    "Descricao": row[3],
                    "TipoAcao": row[4],
                    "TipoAcaoLabel": TIPO_ACAO_LABEL.get(row[4], row[4]),
                    "Observacao": row[5],
                    "UsuarioVersao": row[6],
                    "DataVersao": row[7],
                    "TotalColunas": row[8],
                }
            )
        return versoes
    except Exception as exc:
        logger.error(f"Erro ao listar versões do layout {layout_id}: {exc}")
        return []
    finally:
        conn.close()


def obter_versao(layout_versao_id):
    """Retorna o cabeçalho e as colunas de uma versão específica."""
    conn = conectar_banco()
    if not conn:
        return None

    try:
        cursor = conn.cursor()
        garantir_tabelas(cursor)
        conn.commit()
        cursor.execute(
            """
            SELECT LayoutVersaoID, LayoutID, Versao, NomeLayout, Descricao,
                   TipoAcao, Observacao, UsuarioVersao, DataVersao
            FROM dbo.LayoutVersoes
            WHERE LayoutVersaoID = ?
            """,
            (layout_versao_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        versao = {
            "LayoutVersaoID": row[0],
            "LayoutID": row[1],
            "Versao": row[2],
            "NomeLayout": row[3],
            "Descricao": row[4],
            "TipoAcao": row[5],
            "TipoAcaoLabel": TIPO_ACAO_LABEL.get(row[5], row[5]),
            "Observacao": row[6],
            "UsuarioVersao": row[7],
            "DataVersao": row[8],
            "colunas": [],
        }

        cursor.execute(
            """
            SELECT Posicao, Descricao, Obrigatorio, Validacao, TipoDado
            FROM dbo.LayoutVersaoColunas
            WHERE LayoutVersaoID = ?
            ORDER BY Posicao
            """,
            (layout_versao_id,),
        )
        for coluna in cursor.fetchall():
            versao["colunas"].append(
                {
                    "Posicao": coluna[0],
                    "Descricao": coluna[1],
                    "Obrigatorio": coluna[2],
                    "Validacao": coluna[3],
                    "TipoDado": coluna[4],
                }
            )
        return versao
    except Exception as exc:
        logger.error(f"Erro ao obter versão {layout_versao_id}: {exc}")
        return None
    finally:
        conn.close()


def restaurar_versao(layout_versao_id, usuario_id=None):
    """Restaura o layout para o estado de uma versão.

    Antes de sobrescrever, guarda o estado atual como uma nova versão para que a
    restauração também seja reversível. Retorna (sucesso, mensagem).
    """
    conn = conectar_banco()
    if not conn:
        return False, "Erro ao conectar com o banco de dados"

    try:
        cursor = conn.cursor()
        garantir_tabelas(cursor)

        # Dados da versão a restaurar
        cursor.execute(
            """
            SELECT LayoutID, Versao, NomeLayout, Descricao
            FROM dbo.LayoutVersoes
            WHERE LayoutVersaoID = ?
            """,
            (layout_versao_id,),
        )
        versao = cursor.fetchone()
        if not versao:
            return False, "Versão não encontrada"

        layout_id, numero_versao, nome_layout, descricao = versao

        cursor.execute(
            "SELECT 1 FROM dbo.Layouts WHERE LayoutID = ?",
            (layout_id,),
        )
        if not cursor.fetchone():
            return False, "O layout desta versão não existe mais"

        # Preserva o estado atual antes de sobrescrever
        registrar_snapshot(
            cursor,
            layout_id,
            tipo_acao=TIPO_RESTAURACAO,
            observacao=f"Estado antes de restaurar a versão {numero_versao}",
            usuario_id=usuario_id,
        )

        # Sobrescreve o estado ao vivo com o snapshot da versão
        cursor.execute(
            "UPDATE dbo.Layouts SET NomeLayout = ?, Descricao = ? WHERE LayoutID = ?",
            (nome_layout, descricao, layout_id),
        )
        cursor.execute("DELETE FROM dbo.LayoutColunas WHERE LayoutID = ?", (layout_id,))

        cursor.execute(
            """
            SELECT Posicao, Descricao, Obrigatorio, Validacao, TipoDado
            FROM dbo.LayoutVersaoColunas
            WHERE LayoutVersaoID = ?
            ORDER BY Posicao
            """,
            (layout_versao_id,),
        )
        colunas = cursor.fetchall()
        for coluna in colunas:
            cursor.execute(
                """
                INSERT INTO dbo.LayoutColunas
                    (LayoutID, Posicao, Descricao, Obrigatorio, Validacao, TipoDado)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (layout_id, coluna[0], coluna[1], coluna[2], coluna[3], coluna[4]),
            )

        conn.commit()
        logger.info(
            f"Layout {layout_id} restaurado para o estado da versão {numero_versao}."
        )
        return True, f"Layout restaurado para a versão {numero_versao}"
    except Exception as exc:
        conn.rollback()
        logger.error(f"Erro ao restaurar versão {layout_versao_id}: {exc}")
        return False, f"Erro ao restaurar versão: {exc}"
    finally:
        conn.close()
