"""Controle de layouts importados por projeto + status dos obrigatórios por escopo."""

from __future__ import annotations

from flask import has_request_context, session

from db.connection import conectar_banco
from logger import logger
from utils.layout_escopo import TIPOS_ESCOPO_LAYOUT, garantir_colunas_layout_escopo, label_tipo_escopo


def _tabela_existe(cursor, nome):
    cursor.execute(
        "SELECT 1 FROM sys.tables WHERE name = ? AND schema_id = SCHEMA_ID('dbo')",
        (nome,),
    )
    return cursor.fetchone() is not None


def garantir_tabela_layout_importacao(cursor, conn=None):
    if _tabela_existe(cursor, "LayoutImportacaoProjeto"):
        return
    cursor.execute(
        """
        CREATE TABLE dbo.LayoutImportacaoProjeto (
            LayoutImportacaoProjetoID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            ProjetoID INT NOT NULL,
            LayoutID INT NOT NULL,
            NomeLayout NVARCHAR(200) NULL,
            TipoEscopo NVARCHAR(50) NULL,
            DataUltimaImportacao DATETIME NOT NULL
                CONSTRAINT DF_LayoutImportacaoProjeto_Data DEFAULT (GETDATE()),
            UsuarioID INT NULL,
            CONSTRAINT UQ_LayoutImportacaoProjeto UNIQUE (ProjetoID, LayoutID)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IX_LayoutImportacaoProjeto_Projeto "
        "ON dbo.LayoutImportacaoProjeto (ProjetoID)"
    )
    logger.info("Tabela LayoutImportacaoProjeto criada")
    if conn:
        conn.commit()


def registrar_layout_importado(
    projeto_id,
    layout_id,
    nome_layout=None,
    tipo_escopo=None,
    usuario_id=None,
):
    """Marca o layout como importado para o projeto (após importação OK)."""
    if not projeto_id or not layout_id:
        return False

    if usuario_id is None and has_request_context():
        usuario_id = session.get("usuario", {}).get("usuario_id")

    conn = conectar_banco()
    if not conn:
        logger.warning("Não foi possível registrar layout importado: sem conexão")
        return False

    cursor = conn.cursor()
    try:
        garantir_tabela_layout_importacao(cursor, conn)
        garantir_colunas_layout_escopo(cursor, conn)

        if not tipo_escopo or not nome_layout:
            cursor.execute(
                "SELECT NomeLayout, TipoEscopo FROM Layouts WHERE LayoutID = ?",
                (int(layout_id),),
            )
            row = cursor.fetchone()
            if row:
                nome_layout = nome_layout or row.NomeLayout
                tipo_escopo = tipo_escopo or row.TipoEscopo

        cursor.execute(
            """
            MERGE dbo.LayoutImportacaoProjeto AS alvo
            USING (SELECT ? AS ProjetoID, ? AS LayoutID) AS origem
                ON alvo.ProjetoID = origem.ProjetoID
               AND alvo.LayoutID = origem.LayoutID
            WHEN MATCHED THEN
                UPDATE SET
                    DataUltimaImportacao = GETDATE(),
                    NomeLayout = ?,
                    TipoEscopo = ?,
                    UsuarioID = ?
            WHEN NOT MATCHED THEN
                INSERT (ProjetoID, LayoutID, NomeLayout, TipoEscopo,
                        DataUltimaImportacao, UsuarioID)
                VALUES (?, ?, ?, ?, GETDATE(), ?);
            """,
            (
                int(projeto_id),
                int(layout_id),
                nome_layout,
                tipo_escopo,
                usuario_id,
                int(projeto_id),
                int(layout_id),
                nome_layout,
                tipo_escopo,
                usuario_id,
            ),
        )
        conn.commit()
        logger.info(
            "Layout importado registrado: projeto=%s layout=%s",
            projeto_id,
            layout_id,
        )
        return True
    except Exception as exc:
        logger.error("Erro ao registrar layout importado: %s", exc)
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def ids_layouts_importados(projeto_id) -> set[int]:
    if not projeto_id:
        return set()
    conn = conectar_banco()
    if not conn:
        return set()
    cursor = conn.cursor()
    try:
        garantir_tabela_layout_importacao(cursor, conn)
        cursor.execute(
            "SELECT LayoutID FROM LayoutImportacaoProjeto WHERE ProjetoID = ?",
            (int(projeto_id),),
        )
        return {int(r[0]) for r in cursor.fetchall()}
    except Exception as exc:
        logger.warning("Erro ao listar layouts importados: %s", exc)
        return set()
    finally:
        cursor.close()
        conn.close()


def obter_status_layouts_obrigatorios(projeto_id, escopos_habilitados):
    """
    Retorna status dos layouts obrigatórios por escopo do projeto.

    {
      'ativo': True,
      'total_obrigatorios': N,
      'total_importados': M,
      'total_pendentes': N-M,
      'completo': bool,
      'escopos': [
        {
          'codigo': 'PESSOA',
          'nome': 'Pessoa',
          'total': 2,
          'importados': 1,
          'pendentes': 1,
          'completo': False,
          'layouts': [
            {'layout_id': 1, 'nome': '...', 'importado': True/False, 'data': '...'}
          ]
        },
        ...
      ],
      'faltando': [{'layout_id', 'nome', 'escopo', 'escopo_nome'}, ...]
    }
    """
    vazio = {
        "ativo": False,
        "total_obrigatorios": 0,
        "total_importados": 0,
        "total_pendentes": 0,
        "completo": True,
        "escopos": [],
        "faltando": [],
    }

    escopos = [str(e).strip().upper() for e in (escopos_habilitados or []) if str(e).strip()]
    if not projeto_id or not escopos:
        return vazio

    conn = conectar_banco()
    if not conn:
        return vazio

    cursor = conn.cursor()
    try:
        garantir_colunas_layout_escopo(cursor, conn)
        garantir_tabela_layout_importacao(cursor, conn)

        placeholders = ",".join("?" for _ in escopos)
        cursor.execute(
            f"""
            SELECT LayoutID, NomeLayout, TipoEscopo,
                   CAST(ISNULL(ObrigatorioNoEscopo, 0) AS INT) AS ObrigatorioNoEscopo
            FROM Layouts
            WHERE TipoEscopo IN ({placeholders})
            ORDER BY TipoEscopo, ObrigatorioNoEscopo DESC, NomeLayout
            """,
            tuple(escopos),
        )
        layouts = cursor.fetchall()

        cursor.execute(
            """
            SELECT LayoutID, DataUltimaImportacao
            FROM LayoutImportacaoProjeto
            WHERE ProjetoID = ?
            """,
            (int(projeto_id),),
        )
        importados_map = {}
        for row in cursor.fetchall():
            data = row.DataUltimaImportacao
            importados_map[int(row.LayoutID)] = (
                data.strftime("%d/%m/%Y %H:%M")
                if data and hasattr(data, "strftime")
                else (str(data) if data else "")
            )

        # Fallback legado: BlocoDeParaDisponivel.NomeLayout (importações anteriores)
        nomes_bloco = {}
        try:
            cursor.execute(
                """
                SELECT 1 FROM sys.tables
                WHERE name = 'BlocoDeParaDisponivel' AND schema_id = SCHEMA_ID('dbo')
                """
            )
            if cursor.fetchone():
                cursor.execute(
                    """
                    SELECT BlocoEscopo, NomeLayout, DataUltimaImportacao
                    FROM BlocoDeParaDisponivel
                    WHERE ProjetoID = ?
                    """,
                    (int(projeto_id),),
                )
                for row in cursor.fetchall():
                    nome = (row.NomeLayout or "").strip().lower()
                    if not nome:
                        continue
                    data = row.DataUltimaImportacao
                    nomes_bloco[nome] = (
                        data.strftime("%d/%m/%Y %H:%M")
                        if data and hasattr(data, "strftime")
                        else (str(data) if data else "")
                    )
        except Exception:
            nomes_bloco = {}

        def _item_layout(row):
            lid = int(row.LayoutID)
            nome = row.NomeLayout or f"Layout #{lid}"
            importado = lid in importados_map
            data_txt = importados_map.get(lid, "")
            if not importado:
                data_legado = nomes_bloco.get(nome.strip().lower())
                if data_legado is not None:
                    importado = True
                    data_txt = data_legado
            return {
                "layout_id": lid,
                "nome": nome,
                "importado": importado,
                "data": data_txt,
                "obrigatorio": bool(row.ObrigatorioNoEscopo),
            }

        por_escopo_obr = {cod: [] for cod in escopos}
        por_escopo_opc = {cod: [] for cod in escopos}
        faltando = []
        total_obr = 0
        total_ok = 0

        for row in layouts:
            codigo = (row.TipoEscopo or "").strip().upper()
            if codigo not in por_escopo_obr:
                continue
            item = _item_layout(row)
            if item["obrigatorio"]:
                por_escopo_obr[codigo].append(item)
                total_obr += 1
                if item["importado"]:
                    total_ok += 1
                else:
                    faltando.append({
                        "layout_id": item["layout_id"],
                        "nome": item["nome"],
                        "escopo": codigo,
                        "escopo_nome": label_tipo_escopo(codigo),
                    })
            else:
                por_escopo_opc[codigo].append(item)

        def _faixa_cor(pct):
            if pct >= 100:
                return "verde"
            if pct >= 50:
                return "amarelo"
            return "vermelho"

        escopos_out = []
        for codigo in escopos:
            obr = por_escopo_obr.get(codigo) or []
            opc = por_escopo_opc.get(codigo) or []
            # Só exibe escopo do projeto se houver layout vinculado a ele
            if not obr and not opc:
                continue

            nome_esc = label_tipo_escopo(codigo) or TIPOS_ESCOPO_LAYOUT.get(codigo, codigo)
            ok = sum(1 for i in obr if i["importado"])
            pend = len(obr) - ok
            if len(obr) == 0:
                pct = 100  # só opcionais — % dos obrigatórios fica 100
                sem_obr = True
            else:
                pct = int(round((ok / len(obr)) * 100))
                sem_obr = False

            escopos_out.append({
                "codigo": codigo,
                "nome": nome_esc,
                "total": len(obr),
                "importados": ok,
                "pendentes": pend,
                "percentual": pct,
                "cor": _faixa_cor(pct),
                "completo": pend == 0 and len(obr) > 0,
                "sem_obrigatorios": sem_obr,
                "layouts": obr,
                "layouts_opcionais": opc,
            })

        if not escopos_out:
            return {
                **vazio,
                "ativo": False,
            }

        return {
            "ativo": True,
            "total_obrigatorios": total_obr,
            "total_importados": total_ok,
            "total_pendentes": total_obr - total_ok,
            "completo": (total_obr - total_ok) == 0,
            "escopos": escopos_out,
            "faltando": faltando,
        }
    except Exception as exc:
        logger.error("Erro ao montar status de layouts obrigatórios: %s", exc)
        return vazio
    finally:
        cursor.close()
        conn.close()
