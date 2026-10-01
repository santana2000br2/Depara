"""Tabela de controle das importações de arquivo (banco da aplicação)."""

from __future__ import annotations

from datetime import datetime

from logger import logger

STATUS_RECEBENDO = "RECEBENDO"
STATUS_AGUARDANDO = "AGUARDANDO_PROCESSAMENTO"
STATUS_PROCESSANDO = "PROCESSANDO"
STATUS_CONCLUIDO = "CONCLUIDO"
STATUS_CONCLUIDO_ERROS = "CONCLUIDO_COM_ERROS"
STATUS_ERRO = "ERRO"
STATUS_CANCELADO = "CANCELADO"

_CAMPOS_ATUALIZAVEIS = {
    "JobId",
    "ProcessId",
    "Status",
    "Percentual",
    "Mensagem",
    "MensagemErro",
    "TotalRegistros",
    "RegistrosProcessados",
    "RegistrosImportados",
    "RegistrosComErro",
    "DataFimUpload",
    "DataInicioProcessamento",
    "DataFimProcessamento",
}

_importacao_ativa = None
_tabela_confirmada = False


def _como_datetime(valor):
    if valor is None or valor == "":
        return None
    if isinstance(valor, datetime):
        return valor
    texto = str(valor).strip()
    if not texto:
        return None
    try:
        return datetime.fromisoformat(texto.replace("Z", "+00:00")[:26])
    except ValueError:
        return None


def epoch_local(valor):
    dt = _como_datetime(valor)
    if not dt:
        return None
    try:
        return int(dt.timestamp())
    except (OSError, OverflowError, TypeError, ValueError):
        return None


def duracao_segundos(inicio, fim=None):
    ini = _como_datetime(inicio)
    if not ini:
        return None
    termino = _como_datetime(fim) or datetime.now()
    try:
        return max(0, int((termino - ini).total_seconds()))
    except (TypeError, OverflowError):
        return None


def formatar_duracao(segundos):
    if segundos is None:
        return ""
    try:
        total = max(0, int(segundos))
    except (TypeError, ValueError):
        return ""
    horas, resto = divmod(total, 3600)
    minutos, segs = divmod(resto, 60)
    if horas:
        return f"{horas} h {minutos:02d} min {segs:02d} s"
    if minutos:
        return f"{minutos} min {segs:02d} s"
    return f"{segs} s"


def _tabela_existe(cursor, nome):
    cursor.execute(
        "SELECT 1 FROM sys.tables WHERE name = ? AND schema_id = SCHEMA_ID('dbo')",
        (nome,),
    )
    return cursor.fetchone() is not None


def garantir_tabela_importacao_arquivo(cursor, conn=None):
    global _tabela_confirmada
    if _tabela_confirmada:
        return
    if _tabela_existe(cursor, "ImportacaoArquivo"):
        _tabela_confirmada = True
        return
    cursor.execute("SET NOCOUNT ON")
    cursor.execute(
        """
        CREATE TABLE dbo.ImportacaoArquivo (
            ImportacaoArquivoID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            ProjetoID INT NOT NULL,
            NomeProjeto NVARCHAR(200) NULL,
            LayoutID INT NULL,
            NomeLayout NVARCHAR(200) NULL,
            NomeArquivoOriginal NVARCHAR(260) NULL,
            NomeArquivoFisico NVARCHAR(260) NULL,
            CaminhoArquivo NVARCHAR(1000) NULL,
            JobId NVARCHAR(64) NULL,
            ProcessId NVARCHAR(64) NULL,
            UsuarioID INT NULL,
            UsuarioNome NVARCHAR(200) NULL,
            SomenteValidacao BIT NOT NULL CONSTRAINT DF_ImpArq_Val DEFAULT (0),
            DataInicioUpload DATETIME NOT NULL CONSTRAINT DF_ImpArq_IniUp DEFAULT (GETDATE()),
            DataFimUpload DATETIME NULL,
            DataInicioProcessamento DATETIME NULL,
            DataFimProcessamento DATETIME NULL,
            Status NVARCHAR(40) NOT NULL CONSTRAINT DF_ImpArq_St DEFAULT ('RECEBENDO'),
            Percentual DECIMAL(5,2) NOT NULL CONSTRAINT DF_ImpArq_Pct DEFAULT (0),
            TotalRegistros INT NOT NULL CONSTRAINT DF_ImpArq_Tot DEFAULT (0),
            RegistrosProcessados INT NOT NULL CONSTRAINT DF_ImpArq_Proc DEFAULT (0),
            RegistrosImportados INT NOT NULL CONSTRAINT DF_ImpArq_Imp DEFAULT (0),
            RegistrosComErro INT NOT NULL CONSTRAINT DF_ImpArq_Err DEFAULT (0),
            Mensagem NVARCHAR(500) NULL,
            MensagemErro NVARCHAR(1000) NULL
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IX_ImportacaoArquivo_Projeto "
        "ON dbo.ImportacaoArquivo (ProjetoID, ImportacaoArquivoID DESC)"
    )
    logger.info("Tabela ImportacaoArquivo criada")
    if conn:
        conn.commit()
    _tabela_confirmada = True


def criar_importacao(
    projeto_id,
    nome_projeto,
    layout_id,
    nome_layout,
    nome_original,
    nome_fisico,
    caminho,
    usuario_id=None,
    usuario_nome=None,
    somente_validacao=False,
    job_id=None,
    status=STATUS_AGUARDANDO,
    mensagem="Arquivo recebido. Aguardando processamento.",
):
    from db.connection import conectar_banco

    conn = conectar_banco()
    if not conn:
        raise RuntimeError("Não foi possível conectar ao banco para registrar a importação.")
    try:
        cursor = conn.cursor()
        garantir_tabela_importacao_arquivo(cursor, conn)
        cursor.execute("SET NOCOUNT ON")
        cursor.execute(
            """
            INSERT INTO dbo.ImportacaoArquivo (
                ProjetoID, NomeProjeto, LayoutID, NomeLayout,
                NomeArquivoOriginal, NomeArquivoFisico, CaminhoArquivo,
                JobId, UsuarioID, UsuarioNome, SomenteValidacao,
                DataFimUpload, Status, Percentual, Mensagem
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, 5, ?)
            """,
            (
                projeto_id,
                (nome_projeto or "")[:200],
                layout_id,
                (nome_layout or "")[:200],
                (nome_original or "")[:260],
                (nome_fisico or "")[:260],
                (caminho or "")[:1000],
                job_id,
                usuario_id,
                (usuario_nome or "")[:200],
                1 if somente_validacao else 0,
                status,
                (mensagem or "")[:500],
            ),
        )
        ident = None
        try:
            cursor.execute("SELECT CAST(SCOPE_IDENTITY() AS INT)")
            row = cursor.fetchone()
            if row and row[0] is not None:
                ident = int(row[0])
        except Exception:
            ident = None
        if ident is None:
            cursor.execute(
                """
                SELECT MAX(ImportacaoArquivoID)
                FROM dbo.ImportacaoArquivo
                WHERE CaminhoArquivo = ?
                """,
                ((caminho or "")[:1000],),
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                ident = int(row[0])
        conn.commit()
        if not ident:
            raise RuntimeError("Não foi possível obter o ID da importação recém-gravada.")
        logger.info(
            "Importação criada: id=%s projeto=%s arquivo=%s",
            ident, nome_projeto, nome_fisico,
        )
        return ident
    finally:
        conn.close()


def atualizar_importacao(importacao_id, **kwargs):
    if not importacao_id:
        return
    sets = []
    params = []
    extras = list(kwargs.pop("_sql_extra", None) or [])
    for chave, valor in kwargs.items():
        if chave not in _CAMPOS_ATUALIZAVEIS or valor is None:
            continue
        if chave == "Mensagem":
            valor = str(valor)[:500]
        elif chave == "MensagemErro":
            valor = str(valor)[:1000]
        elif chave == "Percentual":
            try:
                valor = max(0, min(100, float(valor)))
            except (TypeError, ValueError):
                continue
        sets.append(f"{chave} = ?")
        params.append(valor)
    sets.extend(extras)
    if not sets:
        return
    params.append(int(importacao_id))
    from db.connection import conectar_banco

    conn = conectar_banco()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        garantir_tabela_importacao_arquivo(cursor, conn)
        cursor.execute(
            f"UPDATE dbo.ImportacaoArquivo SET {', '.join(sets)} WHERE ImportacaoArquivoID = ?",
            tuple(params),
        )
        conn.commit()
    except Exception:
        logger.exception("Falha ao atualizar ImportacaoArquivo %s", importacao_id)
    finally:
        conn.close()


def marcar_inicio_processamento(importacao_id, mensagem="Processamento iniciado"):
    atualizar_importacao(
        importacao_id,
        Status=STATUS_PROCESSANDO,
        Percentual=10,
        Mensagem=mensagem,
        _sql_extra=["DataInicioProcessamento = GETDATE()"],
    )


def marcar_conclusao(importacao_id, com_erros=False, **kwargs):
    status = STATUS_CONCLUIDO_ERROS if com_erros else STATUS_CONCLUIDO
    dados = dict(kwargs)
    dados["Status"] = status
    dados.setdefault("Percentual", 100)
    dados["_sql_extra"] = ["DataFimProcessamento = GETDATE()"]
    atualizar_importacao(importacao_id, **dados)


def marcar_conclusao_ativa(com_erros=False, **kwargs):
    marcar_conclusao(_importacao_ativa, com_erros=com_erros, **kwargs)


def marcar_erro(importacao_id, mensagem):
    detalhe = str(mensagem or "").strip() or "Erro não identificado durante o processamento."
    nota = "A tabela oficial não foi alterada."
    texto = detalhe if nota.lower() in detalhe.lower() else f"{detalhe} {nota}"
    atualizar_importacao(
        importacao_id,
        Status=STATUS_ERRO,
        Mensagem=texto[:500],
        MensagemErro=detalhe[:1000],
        _sql_extra=["DataFimProcessamento = GETDATE()"],
    )
    logger.error("Importação %s em ERRO: %s", importacao_id, detalhe)


def definir_importacao_ativa(importacao_id):
    global _importacao_ativa
    _importacao_ativa = importacao_id


def reportar_progresso(percentual=None, mensagem=None, **kwargs):
    if not _importacao_ativa:
        return
    dados = dict(kwargs)
    if percentual is not None:
        dados["Percentual"] = percentual
    if mensagem:
        dados["Mensagem"] = mensagem
        logger.info("Importação %s: %s", _importacao_ativa, mensagem)
    atualizar_importacao(_importacao_ativa, **dados)


def carregar_importacao(importacao_id, usuario_id=None, projeto_id=None):
    from db.connection import conectar_banco

    conn = conectar_banco()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        garantir_tabela_importacao_arquivo(cursor, conn)
        cursor.execute(
            """
            SELECT ImportacaoArquivoID, ProjetoID, NomeProjeto, LayoutID, NomeLayout,
                   NomeArquivoOriginal, NomeArquivoFisico, CaminhoArquivo,
                   JobId, ProcessId, UsuarioID, UsuarioNome, SomenteValidacao,
                   DataInicioUpload, DataFimUpload, DataInicioProcessamento, DataFimProcessamento,
                   Status, Percentual, TotalRegistros, RegistrosProcessados,
                   RegistrosImportados, RegistrosComErro, Mensagem, MensagemErro
            FROM dbo.ImportacaoArquivo
            WHERE ImportacaoArquivoID = ?
            """,
            (int(importacao_id),),
        )
        row = cursor.fetchone()
        if not row:
            return None
        nomes = [c[0] for c in cursor.description]
        data = dict(zip(nomes, row))
        if usuario_id is not None and str(data.get("UsuarioID")) != str(usuario_id):
            return None
        if projeto_id is not None and str(data.get("ProjetoID")) != str(projeto_id):
            return None
        return data
    finally:
        conn.close()


def carregar_importacao_por_process_id(process_id, usuario_id=None, projeto_id=None):
    if not process_id:
        return None
    from db.connection import conectar_banco

    conn = conectar_banco()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        garantir_tabela_importacao_arquivo(cursor, conn)
        cursor.execute(
            """
            SELECT TOP 1 ImportacaoArquivoID, ProjetoID, NomeProjeto, LayoutID, NomeLayout,
                   NomeArquivoOriginal, NomeArquivoFisico, CaminhoArquivo,
                   JobId, ProcessId, UsuarioID, UsuarioNome, SomenteValidacao,
                   DataInicioUpload, DataFimUpload, DataInicioProcessamento, DataFimProcessamento,
                   Status, Percentual, TotalRegistros, RegistrosProcessados,
                   RegistrosImportados, RegistrosComErro, Mensagem, MensagemErro
            FROM dbo.ImportacaoArquivo
            WHERE ProcessId = ?
            ORDER BY ImportacaoArquivoID DESC
            """,
            (str(process_id)[:64],),
        )
        row = cursor.fetchone()
        if not row:
            return None
        nomes = [c[0] for c in cursor.description]
        data = dict(zip(nomes, row))
        if usuario_id is not None and str(data.get("UsuarioID")) != str(usuario_id):
            return None
        if projeto_id is not None and str(data.get("ProjetoID")) != str(projeto_id):
            return None
        return data
    except Exception:
        logger.exception("Falha ao localizar importação pelo process_id %s", process_id)
        return None
    finally:
        conn.close()


def _tempo_status(data: dict, em_andamento=False):
    inicio = (
        data.get("DataInicioProcessamento")
        or data.get("DataFimUpload")
        or data.get("DataInicioUpload")
    )
    fim = None if em_andamento else data.get("DataFimProcessamento")
    segundos = duracao_segundos(inicio, fim)
    return {
        "inicio_epoch": epoch_local(inicio),
        "tempo_processamento_segundos": segundos,
        "tempo_processamento": formatar_duracao(segundos),
    }


def serializar_status(data: dict) -> dict:
    if not data:
        return {}
    percentual = data.get("Percentual")
    try:
        percentual = float(percentual or 0)
    except (TypeError, ValueError):
        percentual = 0
    status = (data.get("Status") or "").upper()
    em_andamento = status in (
        STATUS_RECEBENDO,
        STATUS_AGUARDANDO,
        STATUS_PROCESSANDO,
    )
    payload = {
        "importacao_id": data.get("ImportacaoArquivoID"),
        "status": data.get("Status"),
        "percentual": percentual,
        "total_registros": int(data.get("TotalRegistros") or 0),
        "registros_processados": int(data.get("RegistrosProcessados") or 0),
        "registros_importados": int(data.get("RegistrosImportados") or 0),
        "registros_com_erro": int(data.get("RegistrosComErro") or 0),
        "mensagem": data.get("Mensagem") or "",
        "mensagem_erro": data.get("MensagemErro") or "",
        "arquivo": data.get("NomeArquivoFisico") or data.get("NomeArquivoOriginal") or "",
        "layout": data.get("NomeLayout") or "",
        "job_id": data.get("JobId"),
        "process_id": data.get("ProcessId"),
    }
    payload.update(_tempo_status(data, em_andamento=em_andamento))
    return payload
