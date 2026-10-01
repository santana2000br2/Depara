from flask import Blueprint, render_template, redirect, url_for, flash, session, request, jsonify
from db.connection import conectar_banco, garantir_colunas_dadosgx
from logger import logger
from utils.credential_crypto import (
    criptografar_segredo,
    descriptografar_segredo,
    esta_criptografado,
)

projetos_bp = Blueprint("projetos", __name__)

# Tipos de escopo disponíveis para cadastro via tela de projetos
TIPOS_ESCOPO = {
    "PESSOA": "Pessoa",
    "PRODUTOS": "Produtos",
    "VEICULOS": "Veiculos",
    "FINANCEIRO": "Financeiro",
    "CONTABILIDADE": "Contabilidade",
    "FISCAL": "Fiscal",
    "GERAL": "Geral"
}

TIPOS_PROJETO = {
    "windows_workflow": "Windows X Workflow",
    "workflow_workflow": "Workflow X Workflow",
    "arquivo_workflow": "Arquivo X Workflow",
}


def _format_date(val):
    if val is None:
        return ''
    if hasattr(val, 'strftime'):
        return val.strftime('%Y-%m-%d')
    return str(val)[:10]


def _parse_date(val):
    if not val or not str(val).strip():
        return None
    return str(val).strip()[:10]


def _tipo_projeto_codigo(row):
    """Retorna o código do tipo a partir das flags do banco."""
    if getattr(row, 'TipoWindowsWorkflow', None):
        return 'windows_workflow'
    if getattr(row, 'TipoWorkflowWorkflow', None):
        return 'workflow_workflow'
    if getattr(row, 'TipoArquivoWorkflow', None):
        return 'arquivo_workflow'
    return ''


def _tipo_projeto_label(codigo):
    return TIPOS_PROJETO.get(codigo, '')


def _flags_tipo_projeto(tipo_codigo):
    return {
        'TipoWindowsWorkflow': 1 if tipo_codigo == 'windows_workflow' else 0,
        'TipoWorkflowWorkflow': 1 if tipo_codigo == 'workflow_workflow' else 0,
        'TipoArquivoWorkflow': 1 if tipo_codigo == 'arquivo_workflow' else 0,
    }


def _projeto_para_dict(projeto):
    tipo_codigo = _tipo_projeto_codigo(projeto)
    return {
        'ProjetoID': projeto.ProjetoID,
        'NomeProjeto': projeto.NomeProjeto,
        'DadosGX': projeto.DadosGX,
        'servidorproducao': projeto.servidorproducao,
        # Usuários DB: descriptografados só para a tela admin
        'usuarioProducao': descriptografar_segredo(projeto.usuarioProducao) or '',
        # Nunca devolver senha em claro/cifrada à UI — só flags
        'senhaproducao': '',
        'senhaproducao_configurada': bool(getattr(projeto, 'senhaproducao', None)),
        'servidorhomologacao': projeto.servidorhomologacao,
        'usuariohomologacao': descriptografar_segredo(projeto.usuariohomologacao) or '',
        'senhahomologacao': '',
        'senhahomologacao_configurada': bool(getattr(projeto, 'senhahomologacao', None)),
        'servidordadosgx': getattr(projeto, 'servidordadosgx', None) or '',
        'usuariodadosgx': descriptografar_segredo(getattr(projeto, 'usuariodadosgx', None)) or '',
        'senhadadosgx': '',
        'senhadadosgx_configurada': bool(getattr(projeto, 'senhadadosgx', None)),
        'BancoHomo': projeto.BancoHomo,
        'PontoFocal': projeto.PontoFocal,
        'ConsultorLider': projeto.ConsultorLider,
        'LiderProjeto': projeto.LiderProjeto,
        'migrador': projeto.migrador,
        'bancoProducao': projeto.bancoProducao,
        'Concluido': bool(projeto.Concluido),
        'TipoProjeto': tipo_codigo,
        'TipoProjetoLabel': _tipo_projeto_label(tipo_codigo),
        'TipoWindowsWorkflow': bool(getattr(projeto, 'TipoWindowsWorkflow', False)),
        'TipoWorkflowWorkflow': bool(getattr(projeto, 'TipoWorkflowWorkflow', False)),
        'TipoArquivoWorkflow': bool(getattr(projeto, 'TipoArquivoWorkflow', False)),
        'Fase1DataInicio': _format_date(getattr(projeto, 'Fase1DataInicio', None)),
        'Fase1DataTermino': _format_date(getattr(projeto, 'Fase1DataTermino', None)),
        'Fase2DataInicio': _format_date(getattr(projeto, 'Fase2DataInicio', None)),
        'Fase2DataTermino': _format_date(getattr(projeto, 'Fase2DataTermino', None)),
        'ImportacaoLiberada': bool(getattr(projeto, 'ImportacaoLiberada', False)),
    }


def _garantir_colunas_senha_largas(cursor):
    """Token Fernet cabe em NVARCHAR(500); amplia colunas curtas se necessário."""
    for coluna in (
        "senhaproducao",
        "senhahomologacao",
        "senhadadosgx",
        "usuarioProducao",
        "usuariohomologacao",
        "usuariodadosgx",
    ):
        try:
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
                cursor.execute(
                    f"ALTER TABLE Projeto ALTER COLUMN {coluna} NVARCHAR(500) NULL"
                )
                logger.info("Coluna Projeto.%s ampliada para NVARCHAR(500)", coluna)
        except Exception as exc:
            logger.warning("Não foi possível ajustar coluna %s: %s", coluna, exc)


def _cifrar_usuario_db(valor):
    texto = (valor or "").strip()
    if not texto:
        return None
    return criptografar_segredo(texto)


def _resolver_senha_para_gravar(nova_senha, senha_atual_banco):
    """Cifra senha nova; se vazia na edição, mantém a já gravada."""
    if nova_senha is None:
        nova_senha = ""
    nova_senha = str(nova_senha).strip()
    if nova_senha:
        return criptografar_segredo(nova_senha)
    return senha_atual_banco


_COLUNAS_PROJETO_SELECT = """
    ProjetoID,
    NomeProjeto,
    DadosGX,
    servidorproducao,
    usuarioProducao,
    senhaproducao,
    servidorhomologacao,
    usuariohomologacao,
    senhahomologacao,
    servidordadosgx,
    usuariodadosgx,
    senhadadosgx,
    BancoHomo,
    PontoFocal,
    ConsultorLider,
    LiderProjeto,
    migrador,
    bancoProducao,
    Concluido,
    TipoWindowsWorkflow,
    TipoWorkflowWorkflow,
    TipoArquivoWorkflow,
    Fase1DataInicio,
    Fase1DataTermino,
    Fase2DataInicio,
    Fase2DataTermino,
    ImportacaoLiberada
"""

@projetos_bp.route("/gerenciar_projetos")
def gerenciar_projetos():
    if "usuario" not in session or not session["usuario"].get("adm"):
        flash("Acesso não autorizado", "error")
        return redirect(url_for("dashboard.dashboard"))

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            flash("Erro de conexão com o banco de dados", "error")
            return render_template(
                "projetos.html",
                projetos=[],
                tipos_escopo=TIPOS_ESCOPO,
                tipos_projeto=TIPOS_PROJETO,
                mostrar_concluidos=False,
            )

        cursor = conn.cursor()
        garantir_colunas_dadosgx(cursor)
        conn.commit()
        _garantir_colunas_senha_largas(cursor)

        mostrar_concluidos = request.args.get('mostrar_concluidos', '').strip().lower() in (
            '1', 'true', 'sim', 'on', 'yes',
        )

        if mostrar_concluidos:
            cursor.execute(f"""
                SELECT {_COLUNAS_PROJETO_SELECT}
                FROM Projeto
                ORDER BY
                    CASE WHEN ISNULL(Concluido, 0) = 0 THEN 0 ELSE 1 END,
                    NomeProjeto
            """)
        else:
            cursor.execute(f"""
                SELECT {_COLUNAS_PROJETO_SELECT}
                FROM Projeto
                WHERE ISNULL(Concluido, 0) = 0
                ORDER BY NomeProjeto
            """)
        
        projetos_raw = cursor.fetchall()
        projetos_list = [_projeto_para_dict(projeto) for projeto in projetos_raw]

        return render_template(
            "projetos.html",
            projetos=projetos_list,
            tipos_escopo=TIPOS_ESCOPO,
            tipos_projeto=TIPOS_PROJETO,
            mostrar_concluidos=mostrar_concluidos,
            usuario=session["usuario"]
        )

    except Exception as e:
        logger.error(f"Erro ao carregar projetos: {e}")
        flash(f"Erro ao carregar lista de projetos: {str(e)}", "error")
        return render_template(
            "projetos.html",
            projetos=[],
            tipos_escopo=TIPOS_ESCOPO,
            tipos_projeto=TIPOS_PROJETO,
            mostrar_concluidos=False,
        )
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@projetos_bp.route("/salvar_projeto", methods=["POST"])
def salvar_projeto():
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"status": "error", "message": "Acesso não autorizado"}), 403

    data = request.get_json()
    dados_log = {
        k: ("***" if ("senha" in k.lower() or k.lower() in ("usuarioproducao", "usuariohomologacao", "usuariodadosgx")) else v)
        for k, v in (data or {}).items()
    }
    logger.info(f"Dados recebidos para salvar projeto: {dados_log}")
    
    projeto_id = data.get("projeto_id")
    nome_projeto = data.get("nome_projeto")
    dados_gx = data.get("dados_gx")
    
    # CAMPOS DE PRODUÇÃO
    servidorproducao = data.get("servidorproducao")
    usuarioProducao = _cifrar_usuario_db(data.get("usuarioProducao"))
    senhaproducao_input = data.get("senhaproducao")
    
    # CAMPOS DE HOMOLOGAÇÃO
    servidorhomologacao = data.get("servidorhomologacao")
    usuariohomologacao = _cifrar_usuario_db(data.get("usuariohomologacao"))
    senhahomologacao_input = data.get("senhahomologacao")

    # CAMPOS DE DADOS GX
    servidordadosgx = data.get("servidordadosgx")
    usuariodadosgx = _cifrar_usuario_db(data.get("usuariodadosgx"))
    senhadadosgx_input = data.get("senhadadosgx")
    
    banco_homo = data.get("banco_homo")
    ponto_focal = data.get("ponto_focal")
    consultor_lider = data.get("consultor_lider")
    lider_projeto = data.get("lider_projeto")
    
    # NOVOS CAMPOS - USANDO OS NOMES CORRETOS
    migrador = data.get("migrador")
    bancoProducao = data.get("bancoProducao")
    concluido = 1 if data.get("concluido") else 0
    tipos_escopo = data.get("tipos_escopo", [])
    tipo_projeto = data.get("tipo_projeto") or ''
    if tipo_projeto and tipo_projeto not in TIPOS_PROJETO:
        return jsonify({"status": "error", "message": "Tipo de projeto inválido."}), 400
    flags_tipo = _flags_tipo_projeto(tipo_projeto)
    fase1_inicio = _parse_date(data.get("fase1_data_inicio"))
    fase1_termino = _parse_date(data.get("fase1_data_termino"))
    fase2_inicio = _parse_date(data.get("fase2_data_inicio"))
    fase2_termino = _parse_date(data.get("fase2_data_termino"))
    importacao_liberada = (
        1 if tipo_projeto == 'arquivo_workflow' and data.get('importacao_liberada') else 0
    )

    tipos_validos = [tipo for tipo in tipos_escopo if tipo in TIPOS_ESCOPO]
    tipos_escopo_str = ",".join(tipos_validos) if tipos_validos else None

    logger.info(
        f"DEBUG - Valores dos novos campos: migrador='{migrador}', bancoProducao='{bancoProducao}', concluido={concluido}, tipos_escopo={tipos_validos}"
    )

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            logger.error("Falha na conexão com o banco")
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()
        garantir_colunas_dadosgx(cursor)
        conn.commit()
        _garantir_colunas_senha_largas(cursor)

        nome_projeto = (nome_projeto or '').strip()
        if not nome_projeto:
            return jsonify({"status": "error", "message": "Nome do projeto é obrigatório."}), 400

        # Unique por nome (case-insensitive) — evita duplicidade na criação/edição
        if projeto_id and projeto_id != 'null' and projeto_id != '':
            cursor.execute("""
                SELECT TOP 1 ProjetoID, NomeProjeto
                FROM Projeto
                WHERE LOWER(LTRIM(RTRIM(NomeProjeto))) = LOWER(?)
                  AND ProjetoID <> ?
            """, (nome_projeto, int(projeto_id)))
        else:
            cursor.execute("""
                SELECT TOP 1 ProjetoID, NomeProjeto
                FROM Projeto
                WHERE LOWER(LTRIM(RTRIM(NomeProjeto))) = LOWER(?)
            """, (nome_projeto,))
        duplicado = cursor.fetchone()
        if duplicado:
            return jsonify({
                "status": "error",
                "message": (
                    f'Já existe um projeto com o nome "{duplicado.NomeProjeto}" '
                    f'(ID {duplicado.ProjetoID}). Escolha outro nome.'
                ),
            }), 400

        if projeto_id and projeto_id != 'null' and projeto_id != '':  # EDITANDO projeto existente
            projeto_id = int(projeto_id)
            logger.info(f"Editando projeto ID: {projeto_id}")
            nome_projeto_antigo = None
            
            # Verificar se o projeto existe
            cursor.execute(
                """
                SELECT ProjetoID, NomeProjeto, senhaproducao, senhahomologacao, senhadadosgx
                FROM Projeto WHERE ProjetoID = ?
                """,
                (projeto_id,),
            )
            projeto_existente = cursor.fetchone()
            if not projeto_existente:
                return jsonify({"status": "error", "message": "Projeto não encontrado"}), 404
            nome_projeto_antigo = projeto_existente.NomeProjeto

            senhaproducao = _resolver_senha_para_gravar(
                senhaproducao_input, projeto_existente.senhaproducao
            )
            senhahomologacao = _resolver_senha_para_gravar(
                senhahomologacao_input, projeto_existente.senhahomologacao
            )
            senhadadosgx = _resolver_senha_para_gravar(
                senhadadosgx_input, getattr(projeto_existente, "senhadadosgx", None)
            )
            # Re-cifra legado em texto claro se o campo não foi alterado nesta edição
            if senhaproducao and not esta_criptografado(str(senhaproducao)):
                senhaproducao = criptografar_segredo(senhaproducao)
            if senhahomologacao and not esta_criptografado(str(senhahomologacao)):
                senhahomologacao = criptografar_segredo(senhahomologacao)
            if senhadadosgx and not esta_criptografado(str(senhadadosgx)):
                senhadadosgx = criptografar_segredo(senhadadosgx)

            # Atualizar projeto COM OS NOMES CORRETOS DAS COLUNAS
            cursor.execute("""
                UPDATE Projeto SET 
                    NomeProjeto = ?, 
                    DadosGX = ?, 
                    servidorproducao = ?, 
                    usuarioProducao = ?, 
                    senhaproducao = ?, 
                    servidorhomologacao = ?,
                    usuariohomologacao = ?,
                    senhahomologacao = ?,
                    servidordadosgx = ?,
                    usuariodadosgx = ?,
                    senhadadosgx = ?,
                    BancoHomo = ?, 
                    PontoFocal = ?, 
                    ConsultorLider = ?, 
                    LiderProjeto = ?,
                    migrador = ?,
                    bancoProducao = ?,
                    Concluido = ?,
                    TipoWindowsWorkflow = ?,
                    TipoWorkflowWorkflow = ?,
                    TipoArquivoWorkflow = ?,
                    Fase1DataInicio = ?,
                    Fase1DataTermino = ?,
                    Fase2DataInicio = ?,
                    Fase2DataTermino = ?,
                    ImportacaoLiberada = ?
                WHERE ProjetoID = ?
            """, (
                nome_projeto,
                dados_gx,
                servidorproducao,
                usuarioProducao,
                senhaproducao,
                servidorhomologacao,
                usuariohomologacao,
                senhahomologacao,
                servidordadosgx,
                usuariodadosgx,
                senhadadosgx,
                banco_homo,
                ponto_focal,
                consultor_lider,
                lider_projeto,
                migrador,
                bancoProducao,
                concluido,
                flags_tipo['TipoWindowsWorkflow'],
                flags_tipo['TipoWorkflowWorkflow'],
                flags_tipo['TipoArquivoWorkflow'],
                fase1_inicio,
                fase1_termino,
                fase2_inicio,
                fase2_termino,
                importacao_liberada,
                projeto_id
            ))
            logger.info(f"Projeto {projeto_id} atualizado - linhas afetadas: {cursor.rowcount}")

            # Regra de unificação: manter escopo padrão alinhado ao nome do projeto.
            cursor.execute("""
                UPDATE Escopo
                SET NomeEscopo = ?
                WHERE ProjetoID = ? AND NomeEscopo = ?
            """, (nome_projeto, projeto_id, nome_projeto_antigo))

            # Salvar também os tipos de escopo via tela de projeto
            cursor.execute("SELECT TOP 1 EscopoID FROM Escopo WHERE ProjetoID = ? ORDER BY EscopoID", (projeto_id,))
            escopo_existente = cursor.fetchone()
            if escopo_existente:
                cursor.execute("""
                    UPDATE Escopo
                    SET NomeEscopo = ?, TipoEscopoIDs = ?
                    WHERE EscopoID = ?
                """, (nome_projeto, tipos_escopo_str, escopo_existente.EscopoID))
            else:
                cursor.execute("""
                    INSERT INTO Escopo (NomeEscopo, Descricao, ProjetoID, TipoEscopoIDs)
                    VALUES (?, ?, ?, ?)
                """, (
                    nome_projeto,
                    f"Escopo padrão gerado automaticamente para o projeto {nome_projeto}.",
                    projeto_id,
                    tipos_escopo_str
                ))

        else:  # NOVO projeto
            logger.info("Criando novo projeto")
            senhaproducao = criptografar_segredo(senhaproducao_input) if senhaproducao_input else None
            senhahomologacao = (
                criptografar_segredo(senhahomologacao_input) if senhahomologacao_input else None
            )
            senhadadosgx = (
                criptografar_segredo(senhadadosgx_input) if senhadadosgx_input else None
            )
            
            # Inserir novo projeto e obter o ID diretamente
            cursor.execute("""
                INSERT INTO Projeto (
                    NomeProjeto, DadosGX, servidorproducao, usuarioProducao, 
                    senhaproducao, servidorhomologacao, usuariohomologacao,
                    senhahomologacao, servidordadosgx, usuariodadosgx, senhadadosgx,
                    BancoHomo, PontoFocal, ConsultorLider, LiderProjeto,
                    migrador, bancoProducao, Concluido,
                    TipoWindowsWorkflow, TipoWorkflowWorkflow, TipoArquivoWorkflow,
                    Fase1DataInicio, Fase1DataTermino, Fase2DataInicio, Fase2DataTermino,
                    ImportacaoLiberada
                ) OUTPUT INSERTED.ProjetoID 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                nome_projeto,
                dados_gx,
                servidorproducao,
                usuarioProducao,
                senhaproducao,
                servidorhomologacao,
                usuariohomologacao,
                senhahomologacao,
                servidordadosgx,
                usuariodadosgx,
                senhadadosgx,
                banco_homo,
                ponto_focal,
                consultor_lider,
                lider_projeto,
                migrador,
                bancoProducao,
                concluido,
                flags_tipo['TipoWindowsWorkflow'],
                flags_tipo['TipoWorkflowWorkflow'],
                flags_tipo['TipoArquivoWorkflow'],
                fase1_inicio,
                fase1_termino,
                fase2_inicio,
                fase2_termino,
                importacao_liberada,
            ))
            
            # Obter o ID do novo projeto diretamente do resultado da inserção
            result = cursor.fetchone()
            novo_projeto_id = result[0] if result else None

            logger.info(f"Novo projeto ID: {novo_projeto_id}")

            if not novo_projeto_id:
                conn.rollback()
                return jsonify({"status": "error", "message": "Falha ao obter ID do novo projeto"}), 500

            # Regra de unificação: criar escopo padrão automaticamente para o projeto.
            cursor.execute("SELECT COUNT(1) FROM Escopo WHERE ProjetoID = ?", (novo_projeto_id,))
            escopo_ja_existe = cursor.fetchone()[0] > 0
            if not escopo_ja_existe:
                cursor.execute("""
                    INSERT INTO Escopo (NomeEscopo, Descricao, ProjetoID, TipoEscopoIDs)
                    VALUES (?, ?, ?, ?)
                """, (
                    nome_projeto,
                    f"Escopo padrão gerado automaticamente para o projeto {nome_projeto}.",
                    novo_projeto_id,
                    tipos_escopo_str
                ))
                logger.info(f"Escopo padrão criado automaticamente para o projeto {novo_projeto_id}")

        conn.commit()
        logger.info(f"Projeto {'atualizado' if projeto_id else 'criado'} com sucesso: {nome_projeto}")
        return jsonify({"status": "success", "message": "Projeto salvo com sucesso!"}), 200

    except Exception as e:
        logger.error(f"Erro ao salvar projeto: {e}", exc_info=True)
        if conn:
            conn.rollback()
        msg = str(e)
        if 'UQ_Projeto_NomeProjeto' in msg or 'UNIQUE KEY' in msg.upper() or 'unique index' in msg.lower():
            return jsonify({
                "status": "error",
                "message": f'Já existe um projeto com o nome "{nome_projeto}". Escolha outro nome.',
            }), 400
        return jsonify({"status": "error", "message": f"Erro ao salvar projeto: {msg}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@projetos_bp.route("/obter_projeto/<int:projeto_id>")
def obter_projeto(projeto_id):
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"success": False, "message": "Acesso não autorizado"})

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"success": False, "message": "Erro de conexão com o banco de dados"})

        cursor = conn.cursor()
        garantir_colunas_dadosgx(cursor)
        conn.commit()
        
        # Consulta COM OS NOMES CORRETOS DAS COLUNAS
        cursor.execute(f"""
            SELECT {_COLUNAS_PROJETO_SELECT}
            FROM Projeto
            WHERE ProjetoID = ?
        """, (projeto_id,))
        
        projeto = cursor.fetchone()
        
        if not projeto:
            return jsonify({"success": False, "message": "Projeto não encontrado"})
        
        projeto_dict = _projeto_para_dict(projeto)

        cursor.execute("""
            SELECT TOP 1 TipoEscopoIDs
            FROM Escopo
            WHERE ProjetoID = ?
            ORDER BY EscopoID
        """, (projeto_id,))
        escopo = cursor.fetchone()
        tipos_escopo_list = []
        if escopo and escopo.TipoEscopoIDs:
            tipos_escopo_list = [tipo for tipo in escopo.TipoEscopoIDs.split(",") if tipo]
        projeto_dict["TiposEscopo"] = tipos_escopo_list
        
        return jsonify({"success": True, "projeto": projeto_dict})

    except Exception as e:
        logger.error(f"Erro ao obter projeto: {e}")
        return jsonify({"success": False, "message": f"Erro ao obter projeto: {str(e)}"})
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@projetos_bp.route("/excluir_projeto/<int:projeto_id>", methods=["POST"])
def excluir_projeto(projeto_id):
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"status": "error", "message": "Acesso não autorizado"}), 403

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()
        cursor.execute(
            "SELECT ProjetoID, NomeProjeto FROM Projeto WHERE ProjetoID = ?",
            (projeto_id,),
        )
        projeto = cursor.fetchone()
        if not projeto:
            return jsonify({"status": "error", "message": "Projeto não encontrado"}), 404

        nome = projeto.NomeProjeto

        # Dependências antes de remover o projeto
        cursor.execute("DELETE FROM UsuarioProjeto WHERE ProjetoID = ?", (projeto_id,))
        cursor.execute("DELETE FROM Escopo WHERE ProjetoID = ?", (projeto_id,))
        cursor.execute(
            "UPDATE Empresa SET ProjetoID = NULL WHERE ProjetoID = ?",
            (projeto_id,),
        )
        cursor.execute("DELETE FROM Projeto WHERE ProjetoID = ?", (projeto_id,))

        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({"status": "error", "message": "Projeto não encontrado."}), 404

        # Se o projeto excluído é o da sessão, limpa a seleção
        projeto_sessao = session.get('projeto_selecionado') or {}
        if str(projeto_sessao.get('ProjetoID')) == str(projeto_id):
            session.pop('projeto_selecionado', None)

        conn.commit()
        logger.info("Projeto excluído: ID=%s Nome=%s", projeto_id, nome)
        return jsonify({
            "status": "success",
            "message": f'Projeto "{nome}" excluído com sucesso!',
        })

    except Exception as e:
        logger.error(f"Erro ao excluir projeto: {e}", exc_info=True)
        if conn:
            conn.rollback()
        msg = str(e)
        if 'REFERENCE' in msg.upper() or 'FOREIGN KEY' in msg.upper():
            return jsonify({
                "status": "error",
                "message": (
                    "Não foi possível excluir: existem vínculos neste projeto. "
                    "Remova as associações antes de excluir."
                ),
            }), 400
        return jsonify({"status": "error", "message": f"Erro ao excluir projeto: {msg}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
