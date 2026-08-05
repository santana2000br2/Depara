from flask import Blueprint, render_template, redirect, url_for, session, flash, jsonify
from datetime import datetime
import re
from db.connection import conectar_banco, conectar_homologacao
from logger import logger

# Importar as funções de dados
from utils.dados_depara import (
    dados_condicao_pagamento,
    dados_escolaridade,
    dados_estado,
    dados_estadocivil,
    dados_municipio,
    dados_pais,
    dados_profissao,
    dados_segmentomercado,
    dados_tipologradouro,
    dados_departamento,
    dados_estoque,
    dados_naturezaoperacao,
    dados_equipe,
    dados_usuario_depara,
    dados_clasmontadora,
    dados_grupolucratividade,
    dados_grupoproduto,
    dados_pessoacodfabricante,
    dados_procedencia,
    dados_tabelapreco,
    dados_tipoproduto,
    dados_unidade,
    dados_combustivel,
    dados_corexterna,
    dados_corinterna,
    dados_marca,
    dados_modeloveiculo,
    dados_opcional,
    dados_setorservico,
    dados_tipoos,
    dados_tiposervico,
    dados_tmo,
    dados_veiculoano,
    dados_agentecobrador,
    dados_banco,
    dados_contagerencial,
    dados_tipocobranca,
    dados_tipocreditodebito,
    dados_tipodocumento,
    dados_tipoficharazao,
    dados_tipotitulo,
    dados_centroresultado,
    dados_historicopadrao,
    dados_planoconta,
    dados_subconta,
    dados_tipolote,
    dados_tiposubconta,
)

dashboard_bp = Blueprint("dashboard", __name__)

from utils.depara_escopos import CATEGORIAS_NOMES, ESCOPO_PARA_CATEGORIA

def obter_escopos_projeto(projeto_id):
    """Obtém os tipos de escopo vinculados ao projeto"""
    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if conn is None:
            logger.error("Falha ao conectar ao banco de dados em obter_escopos_projeto")
            return []
        
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT TipoEscopoIDs 
            FROM Escopo 
            WHERE ProjetoID = ?
        """, (projeto_id,))
        
        resultado = cursor.fetchone()
        if resultado and resultado.TipoEscopoIDs:
            return resultado.TipoEscopoIDs.split(',')
        return []
        
    except Exception as e:
        logger.error(f"Erro ao obter escopos do projeto: {e}")
        return []
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def obter_dados_por_categoria(banco_usuario, categorias_habilitadas):
    """Obtém apenas os dados das categorias habilitadas"""
    dados = {}
    funcoes_dados = {
        "cond_pag": dados_condicao_pagamento,
        "escol": dados_escolaridade,
        "estado": dados_estado,
        "estadocivil": dados_estadocivil,
        "municipio": dados_municipio,
        "pais": dados_pais,
        "profissao": dados_profissao,
        "segmentomercado": dados_segmentomercado,
        "tipologradouro": dados_tipologradouro,
        "departamento": dados_departamento,
        "estoque": dados_estoque,
        "naturezaoperacao": dados_naturezaoperacao,
        "equipe": dados_equipe,
        "usuario_depara": dados_usuario_depara,
        "clasmontadora": dados_clasmontadora,
        "grupolucratividade": dados_grupolucratividade,
        "grupoproduto": dados_grupoproduto,
        "pessoacodfabricante": dados_pessoacodfabricante,
        "procedencia": dados_procedencia,
        "tabelapreco": dados_tabelapreco,
        "tipoproduto": dados_tipoproduto,
        "unidade": dados_unidade,
        "combustivel": dados_combustivel,
        "corexterna": dados_corexterna,
        "corinterna": dados_corinterna,
        "marca": dados_marca,
        "modeloveiculo": dados_modeloveiculo,
        "opcional": dados_opcional,
        "setorservico": dados_setorservico,
        "tipoos": dados_tipoos,
        "tiposervico": dados_tiposervico,
        "tmo": dados_tmo,
        "veiculoano": dados_veiculoano,
        "agentecobrador": dados_agentecobrador,
        "banco": dados_banco,
        "contagerencial": dados_contagerencial,
        "tipocobranca": dados_tipocobranca,
        "tipocreditodebito": dados_tipocreditodebito,
        "tipodocumento": dados_tipodocumento,
        "tipoficharazao": dados_tipoficharazao,
        "tipotitulo": dados_tipotitulo,
        "centroresultado": dados_centroresultado,
        "historicopadrao": dados_historicopadrao,
        "planoconta": dados_planoconta,
        "subconta": dados_subconta,
        "tipolote": dados_tipolote,
        "tiposubconta": dados_tiposubconta,
    }
    
    for categoria in categorias_habilitadas:
        if categoria in funcoes_dados:
            try:
                # As funções de dados_depara agora usam conectar_homologacao() 
                # que verifica credenciais do projeto automaticamente
                dados[categoria] = funcoes_dados[categoria](banco_usuario)
            except Exception as e:
                logger.error(f"Erro ao carregar dados da categoria {categoria}: {e}")
                dados[categoria] = {"qtd": 0, "qtdPendente": 0, "percentualConclusao": 0}
    
    return dados

def calcular_progresso_categoria(dados_categoria):
    """Calcula o progresso de uma categoria específica"""
    if not dados_categoria:
        return 0
    
    total_qtd = 0
    total_concluido = 0
    
    for dados in dados_categoria:
        if isinstance(dados, dict):
            qtd = dados.get("qtd", 0)
            qtd_pendente = dados.get("qtdPendente", 0)
            total_qtd += qtd
            total_concluido += qtd - qtd_pendente
    
    if total_qtd > 0:
        return round((total_concluido / total_qtd) * 100, 1)
    else:
        return 0

@dashboard_bp.route("/")
def dashboard():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    if "projeto_selecionado" not in session:
        logger.error("❌ NÃO HÁ PROJETO NA SESSÃO NO DASHBOARD!")
        logger.error(f"Session keys: {list(session.keys())}")
        return redirect(url_for("auth.selecionar_projeto"))
    
    # DIAGNÓSTICO DETALHADO
    projeto = session["projeto_selecionado"]
    logger.info("=" * 80)
    logger.info("DIAGNÓSTICO DASHBOARD")
    logger.info("=" * 80)
    logger.info(f"Usuário: {session.get('usuario', {}).get('usuario')}")
    logger.info(f"Projeto: {projeto.get('NomeProjeto')}")
    logger.info(f"ProjetoID: {projeto.get('ProjetoID')}")
    logger.info(f"DadosGX: {projeto.get('DadosGX')}")
    logger.info(f"Servidor Homologação: {projeto.get('servidorhomologacao')}")
    # Credenciais (usuário/senha) não ficam na sessão — carregadas sob demanda
    logger.info("Credenciais de banco: carregadas sob demanda (não armazenadas na sessão)")

    # Continue com o resto do código original...
    
    # Obter dados da sessão
    usuario = session["usuario"]
    projeto_selecionado = session["projeto_selecionado"]
    banco_usuario = projeto_selecionado["DadosGX"]
    projeto_id = projeto_selecionado.get("ProjetoID")

    logger.debug(f"Carregando dashboard para banco: {banco_usuario}, projeto: {projeto_id}")

    # Obter escopos do projeto
    escopos_habilitados = obter_escopos_projeto(projeto_id)
    logger.debug(f"Escopos habilitados: {escopos_habilitados}")

    # Determinar categorias habilitadas baseadas nos escopos
    categorias_habilitadas = []
    for escopo in escopos_habilitados:
        if escopo in ESCOPO_PARA_CATEGORIA:
            categorias_habilitadas.extend(ESCOPO_PARA_CATEGORIA[escopo])
    
    # Remover duplicatas
    categorias_habilitadas = list(set(categorias_habilitadas))
    logger.debug(f"Categorias habilitadas: {categorias_habilitadas}")

    # Verificar se o usuário tem banco configurado
    if not banco_usuario:
        flash("Banco de dados não configurado para este projeto. Entre em contato com o administrador.", "warning")
        # Retornar estrutura com dados vazios mas com informações de escopo
        return render_template_dashboard_com_escopo(usuario, projeto_selecionado, {}, escopos_habilitados, categorias_habilitadas)

    try:
        # Coletar dados apenas das categorias habilitadas
        dados = obter_dados_por_categoria(banco_usuario, categorias_habilitadas)
        
        # Calcular progresso total apenas com dados habilitados
        todos_dados = list(dados.values())
        progresso_total = calcular_progresso_total(todos_dados)
        
        # Calcular progresso por categoria
        progresso_categorias = calcular_progresso_por_categoria(dados, escopos_habilitados)

        return render_template_dashboard_com_escopo(
            usuario, projeto_selecionado, dados, escopos_habilitados, 
            categorias_habilitadas, progresso_total, progresso_categorias
        )

    except Exception as e:
        logger.error(f"Erro ao carregar dados do dashboard: {e}")
        import traceback
        traceback.print_exc()
        flash(f"Erro ao carregar dados: {str(e)}", "error")
        
        # Retornar estrutura com dados vazios mas com informações de escopo
        return render_template_dashboard_com_escopo(usuario, projeto_selecionado, {}, escopos_habilitados, categorias_habilitadas)


@dashboard_bp.route("/verificar-notificacoes", methods=["POST"])
def verificar_notificacoes():
    """Verifica e-mails depois que o dashboard já foi entregue ao navegador."""
    if "usuario" not in session:
        return jsonify({"success": False, "message": "Sessão expirada"}), 401

    projeto = session.get("projeto_selecionado") or {}
    projeto_id = projeto.get("ProjetoID")
    banco_usuario = projeto.get("DadosGX")
    if not projeto_id or not banco_usuario:
        return jsonify({"success": False, "message": "Projeto sem banco configurado"}), 400

    try:
        from utils.depara_notificacao import verificar_todos_blocos_projeto

        resultados = verificar_todos_blocos_projeto(
            projeto_id,
            banco_usuario,
            projeto.get("NomeProjeto", "Projeto"),
        )
        enviados = sum(1 for resultado in resultados if resultado.get("enviado"))
        return jsonify({"success": True, "enviados": enviados})
    except Exception as exc:
        logger.warning(f"Verificação assíncrona de notificações ignorada: {exc}")
        return jsonify({"success": False, "message": "Falha ao verificar notificações"}), 500

def calcular_progresso_por_categoria(dados, escopos_habilitados):
    """Calcula o progresso para cada categoria habilitada"""
    progresso_categorias = {}
    
    for escopo, categorias in ESCOPO_PARA_CATEGORIA.items():
        if escopo in escopos_habilitados:
            dados_categoria = [dados.get(cat, {}) for cat in categorias]
            progresso_categorias[escopo] = calcular_progresso_categoria(dados_categoria)
    
    return progresso_categorias

def render_template_dashboard_com_escopo(usuario, projeto_selecionado, dados, escopos_habilitados, categorias_habilitadas, progresso_total=None, progresso_categorias=None):
    """Renderiza o template com informações de escopo"""
    
    # Criar dados vazios para todas as categorias possíveis
    dados_vazios = {"qtd": 0, "qtdPendente": 0, "percentualConclusao": 0}
    progresso_vazio = {
        "total_qtd": 0,
        "total_concluido": 0,
        "total_pendente": 0,
        "percentual_total": 0,
    }
    
    # Inicializar progresso_categorias se None
    if progresso_categorias is None:
        progresso_categorias = {}

    datas_blocos = {}
    try:
        from utils.bloco_disponivel import obter_datas_blocos
        datas_raw = obter_datas_blocos(projeto_selecionado.get("ProjetoID"))
        for escopo, info in datas_raw.items():
            data = info.get("DataDisponivel")
            ultima = info.get("DataUltimaImportacao")
            datas_blocos[escopo] = {
                "data": data.strftime("%d/%m/%Y %H:%M") if data and hasattr(data, "strftime") else (str(data) if data else None),
                "ultima": ultima.strftime("%d/%m/%Y %H:%M") if ultima and hasattr(ultima, "strftime") else (str(ultima) if ultima else None),
                "layout": info.get("NomeLayout") or info.get("TipoLayout") or "",
            }
    except Exception as exc:
        logger.warning(f"Não foi possível carregar datas dos blocos: {exc}")

    status_layouts_obrigatorios = None
    try:
        from utils.projeto_acesso import projeto_eh_arquivo_workflow
        from utils.layout_importacao_projeto import obter_status_layouts_obrigatorios
        if projeto_eh_arquivo_workflow(projeto_selecionado):
            status_layouts_obrigatorios = obter_status_layouts_obrigatorios(
                projeto_selecionado.get("ProjetoID"),
                escopos_habilitados,
            )
    except Exception as exc:
        logger.warning("Não foi possível montar indicador de layouts obrigatórios: %s", exc)
    
    # Preencher dados, usando valores reais se disponíveis, senão dados vazios
    template_data = {
        "usuario": usuario,
        "projeto_nome": projeto_selecionado.get("NomeProjeto"),
        "projeto_selecionado": projeto_selecionado,
        "escopos_habilitados": escopos_habilitados,
        "categorias_habilitadas": categorias_habilitadas,
        "progresso_total": progresso_total or progresso_vazio,
        "progresso_categorias": progresso_categorias,
        "categorias_nomes": CATEGORIAS_NOMES,
        "datas_blocos": datas_blocos,
        "status_layouts_obrigatorios": status_layouts_obrigatorios,
        
        # Dados das categorias (usar dados reais se disponíveis, senão vazios)
        "cond_pag": dados.get("cond_pag", dados_vazios),
        "escol": dados.get("escol", dados_vazios),
        "estado": dados.get("estado", dados_vazios),
        "estadocivil": dados.get("estadocivil", dados_vazios),
        "municipio": dados.get("municipio", dados_vazios),
        "pais": dados.get("pais", dados_vazios),
        "profissao": dados.get("profissao", dados_vazios),
        "segmentomercado": dados.get("segmentomercado", dados_vazios),
        "tipologradouro": dados.get("tipologradouro", dados_vazios),
        "departamento": dados.get("departamento", dados_vazios),
        "estoque": dados.get("estoque", dados_vazios),
        "naturezaoperacao": dados.get("naturezaoperacao", dados_vazios),
        "equipe": dados.get("equipe", dados_vazios),
        "usuario_depara": dados.get("usuario_depara", dados_vazios),
        "clasmontadora": dados.get("clasmontadora", dados_vazios),
        "grupolucratividade": dados.get("grupolucratividade", dados_vazios),
        "grupoproduto": dados.get("grupoproduto", dados_vazios),
        "pessoacodfabricante": dados.get("pessoacodfabricante", dados_vazios),
        "procedencia": dados.get("procedencia", dados_vazios),
        "tabelapreco": dados.get("tabelapreco", dados_vazios),
        "tipoproduto": dados.get("tipoproduto", dados_vazios),
        "unidade": dados.get("unidade", dados_vazios),
        "combustivel": dados.get("combustivel", dados_vazios),
        "corexterna": dados.get("corexterna", dados_vazios),
        "corinterna": dados.get("corinterna", dados_vazios),
        "marca": dados.get("marca", dados_vazios),
        "modeloveiculo": dados.get("modeloveiculo", dados_vazios),
        "opcional": dados.get("opcional", dados_vazios),
        "setorservico": dados.get("setorservico", dados_vazios),
        "tipoos": dados.get("tipoos", dados_vazios),
        "tiposervico": dados.get("tiposervico", dados_vazios),
        "tmo": dados.get("tmo", dados_vazios),
        "veiculoano": dados.get("veiculoano", dados_vazios),
        "agentecobrador": dados.get("agentecobrador", dados_vazios),
        "banco": dados.get("banco", dados_vazios),
        "contagerencial": dados.get("contagerencial", dados_vazios),
        "tipocobranca": dados.get("tipocobranca", dados_vazios),
        "tipocreditodebito": dados.get("tipocreditodebito", dados_vazios),
        "tipodocumento": dados.get("tipodocumento", dados_vazios),
        "tipoficharazao": dados.get("tipoficharazao", dados_vazios),
        "tipotitulo": dados.get("tipotitulo", dados_vazios),
        "centroresultado": dados.get("centroresultado", dados_vazios),
        "historicopadrao": dados.get("historicopadrao", dados_vazios),
        "planoconta": dados.get("planoconta", dados_vazios),
        "subconta": dados.get("subconta", dados_vazios),
        "tipolote": dados.get("tipolote", dados_vazios),
        "tiposubconta": dados.get("tiposubconta", dados_vazios),
    }
    
    return render_template("dashboard.html", **template_data)

def calcular_progresso_total(dados_tabelas):
    """
    Calcula o progresso total baseado em todas as tabelas
    """
    total_qtd = 0
    total_concluido = 0

    for dados in dados_tabelas:
        if isinstance(dados, dict):
            qtd = dados.get("qtd", 0)
            qtd_pendente = dados.get("qtdPendente", 0)
            total_qtd += qtd
            total_concluido += qtd - qtd_pendente

    if total_qtd > 0:
        percentual_total = (total_concluido / total_qtd) * 100
    else:
        percentual_total = 0

    return {
        "total_qtd": total_qtd,
        "total_concluido": total_concluido,
        "total_pendente": total_qtd - total_concluido,
        "percentual_total": round(percentual_total, 1),
    }


@dashboard_bp.route("/logout")
def logout():
    session.clear()
    flash("Logout realizado com sucesso", "success")
    return redirect(url_for("auth.login"))


@dashboard_bp.route("/detalhes_tabela/<nome_tabela>")
def detalhes_tabela(nome_tabela):
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    if "projeto_selecionado" not in session:
        flash("Nenhum projeto selecionado. Por favor, selecione um projeto.", "warning")
        return redirect(url_for("auth.trocar_projeto"))

    # Impede SQL injection: apenas identificadores SQL Server válidos
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", nome_tabela or ""):
        flash("Nome de tabela inválido.", "error")
        return redirect(url_for("dashboard.dashboard"))

    projeto_selecionado = session["projeto_selecionado"]
    usuario = session["usuario"]

    conn = None
    cursor = None
    colunas, registros = [], []
    try:
        conn = conectar_homologacao()
        if conn is None:
            raise Exception("Falha na conexão com o banco de dados")

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'
            """,
            (nome_tabela,),
        )
        if not cursor.fetchone():
            flash("Tabela não encontrada.", "error")
            return redirect(url_for("dashboard.dashboard"))

        # Identificador já validado + existência confirmada; brackets evitam palavras reservadas
        cursor.execute(f"SELECT * FROM [{nome_tabela}]")
        colunas = [desc[0] for desc in cursor.description]
        registros = cursor.fetchall()

    except Exception as e:
        logger.error(f"Erro ao buscar detalhes de {nome_tabela}: {e}")
        flash("Erro ao carregar detalhes da tabela.", "error")
        colunas, registros = [], []

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

    return render_template(
        "detalhes_tabela.html",
        usuario=usuario,
        projeto_nome=projeto_selecionado.get("NomeProjeto"),
        nome_tabela=nome_tabela,
        colunas=colunas,
        registros=registros,
    )