from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify
from db.connection import conectar_banco
from logger import logger
from utils.projeto_acesso import acesso_envio_arquivos
from utils import layout_versao
from utils.layout_escopo import (
    TIPOS_ESCOPO_LAYOUT,
    garantir_colunas_layout_escopo,
    ler_escopo_do_form,
    label_tipo_escopo,
)

layout_bp = Blueprint('layout', __name__)


def _verificar_acesso_layout():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    usuario = session.get("usuario", {})
    if not usuario.get("adm"):
        flash("Acesso negado. Apenas administradores podem gerenciar layouts.", "error")
        return redirect(url_for("dashboard.dashboard"))

    if not acesso_envio_arquivos(session.get('projeto_selecionado')):
        flash(
            "Gerenciar layouts está disponível apenas em projetos do tipo Arquivo X Workflow.",
            "warning",
        )
        return redirect(url_for("dashboard.dashboard"))

    return None


def _verificar_acesso_visualizar_layout():
    """Qualquer usuário autenticado com acesso a Envio de Arquivos pode consultar."""
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    if not acesso_envio_arquivos(session.get("projeto_selecionado")):
        flash(
            "Visualizar layouts está disponível apenas em projetos do tipo Arquivo X Workflow.",
            "warning",
        )
        return redirect(url_for("dashboard.dashboard"))

    return None


def _usuario_criacao_id():
    return session.get('usuario', {}).get('usuario_id') or 1

def row_to_dict(row):
    """Converte uma linha do banco para dicionário"""
    if hasattr(row, '_asdict'):
        return row._asdict()
    elif hasattr(row, '__dict__'):
        return {key: value for key, value in row.__dict__.items() if not key.startswith('_')}
    else:
        return dict(zip([column[0] for column in row.cursor_description], row))
@layout_bp.route('/debug_form', methods=['POST'])
def debug_form():
    """Endpoint de debug desabilitado."""
    return jsonify({"error": "Não encontrado"}), 404


@layout_bp.route('/')
def listar_layouts():
    bloqueio = _verificar_acesso_layout()
    if bloqueio:
        return bloqueio
    
    # Buscar layouts do banco
    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return render_template('importacao/layout/listar.html', layouts=[])
    
    try:
        cursor = conn.cursor()
        garantir_colunas_layout_escopo(cursor, conn)
        cursor.execute("""
            SELECT l.LayoutID, l.NomeLayout, l.Descricao, l.DataCriacao, l.UsuarioCriacao,
                   l.TipoEscopo, l.ObrigatorioNoEscopo,
                   (SELECT COUNT(*) FROM LayoutColunas c WHERE c.LayoutID = l.LayoutID) AS TotalColunas
            FROM Layouts l
            ORDER BY l.DataCriacao DESC
        """)
        layouts = cursor.fetchall()
        
        # Converter para lista de dicionários
        layouts_dict = []
        for layout in layouts:
            layout_dict = row_to_dict(layout)
            layout_dict['TipoEscopoLabel'] = label_tipo_escopo(layout_dict.get('TipoEscopo'))
            layout_dict['ObrigatorioNoEscopo'] = bool(layout_dict.get('ObrigatorioNoEscopo'))
            layouts_dict.append(layout_dict)
        
        # Criar dicionário com todas as variáveis necessárias para evitar erros no template
        template_vars = {
            'layouts': layouts_dict,
            'cond_pag': {},
            'escol': {},
            'enquadramento': {},
            'estado': {},
            'estadocivil': {},
            'municipio': {},
            'pais': {},
            'profissao': {},
            'segmentomercado': {},
            'tipologradouro': {},
            'departamento': {},
            'estoque': {},
            'naturezaoperacao': {},
            'equipe': {},
            'usuario_depara': {},
            'clasmontadora': {},
            'grupolucratividade': {},
            'grupoproduto': {},
            'pessoacodfabricante': {},
            'procedencia': {},
            'tabelapreco': {},
            'tipoproduto': {},
            'unidade': {},
            'combustivel': {},
            'corexterna': {},
            'corinterna': {},
            'marca': {},
            'modeloveiculo': {},
            'opcional': {},
            'setorservico': {},
            'tipoos': {},
            'tiposervico': {},
            'tmo': {},
            'veiculoano': {},
            'agentecobrador': {},
            'banco': {},
            'contagerencial': {},
            'tipocobranca': {},
            'tipocreditodebito': {},
            'tipodocumento': {},
            'tipoficharazao': {},
            'tipotitulo': {},
            'centroresultado': {},
            'historicopadrao': {},
            'planoconta': {},
            'subconta': {},
            'tipolote': {},
            'tiposubconta': {},
            'progresso_total': {
                'total_qtd': 0,
                'total_concluido': 0,
                'total_pendente': 0,
                'percentual_total': 0,
            },
            'escopos_habilitados': [],
            'progresso_categorias': {},
        }
        
        return render_template('importacao/layout/listar.html', **template_vars)
        
    except Exception as e:
        logger.error(f"Erro ao buscar layouts: {e}")
        flash("Erro ao carregar layouts", "error")
        
        # Retornar template com variáveis vazias mesmo em caso de erro
        empty_vars = {
            'layouts': [],
            'cond_pag': {},
            'escol': {},
            'enquadramento': {},
            'estado': {},
            'estadocivil': {},
            'municipio': {},
            'pais': {},
            'profissao': {},
            'segmentomercado': {},
            'tipologradouro': {},
            'departamento': {},
            'estoque': {},
            'naturezaoperacao': {},
            'equipe': {},
            'usuario_depara': {},
            'clasmontadora': {},
            'grupolucratividade': {},
            'grupoproduto': {},
            'pessoacodfabricante': {},
            'procedencia': {},
            'tabelapreco': {},
            'tipoproduto': {},
            'unidade': {},
            'combustivel': {},
            'corexterna': {},
            'corinterna': {},
            'marca': {},
            'modeloveiculo': {},
            'opcional': {},
            'setorservico': {},
            'tipoos': {},
            'tiposervico': {},
            'tmo': {},
            'veiculoano': {},
            'agentecobrador': {},
            'banco': {},
            'contagerencial': {},
            'tipocobranca': {},
            'tipocreditodebito': {},
            'tipodocumento': {},
            'tipoficharazao': {},
            'tipotitulo': {},
            'centroresultado': {},
            'historicopadrao': {},
            'planoconta': {},
            'subconta': {},
            'tipolote': {},
            'tiposubconta': {},
            'progresso_total': {
                'total_qtd': 0,
                'total_concluido': 0,
                'total_pendente': 0,
                'percentual_total': 0,
            },
            'escopos_habilitados': [],
            'progresso_categorias': {},
        }
        
        return render_template('importacao/layout/listar.html', **empty_vars)
    finally:
        if conn:
            conn.close()

@layout_bp.route('/novo', methods=['GET', 'POST'])
def novo_layout():
    bloqueio = _verificar_acesso_layout()
    if bloqueio:
        return bloqueio
    
    # Criar dicionário com todas as variáveis necessárias para evitar erros no template
    template_vars = {
        'cond_pag': {},
        'escol': {},
        'enquadramento': {},
        'estado': {},
        'estadocivil': {},
        'municipio': {},
        'pais': {},
        'profissao': {},
        'segmentomercado': {},
        'tipologradouro': {},
        'departamento': {},
        'estoque': {},
        'naturezaoperacao': {},
        'equipe': {},
        'usuario_depara': {},
        'clasmontadora': {},
        'grupolucratividade': {},
        'grupoproduto': {},
        'pessoacodfabricante': {},
        'procedencia': {},
        'tabelapreco': {},
        'tipoproduto': {},
        'unidade': {},
        'combustivel': {},
        'corexterna': {},
        'corinterna': {},
        'marca': {},
        'modeloveiculo': {},
        'opcional': {},
        'setorservico': {},
        'tipoos': {},
        'tiposervico': {},
        'tmo': {},
        'veiculoano': {},
        'agentecobrador': {},
        'banco': {},
        'contagerencial': {},
        'tipocobranca': {},
        'tipocreditodebito': {},
        'tipodocumento': {},
        'tipoficharazao': {},
        'tipotitulo': {},
        'centroresultado': {},
        'historicopadrao': {},
        'planoconta': {},
        'subconta': {},
        'tipolote': {},
        'tiposubconta': {},
        'progresso_total': {
            'total_qtd': 0,
            'total_concluido': 0,
            'total_pendente': 0,
            'percentual_total': 0,
        },
        'escopos_habilitados': [],
        'progresso_categorias': {},
        'tipos_escopo': TIPOS_ESCOPO_LAYOUT,
    }
    
    if request.method == 'POST':
        nome_layout = request.form.get('nome_layout')
        descricao = request.form.get('descricao')
        colunas_json = request.form.getlist('colunas[]')
        tipo_escopo, obrigatorio_escopo = ler_escopo_do_form(request.form)
        
        if not nome_layout:
            flash("Nome do layout é obrigatório", "error")
            return render_template('importacao/layout/novo.html', **template_vars)
        
        conn = conectar_banco()
        if not conn:
            flash("Erro ao conectar com o banco de dados", "error")
            return render_template('importacao/layout/novo.html', **template_vars)

        usuario_id = _usuario_criacao_id()
        layout_id = None

        try:
            cursor = conn.cursor()
            garantir_colunas_layout_escopo(cursor, conn)

            # MÉTODO 1: Tentar usar OUTPUT INSERTED (mais confiável)
            try:
                cursor.execute("""
                    INSERT INTO Layouts (NomeLayout, Descricao, UsuarioCriacao, DataCriacao, TipoEscopo, ObrigatorioNoEscopo)
                    OUTPUT INSERTED.LayoutID
                    VALUES (?, ?, ?, GETDATE(), ?, ?)
                """, (nome_layout, descricao, usuario_id, tipo_escopo, obrigatorio_escopo))
                
                result = cursor.fetchone()
                if result:
                    layout_id = result[0]
                else:
                    raise Exception("OUTPUT INSERTED não retornou resultado")
                    
            except Exception as e1:
                logger.warning("Insert layout método 1 falhou: %s", e1)
                
                # MÉTODO 2: Tentar usar SCOPE_IDENTITY() após INSERT
                try:
                    cursor.execute("""
                        INSERT INTO Layouts (NomeLayout, Descricao, UsuarioCriacao, DataCriacao, TipoEscopo, ObrigatorioNoEscopo)
                        VALUES (?, ?, ?, GETDATE(), ?, ?)
                    """, (nome_layout, descricao, usuario_id, tipo_escopo, obrigatorio_escopo))
                    
                    cursor.execute("SELECT SCOPE_IDENTITY()")
                    result = cursor.fetchone()
                    
                    if result and result[0]:
                        layout_id = int(result[0])
                    else:
                        raise Exception("SCOPE_IDENTITY não retornou resultado")
                        
                except Exception as e2:
                    logger.warning("Insert layout método 2 falhou: %s", e2)

                    cursor.execute("""
                        INSERT INTO Layouts (NomeLayout, Descricao, UsuarioCriacao, DataCriacao, TipoEscopo, ObrigatorioNoEscopo)
                        VALUES (?, ?, ?, GETDATE(), ?, ?)
                    """, (nome_layout, descricao, usuario_id, tipo_escopo, obrigatorio_escopo))

                    cursor.execute("SELECT @@IDENTITY")
                    result = cursor.fetchone()

                    if result and result[0]:
                        layout_id = int(result[0])
                    else:
                        cursor.execute("SELECT MAX(LayoutID) FROM Layouts")
                        result = cursor.fetchone()

                        if result and result[0]:
                            layout_id = int(result[0])
                        else:
                            flash("Erro: Não foi possível obter o ID do layout criado", "error")
                            conn.rollback()
                            return render_template('importacao/layout/novo.html', **template_vars)
            
            # Verificar se conseguimos um layout_id válido
            if not layout_id or layout_id <= 0:
                flash("Erro: ID do layout não foi gerado corretamente", "error")
                conn.rollback()
                return render_template('importacao/layout/novo.html', **template_vars)
            
            # Inserir colunas
            colunas_inseridas = 0
            for coluna_data in colunas_json:
                if coluna_data and coluna_data.strip():
                    try:
                        import json
                        coluna = json.loads(coluna_data)
                        
                        cursor.execute("""
                            INSERT INTO LayoutColunas (LayoutID, Posicao, Descricao, Obrigatorio, Validacao, TipoDado)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            layout_id, 
                            int(coluna.get('posicao', 0)), 
                            coluna.get('descricao', ''), 
                            1 if coluna.get('obrigatorio') else 0,
                            coluna.get('validacao', ''),
                            coluna.get('tipo_dado', 'texto')
                        ))
                        colunas_inseridas += 1
                        
                    except Exception as coluna_error:
                        logger.warning("Erro ao processar coluna do layout: %s", coluna_error)
                        continue
            
            conn.commit()
            flash(f"Layout '{nome_layout}' criado com sucesso! {colunas_inseridas} colunas adicionadas.", "success")
            return redirect(url_for('layout.listar_layouts'))
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Erro ao criar layout: {e}")
            flash(f"Erro ao criar layout: {str(e)}", "error")
            return render_template('importacao/layout/novo.html', **template_vars)
        finally:
            if conn:
                conn.close()
    
    return render_template('importacao/layout/novo.html', **template_vars)

@layout_bp.route('/api/visualizar/<int:layout_id>', methods=['GET'])
def api_visualizar_layout(layout_id):
    """JSON do layout para modal de visualização (somente leitura)."""
    if "usuario" not in session:
        return jsonify({"success": False, "message": "Não autenticado"}), 401

    if not acesso_envio_arquivos(session.get("projeto_selecionado")):
        return jsonify({"success": False, "message": "Acesso negado"}), 403

    conn = conectar_banco()
    if not conn:
        return jsonify({"success": False, "message": "Erro de conexão"}), 500

    try:
        cursor = conn.cursor()
        garantir_colunas_layout_escopo(cursor, conn)
        cursor.execute(
            """
            SELECT LayoutID, NomeLayout, Descricao, TipoEscopo, ObrigatorioNoEscopo
            FROM Layouts WHERE LayoutID = ?
            """,
            (layout_id,),
        )
        layout = cursor.fetchone()
        if not layout:
            return jsonify({"success": False, "message": "Layout não encontrado"}), 404

        cursor.execute(
            """
            SELECT Posicao, Descricao, Obrigatorio, Validacao, TipoDado
            FROM LayoutColunas
            WHERE LayoutID = ?
            ORDER BY Posicao
            """,
            (layout_id,),
        )
        colunas = []
        for row in cursor.fetchall():
            colunas.append({
                "Posicao": row.Posicao,
                "Descricao": row.Descricao or "",
                "Obrigatorio": bool(row.Obrigatorio),
                "Validacao": row.Validacao or "",
                "TipoDado": row.TipoDado or "texto",
            })

        from utils.importacao_forn_cli_telefone import (
            layout_eh_forn_cli_telefone,
            INSTRUCAO_TELEFONE,
        )
        layout_eh_tel = layout_eh_forn_cli_telefone(
            layout.NomeLayout, layout.Descricao, colunas,
        )

        return jsonify({
            "success": True,
            "layout": {
                "LayoutID": layout.LayoutID,
                "NomeLayout": layout.NomeLayout or "",
                "Descricao": layout.Descricao or "",
                "TipoEscopo": getattr(layout, "TipoEscopo", None) or "",
                "TipoEscopoLabel": label_tipo_escopo(getattr(layout, "TipoEscopo", None)),
                "ObrigatorioNoEscopo": bool(getattr(layout, "ObrigatorioNoEscopo", False)),
                "EhTelefone": layout_eh_tel,
                "InstrucaoTelefone": INSTRUCAO_TELEFONE if layout_eh_tel else "",
            },
            "colunas": colunas,
        })
    except Exception as e:
        logger.error(f"Erro API visualizar layout {layout_id}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        conn.close()


@layout_bp.route('/visualizar/<int:layout_id>', methods=['GET'])
def visualizar_layout(layout_id):
    """Consulta somente leitura do layout (sem edição)."""
    bloqueio = _verificar_acesso_visualizar_layout()
    if bloqueio:
        return bloqueio

    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return redirect(url_for("dashboard.dashboard"))

    try:
        cursor = conn.cursor()
        garantir_colunas_layout_escopo(cursor, conn)
        cursor.execute("SELECT * FROM Layouts WHERE LayoutID = ?", (layout_id,))
        layout = cursor.fetchone()
        if not layout:
            flash("Layout não encontrado", "error")
            return redirect(url_for("dashboard.dashboard"))

        cursor.execute(
            "SELECT * FROM LayoutColunas WHERE LayoutID = ? ORDER BY Posicao",
            (layout_id,),
        )
        colunas = cursor.fetchall()
        layout_dict = row_to_dict(layout)
        layout_dict['TipoEscopoLabel'] = label_tipo_escopo(layout_dict.get('TipoEscopo'))
        layout_dict['ObrigatorioNoEscopo'] = bool(layout_dict.get('ObrigatorioNoEscopo'))
        return render_template(
            "importacao/layout/visualizar.html",
            layout=layout_dict,
            colunas=[row_to_dict(c) for c in colunas],
            tipos_escopo=TIPOS_ESCOPO_LAYOUT,
        )
    except Exception as e:
        logger.error(f"Erro ao visualizar layout {layout_id}: {e}")
        flash(f"Erro ao visualizar layout: {e}", "error")
        return redirect(url_for("dashboard.dashboard"))
    finally:
        conn.close()


@layout_bp.route('/editar/<int:layout_id>', methods=['GET', 'POST'])
def editar_layout(layout_id):
    bloqueio = _verificar_acesso_layout()
    if bloqueio:
        return bloqueio
    
    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return redirect(url_for('layout.listar_layouts'))
    
    try:
        cursor = conn.cursor()
        garantir_colunas_layout_escopo(cursor, conn)
        
        if request.method == 'POST':
            nome_layout = request.form.get('nome_layout')
            descricao = request.form.get('descricao')
            colunas = request.form.getlist('colunas[]')
            tipo_escopo, obrigatorio_escopo = ler_escopo_do_form(request.form)
            
            if not nome_layout:
                flash("Nome do layout é obrigatório", "error")
                return redirect(url_for('layout.editar_layout', layout_id=layout_id))
            
            # Antes de aplicar as mudanças, salva o estado atual como uma versão.
            layout_versao.registrar_snapshot(
                cursor,
                layout_id,
                tipo_acao=layout_versao.TIPO_EDICAO,
                usuario_id=_usuario_criacao_id(),
            )

            # Atualizar layout
            cursor.execute("""
                UPDATE Layouts 
                SET NomeLayout = ?, Descricao = ?, TipoEscopo = ?, ObrigatorioNoEscopo = ?
                WHERE LayoutID = ?
            """, (nome_layout, descricao, tipo_escopo, obrigatorio_escopo, layout_id))
            
            # Remover colunas existentes
            cursor.execute("DELETE FROM LayoutColunas WHERE LayoutID = ?", (layout_id,))
            
            # Inserir novas colunas
            for coluna_data in colunas:
                if coluna_data:
                    import json
                    coluna = json.loads(coluna_data)
                    cursor.execute("""
                        INSERT INTO LayoutColunas (LayoutID, Posicao, Descricao, Obrigatorio, Validacao, TipoDado)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        layout_id, 
                        coluna['posicao'], 
                        coluna['descricao'], 
                        1 if coluna['obrigatorio'] else 0,
                        coluna['validacao'],
                        coluna['tipo_dado']
                    ))
            
            conn.commit()
            flash("Layout atualizado com sucesso!", "success")
            return redirect(url_for('layout.listar_layouts'))
        
        else:
            # Buscar layout
            cursor.execute("SELECT * FROM Layouts WHERE LayoutID = ?", (layout_id,))
            layout = cursor.fetchone()
            
            if not layout:
                flash("Layout não encontrado", "error")
                return redirect(url_for('layout.listar_layouts'))
            
            # Buscar colunas
            cursor.execute("SELECT * FROM LayoutColunas WHERE LayoutID = ? ORDER BY Posicao", (layout_id,))
            colunas = cursor.fetchall()
            
            # Converter para dicionários
            layout_dict = row_to_dict(layout)
            layout_dict['ObrigatorioNoEscopo'] = bool(layout_dict.get('ObrigatorioNoEscopo'))
            colunas_dict = [row_to_dict(coluna) for coluna in colunas]
            
            return render_template(
                'importacao/layout/editar.html',
                layout=layout_dict,
                colunas=colunas_dict,
                tipos_escopo=TIPOS_ESCOPO_LAYOUT,
            )
            
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erro ao editar layout: {e}")
        flash(f"Erro ao editar layout: {str(e)}", "error")
        return redirect(url_for('layout.listar_layouts'))
    finally:
        if conn:
            conn.close()

@layout_bp.route('/excluir/<int:layout_id>', methods=['POST'])
def excluir_layout(layout_id):
    bloqueio = _verificar_acesso_layout()
    if bloqueio:
        if request.accept_mimetypes.accept_json:
            return jsonify({"success": False, "message": "Acesso negado"}), 403
        return bloqueio
    
    conn = conectar_banco()
    if not conn:
        return jsonify({"success": False, "message": "Erro de conexão"}), 500
    
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Layouts WHERE LayoutID = ?", (layout_id,))
        conn.commit()
        
        return jsonify({"success": True, "message": "Layout excluído com sucesso"})
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Erro ao excluir layout: {e}")
        return jsonify({"success": False, "message": f"Erro ao excluir layout: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()


@layout_bp.route('/versoes/<int:layout_id>')
def versoes_layout(layout_id):
    """Lista o histórico de versões de um layout."""
    bloqueio = _verificar_acesso_layout()
    if bloqueio:
        return bloqueio

    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return redirect(url_for('layout.listar_layouts'))

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT LayoutID, NomeLayout, Descricao FROM Layouts WHERE LayoutID = ?",
            (layout_id,),
        )
        layout = cursor.fetchone()
        if not layout:
            flash("Layout não encontrado", "error")
            return redirect(url_for('layout.listar_layouts'))

        layout_dict = {
            "LayoutID": layout[0],
            "NomeLayout": layout[1],
            "Descricao": layout[2],
        }
    except Exception as e:
        logger.error(f"Erro ao carregar layout {layout_id}: {e}")
        flash("Erro ao carregar layout", "error")
        return redirect(url_for('layout.listar_layouts'))
    finally:
        if conn:
            conn.close()

    versoes = layout_versao.listar_versoes(layout_id)
    return render_template(
        'importacao/layout/versoes.html',
        layout=layout_dict,
        versoes=versoes,
    )


@layout_bp.route('/versao/<int:layout_versao_id>')
def ver_versao(layout_versao_id):
    """Retorna os detalhes de uma versão específica (JSON, usado no modal)."""
    bloqueio = _verificar_acesso_layout()
    if bloqueio:
        if request.accept_mimetypes.accept_json:
            return jsonify({"success": False, "message": "Acesso negado"}), 403
        return bloqueio

    versao = layout_versao.obter_versao(layout_versao_id)
    if not versao:
        return jsonify({"success": False, "message": "Versão não encontrada"}), 404

    data_versao = versao.get("DataVersao")
    if data_versao is not None and not isinstance(data_versao, str):
        versao["DataVersao"] = data_versao.strftime("%d/%m/%Y %H:%M")

    return jsonify({"success": True, "versao": versao})


@layout_bp.route('/restaurar/<int:layout_versao_id>', methods=['POST'])
def restaurar_versao_layout(layout_versao_id):
    """Restaura o layout para o estado de uma versão anterior."""
    bloqueio = _verificar_acesso_layout()
    if bloqueio:
        if request.accept_mimetypes.accept_json:
            return jsonify({"success": False, "message": "Acesso negado"}), 403
        return bloqueio

    sucesso, mensagem = layout_versao.restaurar_versao(
        layout_versao_id,
        usuario_id=_usuario_criacao_id(),
    )

    status = 200 if sucesso else 400
    return jsonify({"success": sucesso, "message": mensagem}), status