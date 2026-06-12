from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify
from db.connection import conectar_banco
from logger import logger

layout_bp = Blueprint('layout', __name__)

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
    """Rota temporária para debug do formulário"""
    print("=== DEBUG FORM DATA ===")
    print("Form data:", request.form)
    print("Files:", request.files)
    print("JSON data:", request.get_json())
    return "Debug completo - verifique o console do servidor"

@layout_bp.route('/')
def listar_layouts():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))
    
    # Verificar se é administrador
    usuario = session.get("usuario", {})
    if not usuario.get("adm"):
        flash("Acesso negado. Apenas administradores podem gerenciar layouts.", "error")
        return redirect(url_for("importacao.index"))
    
    # Buscar layouts do banco
    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return render_template('importacao/layout/listar.html', layouts=[])
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT LayoutID, NomeLayout, Descricao, DataCriacao, UsuarioCriacao
            FROM Layouts 
            ORDER BY DataCriacao DESC
        """)
        layouts = cursor.fetchall()
        
        # Converter para lista de dicionários
        layouts_dict = []
        for layout in layouts:
            layout_dict = row_to_dict(layout)
            layouts_dict.append(layout_dict)
        
        # Criar dicionário com todas as variáveis necessárias para evitar erros no template
        template_vars = {
            'layouts': layouts_dict,
            'cond_pag': {},
            'escol': {},
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
    if "usuario" not in session:
        return redirect(url_for("auth.login"))
    
    usuario = session.get("usuario", {})
    if not usuario.get("adm"):
        flash("Acesso negado. Apenas administradores podem criar layouts.", "error")
        return redirect(url_for("importacao.index"))
    
    # Criar dicionário com todas as variáveis necessárias para evitar erros no template
    template_vars = {
        'cond_pag': {},
        'escol': {},
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
    
    if request.method == 'POST':
        nome_layout = request.form.get('nome_layout')
        descricao = request.form.get('descricao')
        colunas_json = request.form.getlist('colunas[]')
        
        print(f"DEBUG: Nome do layout: {nome_layout}")
        print(f"DEBUG: Descrição: {descricao}")
        print(f"DEBUG: Colunas recebidas: {colunas_json}")
        
        if not nome_layout:
            flash("Nome do layout é obrigatório", "error")
            return render_template('importacao/layout/novo.html', **template_vars)
        
        conn = conectar_banco()
        if not conn:
            flash("Erro ao conectar com o banco de dados", "error")
            return render_template('importacao/layout/novo.html', **template_vars)
        
        try:
            cursor = conn.cursor()
            
            # MÉTODO 1: Tentar usar OUTPUT INSERTED (mais confiável)
            try:
                cursor.execute("""
                    INSERT INTO Layouts (NomeLayout, Descricao, UsuarioCriacao, DataCriacao)
                    OUTPUT INSERTED.LayoutID
                    VALUES (?, ?, ?, GETDATE())
                """, (nome_layout, descricao, usuario.get("usuario_id", 1)))
                
                result = cursor.fetchone()
                if result:
                    layout_id = result[0]
                    print(f"DEBUG: Layout criado com ID (Método 1): {layout_id}")
                else:
                    raise Exception("OUTPUT INSERTED não retornou resultado")
                    
            except Exception as e1:
                print(f"DEBUG: Método 1 falhou: {e1}")
                
                # MÉTODO 2: Tentar usar SCOPE_IDENTITY() após INSERT
                try:
                    cursor.execute("""
                        INSERT INTO Layouts (NomeLayout, Descricao, UsuarioCriacao, DataCriacao)
                        VALUES (?, ?, ?, GETDATE())
                    """, (nome_layout, descricao, usuario.get("usuario_id", 1)))
                    
                    cursor.execute("SELECT SCOPE_IDENTITY()")
                    result = cursor.fetchone()
                    
                    if result and result[0]:
                        layout_id = int(result[0])
                        print(f"DEBUG: Layout criado com ID (Método 2): {layout_id}")
                    else:
                        raise Exception("SCOPE_IDENTITY não retornou resultado")
                        
                except Exception as e2:
                    print(f"DEBUG: Método 2 falhou: {e2}")
                    
                    # MÉTODO 3: Tentar usar @@IDENTITY
                    try:
                        cursor.execute("SELECT @@IDENTITY")
                        result = cursor.fetchone()
                        
                        if result and result[0]:
                            layout_id = int(result[0])
                            print(f"DEBUG: Layout criado com ID (Método 3): {layout_id}")
                        else:
                            raise Exception("@@IDENTITY não retornou resultado")
                            
                    except Exception as e3:
                        print(f"DEBUG: Método 3 falhou: {e3}")
                        
                        # MÉTODO 4: Buscar o último ID manualmente
                        cursor.execute("SELECT MAX(LayoutID) FROM Layouts")
                        result = cursor.fetchone()
                        
                        if result and result[0]:
                            layout_id = int(result[0])
                            print(f"DEBUG: Último ID encontrado (Método 4): {layout_id}")
                        else:
                            flash("Erro: Não foi possível obter o ID do layout criado", "error")
                            conn.rollback()
                            return render_template('importacao/layout/novo.html', **template_vars)
            
            # Verificar se conseguimos um layout_id válido
            if not layout_id or layout_id <= 0:
                flash("Erro: ID do layout não foi gerado corretamente", "error")
                conn.rollback()
                return render_template('importacao/layout/novo.html', **template_vars)
            
            print(f"DEBUG: Inserindo colunas para o layout ID: {layout_id}")
            
            # Inserir colunas
            colunas_inseridas = 0
            for coluna_data in colunas_json:
                if coluna_data and coluna_data.strip():
                    try:
                        import json
                        coluna = json.loads(coluna_data)
                        print(f"DEBUG: Processando coluna: {coluna}")
                        
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
                        print(f"DEBUG: Coluna {colunas_inseridas} inserida com sucesso")
                        
                    except Exception as coluna_error:
                        print(f"DEBUG: Erro ao processar coluna {coluna_data}: {coluna_error}")
                        import traceback
                        traceback.print_exc()
                        continue
            
            conn.commit()
            print(f"DEBUG: Commit realizado - Layout {layout_id} com {colunas_inseridas} colunas")
            flash(f"Layout '{nome_layout}' criado com sucesso! {colunas_inseridas} colunas adicionadas.", "success")
            return redirect(url_for('layout.listar_layouts'))
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Erro ao criar layout: {e}")
            import traceback
            traceback.print_exc()
            flash(f"Erro ao criar layout: {str(e)}", "error")
            return render_template('importacao/layout/novo.html', **template_vars)
        finally:
            if conn:
                conn.close()
    
    return render_template('importacao/layout/novo.html', **template_vars)

@layout_bp.route('/editar/<int:layout_id>', methods=['GET', 'POST'])
def editar_layout(layout_id):
    if "usuario" not in session:
        return redirect(url_for("auth.login"))
    
    usuario = session.get("usuario", {})
    if not usuario.get("adm"):
        flash("Acesso negado. Apenas administradores podem editar layouts.", "error")
        return redirect(url_for("importacao.index"))
    
    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return redirect(url_for('layout.listar_layouts'))
    
    try:
        cursor = conn.cursor()
        
        if request.method == 'POST':
            nome_layout = request.form.get('nome_layout')
            descricao = request.form.get('descricao')
            colunas = request.form.getlist('colunas[]')
            
            if not nome_layout:
                flash("Nome do layout é obrigatório", "error")
                return redirect(url_for('layout.editar_layout', layout_id=layout_id))
            
            # Atualizar layout
            cursor.execute("""
                UPDATE Layouts 
                SET NomeLayout = ?, Descricao = ?
                WHERE LayoutID = ?
            """, (nome_layout, descricao, layout_id))
            
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
            colunas_dict = [row_to_dict(coluna) for coluna in colunas]
            
            return render_template('importacao/layout/editar.html', layout=layout_dict, colunas=colunas_dict)
            
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
    if "usuario" not in session:
        return jsonify({"success": False, "message": "Não autenticado"}), 401
    
    usuario = session.get("usuario", {})
    if not usuario.get("adm"):
        return jsonify({"success": False, "message": "Acesso negado"}), 403
    
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