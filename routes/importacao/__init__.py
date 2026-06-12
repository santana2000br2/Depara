# No arquivo routes/importacao/__init__.py
import pandas as pd
import io
import uuid
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, send_file
from db.connection import conectar_banco
from logger import logger

importacao_bp = Blueprint('importacao', __name__)

# Dicionário para armazenar temporariamente os dados de processamento
temp_data_store = {}

def row_to_dict(row):
    """Converte uma linha do banco para dicionário"""
    if hasattr(row, '_asdict'):
        return row._asdict()
    elif hasattr(row, '__dict__'):
        return {key: value for key, value in row.__dict__.items() if not key.startswith('_')}
    else:
        return dict(zip([column[0] for column in row.cursor_description], row))

@importacao_bp.route('/', methods=['GET', 'POST'])
def index():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))
    
    # Buscar layouts do banco
    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return render_template('importacao/index.html', layouts=[])
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT LayoutID, NomeLayout, Descricao, DataCriacao, UsuarioCriacao
            FROM Layouts 
            ORDER BY NomeLayout
        """)
        layouts = cursor.fetchall()
        
        # Converter para lista de dicionários
        layouts_dict = []
        for layout in layouts:
            layout_dict = row_to_dict(layout)
            layouts_dict.append(layout_dict)
        
        # Variáveis template vazias para evitar erros
        template_vars = {
            'layouts': layouts_dict,
            'cond_pag': {},
            'escol': {},
            # ... (todas as outras variáveis vazias)
        }
        
        if request.method == 'POST':
            return processar_arquivo_com_layout(request, template_vars)
        
        return render_template('importacao/index.html', **template_vars)
        
    except Exception as e:
        logger.error(f"Erro ao buscar layouts: {e}")
        flash("Erro ao carregar layouts", "error")
        return render_template('importacao/index.html', layouts=[])
    finally:
        if conn:
            conn.close()

def processar_arquivo_com_layout(request, template_vars):
    """Processa o arquivo com o layout selecionado"""
    if "arquivo" not in request.files:
        flash("Nenhum arquivo selecionado.", "error")
        return render_template('importacao/index.html', **template_vars)
    
    arquivo = request.files["arquivo"]
    layout_id = request.form.get("layout_id")
    
    if arquivo.filename == "":
        flash("Nenhum arquivo selecionado.", "error")
        return render_template('importacao/index.html', **template_vars)
    
    if not layout_id:
        flash("Selecione um layout para validar o arquivo.", "error")
        return render_template('importacao/index.html', **template_vars)
    
    # Buscar informações do layout selecionado
    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return render_template('importacao/index.html', **template_vars)
    
    try:
        cursor = conn.cursor()
        
        # Buscar layout
        cursor.execute("SELECT * FROM Layouts WHERE LayoutID = ?", (layout_id,))
        layout = cursor.fetchone()
        
        if not layout:
            flash("Layout selecionado não encontrado", "error")
            return render_template('importacao/index.html', **template_vars)
        
        layout_dict = row_to_dict(layout)
        
        # Buscar colunas do layout
        cursor.execute("SELECT * FROM LayoutColunas WHERE LayoutID = ? ORDER BY Posicao", (layout_id,))
        colunas = cursor.fetchall()
        colunas_dict = [row_to_dict(coluna) for coluna in colunas]
        
        # Validar arquivo com o layout
        from utils.layout_validation import validar_arquivo_com_layout
        
        df_processado, df_erros, mensagem = validar_arquivo_com_layout(arquivo, colunas_dict)
        
        if df_processado is None:
            flash(f"Erro na validação: {mensagem}", "error")
            return render_template('importacao/index.html', **template_vars)
        
        # Gerar um ID único para este processamento
        process_id = str(uuid.uuid4())
        
        # Armazenar os DataFrames temporariamente
        temp_data_store[process_id] = {
            'df_processado': df_processado,
            'df_erros': df_erros,
            'layout_nome': layout_dict['NomeLayout'],
            'timestamp': pd.Timestamp.now()
        }
                       
     
               # Preparar dados para exibição
        dados_processados = None
        if df_processado is not None and not df_processado.empty:
            display_df = df_processado.head(10)  # Mostrar apenas 10 linhas
            dados_processados = display_df.to_html(classes="table table-striped", index=False, escape=False)
        
        # Verificar se df_erros não é None antes de calcular o tamanho
        if df_erros is not None and not df_erros.empty:
            total_erros = len(df_erros)
        else:
            total_erros = 0
        
        flash(f"Arquivo validado com layout '{layout_dict['NomeLayout']}'", "success")
        if total_erros > 0:
            flash(f"Encontrados {total_erros} erros de validação", "warning")
        else:
            flash("Nenhum erro encontrado na validação", "success")
        
        # Adicionar resultados ao template_vars
        template_vars.update({
            'dados_processados': dados_processados,
            'df_errors': df_erros,
            'erros_processados': total_erros > 0,
            'layout': layout_dict['NomeLayout'],
            'total_erros': total_erros,
            'process_id': process_id,  # Passar o process_id para o template
        })
        
        return render_template('importacao/index.html', **template_vars)
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo com layout: {e}")
        flash(f"Erro ao processar arquivo: {str(e)}", "error")
        return render_template('importacao/index.html', **template_vars)
    finally:
        if conn:
            conn.close()

@importacao_bp.route('/exportar_erros/<process_id>')
def exportar_erros(process_id):
    """Exporta os erros de validação para Excel"""
    if process_id not in temp_data_store:
        flash("Dados de exportação não encontrados ou expirados.", "error")
        return redirect(url_for('importacao.index'))
    
    data = temp_data_store[process_id]
    df_erros = data['df_erros']
    
    if df_erros is None or df_erros.empty:
        flash("Nenhum erro para exportar.", "info")
        return redirect(url_for('importacao.index'))
    
    # Criar arquivo Excel em memória
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_erros.to_excel(writer, sheet_name='Erros_Validacao', index=False)
    
    output.seek(0)
    
    filename = f"erros_validacao_{data['layout_nome']}_{data['timestamp'].strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename
    )

@importacao_bp.route('/exportar_processado/<process_id>')
def exportar_processado(process_id):
    """Exporta o arquivo processado para Excel"""
    if process_id not in temp_data_store:
        flash("Dados de exportação não encontrados ou expirados.", "error")
        return redirect(url_for('importacao.index'))
    
    data = temp_data_store[process_id]
    df_processado = data['df_processado']
    
    if df_processado is None or df_processado.empty:
        flash("Nenhum dado processado para exportar.", "info")
        return redirect(url_for('importacao.index'))
    
    # Criar arquivo Excel em memória
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_processado.to_excel(writer, sheet_name='Dados_Processados', index=False)
    
    output.seek(0)
    
    filename = f"dados_processados_{data['layout_nome']}_{data['timestamp'].strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename
    )

# Função para limpar dados temporários antigos
def limpar_dados_temporarios():
    """Limpa dados temporários com mais de 1 hora"""
    import datetime
    agora = datetime.datetime.now()
    chaves_remover = []
    
    for chave, dados in temp_data_store.items():
        if (agora - dados['timestamp'].to_pydatetime()).total_seconds() > 3600:
            chaves_remover.append(chave)
    
    for chave in chaves_remover:
        del temp_data_store[chave]

# Importar o layout_bp também aqui para organização
from .layout import layout_bp