# No arquivo routes/importacao/__init__.py
import pandas as pd
import io
import uuid
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, send_file, jsonify
from db.connection import conectar_banco
from logger import logger

from utils.importacao_store import salvar_processamento, carregar_processamento

importacao_bp = Blueprint('importacao', __name__)


def base_template_vars(**kwargs):
    """Variáveis padrão exigidas pelo base.html."""
    vars_dict = {
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
        'importacao_base': {'exibe': False, 'pode': False, 'motivo': ''},
        'importacao_ultimo_resultado': None,
    }
    vars_dict.update(kwargs)
    return vars_dict


def row_to_dict(row):
    """Converte uma linha do banco para dicionário"""
    if hasattr(row, '_asdict'):
        return row._asdict()
    elif hasattr(row, '__dict__'):
        return {key: value for key, value in row.__dict__.items() if not key.startswith('_')}
    else:
        return dict(zip([column[0] for column in row.cursor_description], row))


def _detectar_tipo_importacao(nome_layout, descricao_layout=None, colunas_layout=None):
    from utils.importacao_forn_cli import layout_eh_forn_cli
    from utils.importacao_forn_cli_documento import layout_eh_forn_cli_documento
    from utils.importacao_forn_cli_endereco import layout_eh_forn_cli_endereco
    from utils.importacao_forn_cli_enquadramento import layout_eh_forn_cli_enquadramento

    if layout_eh_forn_cli(nome_layout, descricao_layout, colunas_layout):
        return 'forn_cli'
    if layout_eh_forn_cli_endereco(nome_layout, descricao_layout, colunas_layout):
        return 'forn_cli_endereco'
    if layout_eh_forn_cli_documento(nome_layout, descricao_layout, colunas_layout):
        return 'forn_cli_documento'
    if layout_eh_forn_cli_enquadramento(nome_layout, descricao_layout, colunas_layout):
        return 'forn_cli_enquadramento'
    return None


def _config_importacao_por_tipo(tipo):
    from utils.importacao_procedures import obter_config_procedure

    cfg_proc = obter_config_procedure(tipo) or {}
    proc = cfg_proc.get('procedure', '')

    if tipo == 'forn_cli':
        return {
            'tabela_destino': cfg_proc.get('destino', 'Pessoa_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'Nome, CPF/CNPJ ou colunas faltando no arquivo',
            'flash_flag': 'consulte Ocorrencia em {banco}.dbo.Pessoa_MG.',
        }
    if tipo == 'forn_cli_documento':
        return {
            'tabela_destino': cfg_proc.get('destino', 'PessoaDocumento_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CPF/CNPJ ausente, não cadastrado em Pessoa_MG ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.PessoaDocumento_MG. '
                'Requer cadastro prévio em Pessoa_MG (layout Forn_cli).'
            ),
        }
    if tipo == 'forn_cli_endereco':
        return {
            'tabela_destino': cfg_proc.get('destino', 'PessoaEndereco_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CPF/CNPJ ausente, não cadastrado em Pessoa_MG ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.PessoaEndereco_MG. '
                'Requer cadastro prévio em Pessoa_MG (layout 1 Forn_cli.txt).'
            ),
        }
    if tipo == 'forn_cli_enquadramento':
        return {
            'tabela_destino': cfg_proc.get('destino', 'PessoaEnquadramento_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CPF/CNPJ ausente, não cadastrado em Pessoa_MG ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.PessoaEnquadramento_MG. '
                'Requer cadastro prévio em Pessoa_MG (layout 1 Forn_cli.txt).'
            ),
        }
    return {
        'tabela_destino': '',
        'procedure': '',
        'motivo_nao_suportado': 'importação automática indisponível',
        'motivo_erros': 'erros bloqueantes no arquivo',
        'flash_flag': '',
    }


def _status_importacao_base(nome_layout, df_processado, descricao_layout=None, colunas_layout=None, tem_erros_bloqueantes=False):
    """Indica se a importação automática pode rodar após a validação."""
    tipo = _detectar_tipo_importacao(nome_layout, descricao_layout, colunas_layout)
    cfg = _config_importacao_por_tipo(tipo)
    logger.info(
        "Importação base: layout=%r descricao=%r tipo=%s",
        nome_layout, descricao_layout, tipo,
    )

    if not tipo:
        return {
            'exibe': False,
            'pode': False,
            'motivo': f'Layout "{nome_layout}" não é Forn_cli ({cfg["motivo_nao_suportado"]}).',
            'tipo': None,
            'tabela_destino': '',
        }

    if tem_erros_bloqueantes:
        return {
            'exibe': True,
            'pode': False,
            'motivo': (
                'Importação não realizada: existem erros bloqueantes '
                f'({cfg["motivo_erros"]}).'
            ),
            'tipo': tipo,
            'tabela_destino': cfg['tabela_destino'],
            'procedure': cfg.get('procedure', ''),
        }

    if df_processado is None or df_processado.empty:
        return {
            'exibe': True,
            'pode': False,
            'motivo': 'Nenhum dado processado para importar.',
            'tipo': tipo,
            'tabela_destino': cfg['tabela_destino'],
            'procedure': cfg.get('procedure', ''),
        }

    if 'projeto_selecionado' not in session:
        return {
            'exibe': True,
            'pode': False,
            'motivo': 'Selecione um projeto antes de importar para o banco.',
            'tipo': tipo,
            'tabela_destino': cfg['tabela_destino'],
            'procedure': cfg.get('procedure', ''),
        }

    projeto = session['projeto_selecionado']
    banco_gx = projeto.get('DadosGX')
    banco_wf = projeto.get('BancoHomo')
    if not banco_gx or not banco_wf:
        return {
            'exibe': True,
            'pode': False,
            'motivo': 'Configure DadosGX e BancoHomo no cadastro do projeto.',
            'tipo': tipo,
            'tabela_destino': cfg['tabela_destino'],
            'procedure': cfg.get('procedure', ''),
        }

    return {
        'exibe': True,
        'pode': True,
        'motivo': '',
        'banco_gx': banco_gx,
        'banco_wf': banco_wf,
        'tipo': tipo,
        'tabela_destino': cfg['tabela_destino'],
        'procedure': cfg.get('procedure', ''),
    }


def _executar_importacao_automatica(layout_nome, df_processado, layout_descricao=None, colunas_layout=None):
    """Importa layouts Forn_cli quando validação não tem erros bloqueantes."""
    from utils.importacao_forn_cli import importar_forn_cli_para_base
    from utils.importacao_forn_cli_documento import importar_forn_cli_documento_para_base
    from utils.importacao_forn_cli_endereco import importar_forn_cli_endereco_para_base
    from utils.importacao_forn_cli_enquadramento import importar_forn_cli_enquadramento_para_base
    from utils.importacao_depara_pessoa import formatar_resumo_depara

    tipo = _detectar_tipo_importacao(layout_nome, layout_descricao, colunas_layout)
    if not tipo:
        return None

    cfg = _config_importacao_por_tipo(tipo)
    status = _status_importacao_base(
        layout_nome, df_processado, layout_descricao, colunas_layout, tem_erros_bloqueantes=False,
    )
    if not status.get('pode'):
        if status.get('exibe') and status.get('motivo'):
            flash(status['motivo'], 'warning')
        return {
            'sucesso': False,
            'mensagem': status.get('motivo', 'Importação automática não disponível.'),
            'resumo': {},
            'banco_gx': status.get('banco_gx'),
            'automatica': True,
            'tipo': tipo,
            'tabela_destino': cfg['tabela_destino'],
        }

    banco_gx = status['banco_gx']
    banco_wf = status['banco_wf']
    if tipo == 'forn_cli_documento':
        sucesso, mensagem, resumo = importar_forn_cli_documento_para_base(df_processado, banco_gx, banco_wf)
    elif tipo == 'forn_cli_endereco':
        sucesso, mensagem, resumo = importar_forn_cli_endereco_para_base(df_processado, banco_gx, banco_wf)
    elif tipo == 'forn_cli_enquadramento':
        sucesso, mensagem, resumo = importar_forn_cli_enquadramento_para_base(df_processado, banco_gx, banco_wf)
    else:
        sucesso, mensagem, resumo = importar_forn_cli_para_base(df_processado, banco_gx, banco_wf)

    resultado = {
        'sucesso': sucesso,
        'mensagem': mensagem,
        'resumo': resumo,
        'banco_gx': banco_gx,
        'automatica': True,
        'tipo': tipo,
        'tabela_destino': cfg['tabela_destino'],
        'procedure': cfg.get('procedure', ''),
    }

    if sucesso:
        flash('Arquivo validado e importado automaticamente para o banco.', 'success')
        flash(mensagem, 'success')
        if resumo.get('depara'):
            flash(f"De/Para: {formatar_resumo_depara(resumo['depara'])}", 'info')
        if resumo.get('flag_0', 0) > 0:
            flash(
                f"{resumo['flag_0']} registro(s) com ocorrências (Flag=0) — "
                f"{cfg['flash_flag'].format(banco=banco_gx)}",
                'warning',
            )
    else:
        flash(mensagem, 'error')

    return resultado


def _montar_vars_resultado(process_id, layout_nome, layout_id, df_processado, df_erros, df_avisos, mensagem, descricao_layout=None, colunas_layout=None, importacao_resultado=None):
    """Monta variáveis de template para exibir resultado do processamento."""
    total_erros = len(df_erros) if df_erros is not None and not df_erros.empty else 0
    total_avisos = len(df_avisos) if df_avisos is not None and not df_avisos.empty else 0

    return {
        'erros_processados': total_erros > 0,
        'layout': layout_nome,
        'layout_id_selecionado': layout_id,
        'total_erros': total_erros,
        'total_avisos': total_avisos,
        'process_id': process_id,
        'importacao_base': _status_importacao_base(
            layout_nome, df_processado, descricao_layout, colunas_layout,
            tem_erros_bloqueantes=total_erros > 0,
        ),
        'importacao_ultimo_resultado': importacao_resultado,
        'mensagem_validacao': mensagem,
    }


def _restaurar_processamento(process_id):
    usuario_id = session.get('usuario', {}).get('usuario_id')
    data = carregar_processamento(process_id, usuario_id)
    if not data:
        return None
    return _montar_vars_resultado(
        process_id,
        data.get('layout_nome', ''),
        data.get('layout_id'),
        data.get('df_processado'),
        data.get('df_erros'),
        data.get('df_avisos'),
        data.get('mensagem', ''),
        data.get('layout_descricao'),
        data.get('colunas_layout'),
    )


def _pode_importar_forn_cli(nome_layout, df_processado):
    return _status_importacao_base(nome_layout, df_processado).get('pode', False)

@importacao_bp.route('/', methods=['GET', 'POST'])
def index():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))
    
    # Buscar layouts do banco
    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return render_template('importacao/index.html', **base_template_vars(layouts=[]))
    
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
        
        template_vars = base_template_vars(layouts=layouts_dict)
        
        if request.method == 'POST':
            return processar_arquivo_com_layout(request, template_vars)

        process_id = request.args.get('process_id')
        if process_id:
            vars_resultado = _restaurar_processamento(process_id)
            if vars_resultado:
                template_vars.update(vars_resultado)
            else:
                flash('Resultado do processamento expirado ou não encontrado. Processe o arquivo novamente.', 'warning')

        return render_template('importacao/index.html', **template_vars)
        
    except Exception as e:
        logger.error(f"Erro ao buscar layouts: {e}")
        flash("Erro ao carregar layouts", "error")
        return render_template('importacao/index.html', **base_template_vars(layouts=[]))
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

        if not colunas_dict:
            flash("O layout selecionado não possui colunas configuradas. Edite o layout antes de importar.", "error")
            return render_template('importacao/index.html', **template_vars)
        
        # Validar arquivo com o layout
        from utils.layout_validation import validar_arquivo_com_layout

        projeto = session.get('projeto_selecionado') or {}
        banco_gx = projeto.get('DadosGX')

        df_processado, df_erros, df_avisos, mensagem = validar_arquivo_com_layout(
            arquivo,
            colunas_dict,
            layout_nome=layout_dict['NomeLayout'],
            layout_descricao=layout_dict.get('Descricao'),
            banco_gx=banco_gx,
        )
        
        if df_processado is None:
            flash(mensagem, "error")
            return render_template('importacao/index.html', **template_vars)
        
        # Gerar um ID único para este processamento
        process_id = str(uuid.uuid4())
        usuario_id = session.get("usuario", {}).get("usuario_id")

        salvar_processamento(
            process_id,
            usuario_id,
            layout_dict['NomeLayout'],
            df_processado,
            df_erros if df_erros is not None else pd.DataFrame(),
            df_avisos if df_avisos is not None else pd.DataFrame(),
            layout_id=layout_id,
            layout_descricao=layout_dict.get('Descricao'),
            colunas_layout=colunas_dict,
        )
        
        if df_erros is not None and not df_erros.empty:
            total_erros = len(df_erros)
        else:
            total_erros = 0

        if df_avisos is not None and not df_avisos.empty:
            total_avisos = len(df_avisos)
        else:
            total_avisos = 0
        
        flash(f"Arquivo validado com layout '{layout_dict['NomeLayout']}'", "success")
        if mensagem and mensagem != "Arquivo validado com sucesso":
            flash(mensagem, "info")
        if total_avisos > 0:
            flash(f"{total_avisos} aviso(s): datas ou valores ajustados automaticamente", "warning")
        if total_erros > 0:
            flash(f"Encontrados {total_erros} erros bloqueantes — importação não realizada", "warning")
        elif not banco_gx:
            from utils.importacao_pessoa_mg_dependencia import layout_depende_pessoa_mg
            if layout_depende_pessoa_mg(layout_dict['NomeLayout'], layout_dict.get('Descricao'), colunas_dict):
                flash(
                    "Selecione um projeto para validar se os CPF/CNPJ existem em Pessoa_MG "
                    "(cadastro 1 Forn_cli.txt deve ser importado antes).",
                    "warning",
                )
            flash("Nenhum erro bloqueante — arquivo apto para migração", "success")
        else:
            flash("Nenhum erro bloqueante — arquivo apto para migração", "success")

        importacao_resultado = None
        if total_erros == 0:
            importacao_resultado = _executar_importacao_automatica(
                layout_dict['NomeLayout'],
                df_processado,
                layout_dict.get('Descricao'),
                colunas_dict,
            )

        template_vars.update(_montar_vars_resultado(
            process_id,
            layout_dict['NomeLayout'],
            layout_id,
            df_processado,
            df_erros,
            df_avisos,
            mensagem,
            layout_dict.get('Descricao'),
            colunas_dict,
            importacao_resultado=importacao_resultado,
        ))

        return render_template('importacao/index.html', **template_vars)
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo com layout: {e}")
        flash(f"Erro ao processar arquivo: {str(e)}", "error")
        return render_template('importacao/index.html', **template_vars)
    finally:
        if conn:
            conn.close()

@importacao_bp.route('/api/detalhes/<process_id>')
def api_detalhes(process_id):
    """Retorna erros ou avisos do processamento em JSON (para modal)."""
    if "usuario" not in session:
        return jsonify({'erro': 'Não autenticado'}), 401

    tipo = (request.args.get('tipo') or 'erros').strip().lower()
    if tipo not in ('erros', 'avisos'):
        return jsonify({'erro': 'Tipo inválido. Use erros ou avisos.'}), 400

    usuario_id = session.get("usuario", {}).get("usuario_id")
    data = carregar_processamento(process_id, usuario_id)
    if not data:
        return jsonify({'erro': 'Processamento não encontrado ou expirado.'}), 404

    if tipo == 'erros':
        df = data.get('df_erros')
        col_msg = 'Erro'
    else:
        df = data.get('df_avisos')
        col_msg = 'Aviso'

    if df is None or df.empty:
        return jsonify({'tipo': tipo, 'total': 0, 'items': []})

    items = []
    for _, row in df.iterrows():
        items.append({
            'Linha': int(row.get('Linha', 0) or 0),
            'Coluna': str(row.get('Coluna', '') or ''),
            'Mensagem': str(row.get(col_msg, '') or ''),
        })

    return jsonify({'tipo': tipo, 'total': len(items), 'items': items})


@importacao_bp.route('/exportar_erros/<process_id>')
def exportar_erros(process_id):
    """Exporta erros e/ou avisos para Excel (tipo=erros|avisos|ambos)."""
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    usuario_id = session.get("usuario", {}).get("usuario_id")
    data = carregar_processamento(process_id, usuario_id)
    if not data:
        flash("Dados de exportação não encontrados ou expirados. Processe o arquivo novamente.", "error")
        return redirect(url_for('importacao.index'))

    df_erros = data['df_erros']
    df_avisos = data.get('df_avisos')
    tipo = (request.args.get('tipo') or 'ambos').strip().lower()

    incluir_erros = tipo in ('erros', 'ambos')
    incluir_avisos = tipo in ('avisos', 'ambos')

    tem_erros = incluir_erros and df_erros is not None and not df_erros.empty
    tem_avisos = incluir_avisos and df_avisos is not None and not df_avisos.empty

    if not tem_erros and not tem_avisos:
        flash("Nenhum dado para exportar.", "info")
        return redirect(url_for('importacao.index', process_id=process_id))

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        if tem_erros:
            df_erros.to_excel(writer, sheet_name='Erros_Validacao', index=False)
        if tem_avisos:
            df_avisos.to_excel(writer, sheet_name='Avisos_Conversao', index=False)

    output.seek(0)

    sufixo = tipo if tipo != 'ambos' else 'erros_avisos'
    filename = f"{sufixo}_{data['layout_nome']}_{data['timestamp'].strftime('%Y%m%d_%H%M%S')}.xlsx"

    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename
    )


@importacao_bp.route('/exportar_processado/<process_id>')
def exportar_processado(process_id):
    """Exporta o arquivo processado para Excel"""
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    usuario_id = session.get("usuario", {}).get("usuario_id")
    data = carregar_processamento(process_id, usuario_id)
    if not data:
        flash("Dados de exportação não encontrados ou expirados. Processe o arquivo novamente.", "error")
        return redirect(url_for('importacao.index'))
    
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


@importacao_bp.route('/importar_base/<process_id>', methods=['POST'])
def importar_base(process_id):
    """Redireciona — importação é automática após validação sem erros bloqueantes."""
    flash("A importação é automática ao processar o arquivo. Envie o arquivo novamente.", "info")
    return redirect(url_for('importacao.index', process_id=process_id))


@importacao_bp.route('/gerar_depara/<process_id>', methods=['POST'])
def gerar_depara(process_id):
    """Redireciona — De/Para é gerado automaticamente na importação."""
    flash("O De/Para é gerado automaticamente na importação. Envie o arquivo novamente.", "info")
    return redirect(url_for('importacao.index', process_id=process_id))


# Importar o layout_bp também aqui para organização
from .layout import layout_bp