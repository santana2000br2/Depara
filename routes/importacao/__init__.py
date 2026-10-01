# No arquivo routes/importacao/__init__.py
import pandas as pd
import io
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, send_file, jsonify, current_app
from werkzeug.exceptions import RequestEntityTooLarge
from db.connection import conectar_banco
from logger import logger

from utils.importacao_store import (
    carregar_processamento,
)
from utils.projeto_acesso import importacao_completa_liberada, acesso_envio_arquivos

importacao_bp = Blueprint('importacao', __name__)

TEMPLATE_IMPORTACAO = 'importacao/index.html'


def _render_pagina(template_vars, somente_validacao=False):
    template_vars['modo_validador'] = somente_validacao
    template_vars['blueprint_nome'] = 'validador_estrutura' if somente_validacao else 'importacao'
    if 'ultimos_envios' not in template_vars:
        template_vars['ultimos_envios'] = []
    try:
        from utils.historico_envio_arquivo import listar_historico_envio
        projeto = session.get('projeto_selecionado') or {}
        if projeto.get('ProjetoID'):
            template_vars['ultimos_envios'] = listar_historico_envio(
                projeto.get('ProjetoID'), limite=5,
            )
    except Exception:
        logger.exception("Erro ao carregar últimos envios para a tela")
        template_vars.setdefault('ultimos_envios', [])
    return render_template(TEMPLATE_IMPORTACAO, **template_vars)


def base_template_vars(**kwargs):
    """Variáveis padrão exigidas pelo base.html."""
    vars_dict = {
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
    from utils.importacao_forn_cli_telefone import layout_eh_forn_cli_telefone
    from utils.importacao_forn_cli_contato import layout_eh_forn_cli_contato
    from utils.importacao_produto_estoque import layout_eh_produto_estoque
    from utils.importacao_prod_locacao import layout_eh_prod_locacao
    from utils.importacao_movimento_estoque import layout_eh_movimento_estoque
    from utils.importacao_produto import layout_eh_produto
    from utils.importacao_veiculo import layout_eh_veiculo
    from utils.importacao_fseg_cab import layout_eh_fseg_cab
    from utils.importacao_fseg_prd import layout_eh_fseg_prd
    from utils.importacao_fseg_srv import layout_eh_fseg_srv
    from utils.importacao_financeiro import layout_eh_financeiro
    from utils.importacao_adiantamento import layout_eh_adiantamento
    from utils.importacao_intercambiavel import layout_eh_intercambiavel

    if layout_eh_movimento_estoque(nome_layout, descricao_layout, colunas_layout):
        return 'movimento_estoque'
    if layout_eh_adiantamento(nome_layout, descricao_layout, colunas_layout):
        return 'adiantamento'
    if layout_eh_intercambiavel(nome_layout, descricao_layout, colunas_layout):
        return 'intercambiavel'
    if layout_eh_financeiro(nome_layout, descricao_layout, colunas_layout):
        return 'financeiro'
    if layout_eh_fseg_srv(nome_layout, descricao_layout, colunas_layout):
        return 'fseg_srv'
    if layout_eh_fseg_prd(nome_layout, descricao_layout, colunas_layout):
        return 'fseg_prd'
    if layout_eh_fseg_cab(nome_layout, descricao_layout, colunas_layout):
        return 'fseg_cab'
    if layout_eh_veiculo(nome_layout, descricao_layout, colunas_layout):
        return 'veiculo'
    if layout_eh_prod_locacao(nome_layout, descricao_layout, colunas_layout):
        return 'prod_locacao'
    if layout_eh_produto_estoque(nome_layout, descricao_layout, colunas_layout):
        return 'produto_estoque'
    if layout_eh_produto(nome_layout, descricao_layout, colunas_layout):
        return 'produto'
    if layout_eh_forn_cli_endereco(nome_layout, descricao_layout, colunas_layout):
        return 'forn_cli_endereco'
    if layout_eh_forn_cli_documento(nome_layout, descricao_layout, colunas_layout):
        return 'forn_cli_documento'
    if layout_eh_forn_cli_enquadramento(nome_layout, descricao_layout, colunas_layout):
        return 'forn_cli_enquadramento'
    if layout_eh_forn_cli_telefone(nome_layout, descricao_layout, colunas_layout):
        return 'forn_cli_telefone'
    if layout_eh_forn_cli_contato(nome_layout, descricao_layout, colunas_layout):
        return 'forn_cli_contato'
    if layout_eh_forn_cli(nome_layout, descricao_layout, colunas_layout):
        return 'forn_cli'
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
    if tipo == 'forn_cli_telefone':
        return {
            'tabela_destino': cfg_proc.get('destino', 'PessoaTelefone_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CPF/CNPJ ausente, não cadastrado em Pessoa_MG ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.PessoaTelefone_MG. '
                'Requer cadastro prévio em Pessoa_MG (layout 1 Forn_cli.txt).'
            ),
        }
    if tipo == 'forn_cli_contato':
        return {
            'tabela_destino': cfg_proc.get('destino', 'PessoaContato_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CPF/CNPJ ausente, não cadastrado em Pessoa_MG ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.PessoaContato_MG. '
                'Requer cadastro prévio em Pessoa_MG (layout 1 Forn_cli.txt).'
            ),
        }
    if tipo == 'produto':
        return {
            'tabela_destino': cfg_proc.get('destino', 'Produto_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'referência, descrição, valores ou colunas faltando no arquivo',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.Produto_MG. '
                'Requer Empresa_DePara (CNPJ_EMPRESA) e BancoHomo configurados.'
            ),
        }
    if tipo == 'produto_estoque':
        return {
            'tabela_destino': cfg_proc.get('destino', 'ProdutoEstoque_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'referência ausente em Produto_MG, estoque ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.ProdutoEstoque_MG. '
                'Requer Produto_MG (layout 7 Produto) importado antes.'
            ),
        }
    if tipo == 'prod_locacao':
        return {
            'tabela_destino': cfg_proc.get('destino', 'ProdLocacao_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'referência ausente em Produto_MG, localização ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.ProdLocacao_MG. '
                'Requer Produto_MG (layout 7 Produto) importado antes.'
            ),
        }
    if tipo == 'movimento_estoque':
        return {
            'tabela_destino': cfg_proc.get('destino', 'MovimentoEstoque_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CPF/CNPJ real ausente em Pessoa_MG ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.MovimentoEstoque_MG. '
                'CPF consumidor (00000000000 ou 99999999999) é aceito; CPF real exige Pessoa_MG (layout 1 Forn_cli).'
            ),
        }
    if tipo == 'veiculo':
        return {
            'tabela_destino': cfg_proc.get('destino', 'Veiculo_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'chassi, CNPJ empresa ou colunas faltando no arquivo',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.Veiculo_MG. '
                'Requer Empresa_DePara (CNPJ_EMPRESA), BancoHomo e De/Para de modelo/cor/ano/UF/município/marca.'
            ),
        }
    if tipo == 'fseg_cab':
        return {
            'tabela_destino': cfg_proc.get('destino', 'Ficha_Cab_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CHASSI/CPF ausente em Veiculo_MG/Pessoa_MG ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.Ficha_Cab_MG. '
                'Requer Pessoa_MG (1 Forn_cli), Veiculo_MG (Veículo) e Empresa_DePara.'
            ),
        }
    if tipo == 'fseg_prd':
        return {
            'tabela_destino': cfg_proc.get('destino', 'Ficha_Prd_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CHASSI ausente em Ficha_Cab_MG, produto ausente em Produto_MG ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.Ficha_Prd_MG. '
                'Requer Ficha_Cab_MG (13 Fseg_Cab), Produto_MG (7 Produto), '
                'Empresa_DePara e BancoHomo (ProdutoMarca/Veiculo).'
            ),
        }
    if tipo == 'fseg_srv':
        return {
            'tabela_destino': cfg_proc.get('destino', 'Ficha_Srv_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CHASSI ausente em Ficha_Cab_MG ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.Ficha_Srv_MG. '
                'Requer Ficha_Cab_MG (13 Fseg_Cab), Empresa_DePara e BancoHomo (Veiculo).'
            ),
        }
    if tipo == 'financeiro':
        return {
            'tabela_destino': cfg_proc.get('destino', 'Titulo_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CPF/CNPJ ausente em Pessoa_MG ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.Titulo_MG. '
                'Requer Pessoa_MG (1 Forn_cli), Empresa_DePara e 6 De/Para '
                '(AgenteCobrador, ContaGerencial, TipoTitulo, Departamento, NaturezaOperacao, Banco).'
            ),
        }
    if tipo == 'adiantamento':
        return {
            'tabela_destino': cfg_proc.get('destino', 'FichaRazao_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'CPF/CNPJ, tipo ficha razão, valor saldo ou colunas faltando',
            'flash_flag': (
                'consulte Ocorrencia em {banco}.dbo.FichaRazao_MG. '
                'Requer Pessoa_MG (1 Forn_cli), Empresa_DePara e De/Para TipoFichaRazao. '
                'Erros de validação não impedem a carga: linhas problemáticas tendem a Flag=0.'
            ),
        }
    if tipo == 'intercambiavel':
        return {
            'tabela_destino': cfg_proc.get('destino', 'ProdutoIntercambiavel_MG'),
            'procedure': proc,
            'motivo_nao_suportado': None,
            'motivo_erros': 'código/referência do produto ou colunas faltando no arquivo',
            'flash_flag': (
                'consulte {banco}.dbo.ProdutoIntercambiavel_MG. '
                'Requer Empresa_DePara (marca) e BancoHomo (Marca).'
            ),
        }
    return {
        'tabela_destino': '',
        'procedure': '',
        'motivo_nao_suportado': 'importação automática indisponível',
        'motivo_erros': 'erros bloqueantes no arquivo',
        'flash_flag': '',
    }


def _status_importacao_base(
    nome_layout, df_processado, descricao_layout=None, colunas_layout=None,
    tem_erros_bloqueantes=False, tem_erro_estrutura=False,
):
    """Indica se a importação automática pode rodar após a validação."""
    tipo = _detectar_tipo_importacao(nome_layout, descricao_layout, colunas_layout)
    cfg = _config_importacao_por_tipo(tipo)
    logger.info(
        "Importação base: layout=%r descricao=%r tipo=%s estrutura=%s",
        nome_layout, descricao_layout, tipo, tem_erro_estrutura,
    )

    if not tipo:
        return {
            'exibe': False,
            'pode': False,
            'motivo': f'Layout "{nome_layout}" não possui importação automática configurada.',
            'tipo': None,
            'tabela_destino': '',
        }

    if tem_erro_estrutura:
        return {
            'exibe': True,
            'pode': False,
            'motivo': (
                'Erro de estrutura: quantidade de posições diferente do layout. '
                'Corrija o arquivo (ou o layout) e envie novamente — a importação fica bloqueada.'
            ),
            'tipo': tipo,
            'tabela_destino': cfg['tabela_destino'],
            'procedure': cfg.get('procedure', ''),
            'erro_estrutura': True,
        }

    # Demais erros de validação não bloqueiam: o arquivo segue após confirmação,
    # linhas problemáticas tendem a Flag=0 e o relatório permanece.
    if tem_erros_bloqueantes:
        logger.info(
            "Importação seguirá com erros de validação (relatório/Flag=0): layout=%r",
            nome_layout,
        )

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
    """Importa o arquivo processado; erros de validação não impedem a carga."""
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
    try:
        from utils.importacao_executar import importar_por_tipo, montar_resultado_importacao
        tupla = importar_por_tipo(tipo, df_processado, banco_gx, banco_wf)
        if not tupla:
            return None
        sucesso, mensagem, resumo = tupla
    except MemoryError:
        logger.exception("Memória esgotada na importação automática (layout=%s)", layout_nome)
        return {
            'sucesso': False,
            'mensagem': (
                "O arquivo é grande demais para importar na memória deste servidor. "
                "Divida-o em partes menores (por exemplo até 50 MB) e envie novamente."
            ),
            'resumo': {},
            'banco_gx': banco_gx,
            'automatica': True,
            'tipo': tipo,
            'tabela_destino': cfg['tabela_destino'],
        }

    resultado = montar_resultado_importacao(tipo, cfg, banco_gx, sucesso, mensagem, resumo)
    if not sucesso:
        flash(mensagem, 'error')
    return resultado


def _montar_vars_resultado(process_id, layout_nome, layout_id, df_processado, df_erros, df_avisos, mensagem, descricao_layout=None, colunas_layout=None, importacao_resultado=None, somente_validacao=False, aguardando_confirmacao_importacao=False, tem_erro_estrutura=None):
    """Monta variáveis de template para exibir resultado do processamento."""
    total_erros = _total_validacao(df_erros)
    total_avisos = _total_validacao(df_avisos)

    from utils.layout_validation import tem_erro_estrutura as _detectar_erro_estrutura
    if tem_erro_estrutura is None:
        tem_erro_estrutura = _detectar_erro_estrutura(df_erros)

    importacao_base = {'exibe': False, 'pode': False, 'motivo': ''}
    if not somente_validacao:
        importacao_base = _status_importacao_base(
            layout_nome, df_processado, descricao_layout, colunas_layout,
            tem_erros_bloqueantes=total_erros > 0,
            tem_erro_estrutura=bool(tem_erro_estrutura),
        )

    alerta_dependencia_arquivos = []
    if somente_validacao:
        from utils.importacao_dependencia_avisos import mensagens_alerta_dependencia_estrutura
        alerta_dependencia_arquivos = mensagens_alerta_dependencia_estrutura(
            layout_nome, descricao_layout, colunas_layout,
        )

    amostra_erros = []
    if (
        df_erros is not None
        and not getattr(df_erros, 'empty', True)
    ):
        for _, row in df_erros.head(8).iterrows():
            amostra_erros.append({
                'Linha': row.get('Linha', ''),
                'Coluna': row.get('Coluna', ''),
                'Erro': row.get('Erro', ''),
            })

    total_linhas = 0
    try:
        if df_processado is not None:
            total_linhas = int(len(df_processado))
    except Exception:
        total_linhas = 0

    return {
        'erros_processados': total_erros > 0,
        'layout': layout_nome,
        'layout_id_selecionado': layout_id,
        'total_erros': total_erros,
        'total_avisos': total_avisos,
        'total_linhas': total_linhas,
        'process_id': process_id,
        'importacao_base': importacao_base,
        'importacao_ultimo_resultado': None if somente_validacao else importacao_resultado,
        'mensagem_validacao': mensagem,
        'alerta_dependencia_arquivos': alerta_dependencia_arquivos,
        'aguardando_confirmacao_importacao': bool(aguardando_confirmacao_importacao),
        'amostra_erros': amostra_erros,
        'amostra_erros_confirmacao': amostra_erros,
    }


def _restaurar_processamento(process_id, somente_validacao=False):
    usuario_id = session.get('usuario', {}).get('usuario_id')
    data = carregar_processamento(process_id, usuario_id, carregar_detalhes=False)
    if not data:
        return None

    df_erros = data.get('df_erros')
    total_erros = int(data.get('total_erros') or _total_validacao(df_erros))
    importacao_resultado = None if somente_validacao else data.get('importacao_resultado')

    vars_resultado = _montar_vars_resultado(
        process_id,
        data.get('layout_nome', ''),
        data.get('layout_id'),
        data.get('df_processado'),
        df_erros,
        data.get('df_avisos'),
        data.get('mensagem', ''),
        data.get('layout_descricao'),
        data.get('colunas_layout'),
        importacao_resultado=importacao_resultado,
        somente_validacao=somente_validacao,
        aguardando_confirmacao_importacao=False,
        tem_erro_estrutura=bool(data.get('tem_erro_estrutura')),
    )
    if data.get('total_erros') is not None:
        vars_resultado['total_erros'] = int(data.get('total_erros') or 0)
    if data.get('total_avisos') is not None:
        vars_resultado['total_avisos'] = int(data.get('total_avisos') or 0)
    if data.get('total_linhas') is not None:
        vars_resultado['total_linhas'] = int(data.get('total_linhas') or 0)
    if data.get('amostra_erros'):
        vars_resultado['amostra_erros'] = data.get('amostra_erros') or []
        vars_resultado['amostra_erros_confirmacao'] = vars_resultado['amostra_erros']
    from utils.importacao_controle import (
        carregar_importacao_por_process_id,
        formatar_duracao,
        duracao_segundos,
    )
    imp = carregar_importacao_por_process_id(process_id, usuario_id=usuario_id)
    if imp:
        ini = (
            imp.get("DataInicioProcessamento")
            or imp.get("DataFimUpload")
            or imp.get("DataInicioUpload")
        )
        segs = duracao_segundos(ini, imp.get("DataFimProcessamento"))
        vars_resultado["tempo_processamento"] = formatar_duracao(segs)
        vars_resultado["tempo_processamento_segundos"] = segs
    aguardando = (
        not somente_validacao
        and not importacao_resultado
        and total_erros > 0
        and not data.get('tem_erro_estrutura')
        and bool((vars_resultado.get('importacao_base') or {}).get('pode'))
    )
    if aguardando:
        vars_resultado['aguardando_confirmacao_importacao'] = True
        vars_resultado['amostra_erros_confirmacao'] = data.get('amostra_erros') or []
    return vars_resultado


def _total_validacao(df):
    if df is None:
        return 0
    total_attr = getattr(df, 'attrs', {}).get('total')
    if total_attr is not None:
        return int(total_attr)
    if getattr(df, 'empty', True):
        return 0
    return int(len(df))

def _mensagem_limite_upload():
    max_bytes = current_app.config.get('MAX_CONTENT_LENGTH', 0) or 0
    if max_bytes:
        max_mb = round(max_bytes / (1024 * 1024), 1)
        return (
            f"Arquivo excede o limite de upload ({max_mb} MB). "
            "O arquivo precisa ter no máximo 1 GB."
        )
    return (
        "Arquivo excede o limite de upload permitido. "
        "Divida o arquivo em partes menores."
    )


@importacao_bp.route('/', methods=['GET', 'POST'])
def index():
    return pagina_importacao_arquivo(somente_validacao=False)


def _bloqueio_acesso_envio_arquivos(somente_validacao=False):
    """Retorna redirect/response se o usuário não pode acessar envio de arquivos."""
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    projeto = session.get('projeto_selecionado')
    if not projeto:
        flash('Selecione um projeto para acessar o envio de arquivos.', 'warning')
        return redirect(url_for('auth.trocar_projeto'))

    if not acesso_envio_arquivos(projeto):
        flash(
            'O envio de arquivos está disponível apenas para projetos do tipo Arquivo X Workflow.',
            'warning',
        )
        return redirect(url_for('dashboard.dashboard'))

    if not somente_validacao and not importacao_completa_liberada(projeto):
        flash(
            'A importação completa ainda não está liberada para este projeto. '
            'Use o Validador de Estrutura para conferir o arquivo. '
            'Após aprovação, o administrador deve marcar "Importação Liberada" no cadastro do projeto.',
            'warning',
        )
        return redirect(url_for('validador_estrutura.index'))

    return None


def pagina_importacao_arquivo(somente_validacao=False):
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=somente_validacao)
    if bloqueio:
        return bloqueio

    template_vars = base_template_vars(layouts=[])
    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return _render_pagina(template_vars, somente_validacao)

    try:
        cursor = conn.cursor()
        from utils.layout_escopo import garantir_colunas_layout_escopo
        from utils.importacao_forn_cli_telefone import (
            layout_eh_forn_cli_telefone,
            INSTRUCAO_TELEFONE,
        )
        from routes.dashboard import obter_escopos_projeto

        garantir_colunas_layout_escopo(cursor, conn)

        projeto = session.get("projeto_selecionado") or {}
        escopos_projeto = [
            str(e).strip().upper()
            for e in (obter_escopos_projeto(projeto.get("ProjetoID")) or [])
            if str(e).strip()
        ]

        if escopos_projeto:
            placeholders = ",".join("?" for _ in escopos_projeto)
            cursor.execute(
                f"""
                SELECT LayoutID, NomeLayout, Descricao, DataCriacao, UsuarioCriacao, TipoEscopo
                FROM Layouts
                WHERE TipoEscopo IN ({placeholders})
                ORDER BY
                    TRY_CAST(
                        LEFT(
                            LTRIM(NomeLayout),
                            PATINDEX('%[^0-9]%', LTRIM(NomeLayout) + 'a') - 1
                        ) AS INT
                    ),
                    LayoutID
                """,
                tuple(escopos_projeto),
            )
        else:
            # Projeto sem escopo: não lista layouts (evita mostrar tudo)
            cursor.execute(
                """
                SELECT LayoutID, NomeLayout, Descricao, DataCriacao, UsuarioCriacao, TipoEscopo
                FROM Layouts
                WHERE 1 = 0
                """
            )

        layouts = cursor.fetchall()

        layouts_dict = []
        for layout in layouts:
            item = row_to_dict(layout)
            item['eh_telefone'] = layout_eh_forn_cli_telefone(
                item.get('NomeLayout'), item.get('Descricao'),
            )
            layouts_dict.append(item)

        template_vars = base_template_vars(
            layouts=layouts_dict,
            instrucao_telefone=INSTRUCAO_TELEFONE,
            escopos_projeto_layouts=escopos_projeto,
        )
        if not layouts_dict:
            flash(
                "Nenhum layout disponível para os escopos deste projeto. "
                "Cadastre layouts vinculados aos escopos do projeto ou ajuste o escopo em Gerenciar Escopos.",
                "warning",
            )

    except Exception as e:
        logger.exception("Erro ao buscar layouts")
        flash(f"Erro ao carregar layouts: {e}", "error")
        return _render_pagina(template_vars, somente_validacao)
    finally:
        if conn:
            conn.close()

    if request.method == 'POST':
        try:
            content_length = request.content_length
            logger.info(
                "%s POST recebida (antes de ler o corpo): content_length=%s bytes",
                'Validação' if somente_validacao else 'Importação',
                content_length,
            )
            arquivo = request.files.get('arquivo')
            nome_arquivo = arquivo.filename if arquivo else None
            logger.info(
                "%s POST arquivo disponível: arquivo=%s, layout_id=%s",
                'Validação' if somente_validacao else 'Importação',
                nome_arquivo,
                request.form.get('layout_id'),
            )
            return processar_arquivo_com_layout(
                request, template_vars, somente_validacao=somente_validacao,
            )
        except RequestEntityTooLarge:
            logger.error(
                "Upload rejeitado (413): content_length=%s, limite=%s",
                request.content_length,
                current_app.config.get('MAX_CONTENT_LENGTH'),
            )
            flash(_mensagem_limite_upload(), "error")
            return _render_pagina(template_vars, somente_validacao)
        except MemoryError:
            logger.exception("Memória esgotada ao receber/processar arquivo")
            flash(
                "O arquivo é grande demais para validar neste servidor (falta de memória). "
                "Divida-o em partes menores (por exemplo até 50 MB) e envie novamente.",
                "error",
            )
            return _render_pagina(template_vars, somente_validacao)
        except Exception as e:
            logger.exception("Erro ao processar arquivo")
            flash(f"Erro ao processar arquivo: {e}", "error")
            return _render_pagina(template_vars, somente_validacao)

    process_id = request.args.get('process_id')
    if process_id:
        try:
            vars_resultado = _restaurar_processamento(process_id, somente_validacao=somente_validacao)
            if vars_resultado:
                template_vars.update(vars_resultado)
            else:
                flash(
                    'Resultado do processamento expirado ou não encontrado. Processe o arquivo novamente.',
                    'warning',
                )
        except Exception as e:
            logger.exception("Erro ao restaurar processamento %s", process_id)
            flash(f"Erro ao exibir resultado do processamento: {e}", "error")

    job_id = request.args.get('job_id')
    if job_id:
        template_vars['job_id'] = job_id
        template_vars['job_api_url'] = url_for(
            'validador_estrutura.api_job' if somente_validacao else 'importacao.api_job',
            job_id=job_id,
        )
    importacao_id = request.args.get('importacao_id')
    if importacao_id:
        template_vars['importacao_id'] = importacao_id
        template_vars['importacao_status_url'] = url_for(
            'validador_estrutura.api_importacao_status' if somente_validacao else 'importacao.api_importacao_status',
            importacao_id=importacao_id,
        )

    return _render_pagina(template_vars, somente_validacao)

def processar_arquivo_com_layout(request, template_vars, somente_validacao=False):
    """Processa o arquivo com o layout selecionado (validação e, opcionalmente, importação)."""

    def _render():
        return _render_pagina(template_vars, somente_validacao)

    try:
        if "arquivo" not in request.files:
            flash("Nenhum arquivo selecionado.", "error")
            return _render()

        arquivo = request.files["arquivo"]
    except RequestEntityTooLarge:
        flash(_mensagem_limite_upload(), "error")
        return _render()

    layout_id = request.form.get("layout_id")

    if arquivo.filename == "":
        flash("Nenhum arquivo selecionado.", "error")
        return _render()

    if not layout_id:
        flash("Selecione um layout para validar o arquivo.", "error")
        return _render()

    conn = conectar_banco()
    if not conn:
        flash("Erro ao conectar com o banco de dados", "error")
        return _render()

    try:
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM Layouts WHERE LayoutID = ?", (layout_id,))
        layout = cursor.fetchone()

        if not layout:
            flash("Layout selecionado não encontrado", "error")
            return _render()

        layout_dict = row_to_dict(layout)

        from routes.dashboard import obter_escopos_projeto
        from utils.layout_escopo import garantir_colunas_layout_escopo

        garantir_colunas_layout_escopo(cursor, conn)
        projeto = session.get('projeto_selecionado') or {}
        escopos_projeto = {
            str(e).strip().upper()
            for e in (obter_escopos_projeto(projeto.get("ProjetoID")) or [])
            if str(e).strip()
        }
        tipo_layout_escopo = (layout_dict.get("TipoEscopo") or "").strip().upper()
        if not escopos_projeto or tipo_layout_escopo not in escopos_projeto:
            flash(
                "Este layout não pertence aos escopos do projeto selecionado. "
                "Escolha um layout vinculado ao escopo do projeto.",
                "error",
            )
            return _render()

        cursor.execute("SELECT * FROM LayoutColunas WHERE LayoutID = ? ORDER BY Posicao", (layout_id,))
        colunas = cursor.fetchall()
        colunas_dict = [row_to_dict(coluna) for coluna in colunas]

        if not colunas_dict:
            flash("O layout selecionado não possui colunas configuradas. Edite o layout antes de importar.", "error")
            return _render()

        from utils.validacao_job import iniciar_job

        # Sempre fora do FastCGI/IIS: validar+importar no POST estoura timeout/RAM
        # (HTTP 500) em Produto, Forn_cli, Enquadramento e demais layouts.
        usuario_sess = session.get("usuario") or {}
        try:
            job_id = iniciar_job(
                arquivo=arquivo,
                usuario_id=usuario_sess.get("usuario_id"),
                layout_id=layout_id,
                somente_validacao=somente_validacao,
                projeto=session.get("projeto_selecionado") or {},
                filename=arquivo.filename,
                usuario_nome=usuario_sess.get("usuario"),
            )
        except Exception:
            logger.exception("Falha ao iniciar validação em segundo plano")
            flash(
                "Não foi possível iniciar a validação em segundo plano. Tente novamente.",
                "error",
            )
            return _render()
        logger.info(
            "Validação em segundo plano: job=%s layout=%s arquivo=%s",
            job_id, layout_dict.get("NomeLayout"), getattr(arquivo, "filename", None),
        )
        flash(
            "A validação continua no servidor. "
            "Não feche esta tela — ela atualiza sozinha.",
            "info",
        )
        endpoint = "validador_estrutura.index" if somente_validacao else "importacao.index"
        return redirect(url_for(endpoint, job_id=job_id))

    except RequestEntityTooLarge:
        flash(_mensagem_limite_upload(), "error")
        return _render()
    except MemoryError:
        logger.exception("Memória esgotada ao processar arquivo")
        flash(
            "O arquivo é grande demais para validar neste servidor (falta de memória). "
            "Divida-o em partes menores (por exemplo até 50 MB) e envie novamente.",
            "error",
        )
        return _render()
    except Exception as e:
        logger.exception("Erro ao processar arquivo com layout")
        flash(f"Erro ao processar arquivo: {str(e)}", "error")
        return _render()
    finally:
        if conn:
            conn.close()


def api_detalhes_processamento(process_id):
    """Retorna erros ou avisos do processamento em JSON (para modal).

    A visualização traz no máximo 100 itens; o total completo vem em `total`
    para o Excel exportar tudo via exportar_erros.
    """
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=True)
    if bloqueio:
        if request.accept_mimetypes.accept_json:
            return jsonify({'erro': 'Acesso negado'}), 403
        return bloqueio

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
        return jsonify({
            'tipo': tipo,
            'total': 0,
            'exibidas': 0,
            'limitado': False,
            'items': [],
        })

    total = len(df)
    limite = 100
    df_view = df.head(limite)

    items = []
    for _, row in df_view.iterrows():
        items.append({
            'Linha': int(row.get('Linha', 0) or 0),
            'Coluna': str(row.get('Coluna', '') or ''),
            'Mensagem': str(row.get(col_msg, '') or ''),
        })

    return jsonify({
        'tipo': tipo,
        'total': total,
        'exibidas': len(items),
        'limitado': total > limite,
        'items': items,
    })


def api_preview_top100_processamento(process_id):
    """Retorna as primeiras 100 linhas do arquivo processado para visualização."""
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=True)
    if bloqueio:
        return jsonify({'erro': 'Acesso negado'}), 403

    usuario_id = session.get("usuario", {}).get("usuario_id")
    data = carregar_processamento(process_id, usuario_id)
    if not data:
        return jsonify({'erro': 'Processamento não encontrado ou expirado.'}), 404

    df = data.get('df_processado')
    if df is None or df.empty:
        return jsonify({
            'colunas': [],
            'linhas': [],
            'total_linhas': 0,
            'exibidas': 0,
        })

    total_linhas = len(df)
    preview = df.head(100)
    colunas = [str(c) for c in preview.columns]
    linhas = []
    for i, (_, row) in enumerate(preview.iterrows(), start=1):
        item = {'#': i}
        for col in preview.columns:
            val = row[col]
            item[str(col)] = '' if pd.isna(val) else str(val)
        linhas.append(item)

    return jsonify({
        'colunas': ['#'] + colunas,
        'linhas': linhas,
        'total_linhas': total_linhas,
        'exibidas': len(linhas),
    })


def exportar_erros_processamento(process_id, redirect_endpoint='importacao.index'):
    """Exporta erros e/ou avisos para Excel (tipo=erros|avisos|ambos)."""
    somente_validacao = redirect_endpoint == 'validador_estrutura.index'
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=somente_validacao)
    if bloqueio:
        return bloqueio

    usuario_id = session.get("usuario", {}).get("usuario_id")
    data = carregar_processamento(process_id, usuario_id)
    if not data:
        flash("Dados de exportação não encontrados ou expirados. Processe o arquivo novamente.", "error")
        return redirect(url_for(redirect_endpoint))

    df_erros = data['df_erros']
    df_avisos = data.get('df_avisos')
    tipo = (request.args.get('tipo') or 'ambos').strip().lower()

    incluir_erros = tipo in ('erros', 'ambos')
    incluir_avisos = tipo in ('avisos', 'ambos')

    tem_erros = incluir_erros and df_erros is not None and not df_erros.empty
    tem_avisos = incluir_avisos and df_avisos is not None and not df_avisos.empty

    if not tem_erros and not tem_avisos:
        flash("Nenhum dado para exportar.", "info")
        return redirect(url_for(redirect_endpoint, process_id=process_id))

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


def exportar_processado_processamento(process_id, redirect_endpoint='importacao.index'):
    """Exporta o arquivo processado para Excel."""
    somente_validacao = redirect_endpoint == 'validador_estrutura.index'
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=somente_validacao)
    if bloqueio:
        return bloqueio

    usuario_id = session.get("usuario", {}).get("usuario_id")
    data = carregar_processamento(process_id, usuario_id)
    if not data:
        flash("Dados de exportação não encontrados ou expirados. Processe o arquivo novamente.", "error")
        return redirect(url_for(redirect_endpoint))

    df_processado = data['df_processado']

    if df_processado is None or df_processado.empty:
        flash("Nenhum dado processado para exportar.", "info")
        return redirect(url_for(redirect_endpoint))

    from utils.arquivo_processado import eh_arquivo_processado
    from pathlib import Path

    stamp = data['timestamp'].strftime('%Y%m%d_%H%M%S')
    layout = data['layout_nome']

    # Excel de arquivo grande estoura RAM (e o limite de ~1M linhas). Exporta CSV em disco.
    if eh_arquivo_processado(df_processado) or len(df_processado) > 50000:
        filename = f"dados_processados_{layout}_{stamp}.csv"
        if eh_arquivo_processado(df_processado):
            dest = Path(df_processado.caminho).with_name("dados_processados_excel.csv")
            df_processado.exportar_csv_excel(dest)
        else:
            dest = Path(current_app.root_path) / "temp" / "importacao_exports" / process_id / "dados_processados_excel.csv"
            dest.parent.mkdir(parents=True, exist_ok=True)
            df_processado.to_csv(dest, sep=";", index=False, encoding="utf-8-sig")
        return send_file(
            dest,
            mimetype="text/csv; charset=utf-8",
            as_attachment=True,
            download_name=filename,
        )

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_processado.to_excel(writer, sheet_name='Dados_Processados', index=False)

    output.seek(0)

    filename = f"dados_processados_{layout}_{stamp}.xlsx"

    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename
    )


def _contexto_flag0(process_id):
    """Retorna ((data, banco_gx, tabela, flag_0), None) ou (None, response_erro)."""
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=False)
    if bloqueio:
        return None, bloqueio

    usuario_id = session.get("usuario", {}).get("usuario_id")
    data = carregar_processamento(process_id, usuario_id)
    if not data:
        return None, (jsonify({'erro': 'Processamento não encontrado ou expirado.'}), 404)

    imp = data.get('importacao_resultado') or {}
    if not imp.get('sucesso'):
        return None, (jsonify({'erro': 'Nenhuma importação concluída neste processamento.'}), 400)

    banco_gx = (imp.get('banco_gx') or '').strip()
    tabela = (imp.get('tabela_destino') or '').strip()
    if not banco_gx or not tabela:
        return None, (jsonify({'erro': 'Banco/tabela da importação não disponíveis.'}), 400)

    return (data, banco_gx, tabela, int(imp.get('flag_0') or 0)), None


def api_motivos_flag0_processamento(process_id):
    """Motivos (Ocorrencia) agrupados dos registros Flag=0."""
    ctx, err = _contexto_flag0(process_id)
    if err:
        return err

    _, banco_gx, tabela, flag_0 = ctx
    from db.connection import conectar_segunda_base
    from utils.importacao_forn_cli import obter_motivos_flag0, _validar_identificador_sql

    try:
        banco_gx = _validar_identificador_sql(banco_gx)
        tabela = _validar_identificador_sql(tabela)
    except ValueError as exc:
        return jsonify({'erro': str(exc)}), 400

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return jsonify({'erro': f'Não foi possível conectar ao banco {banco_gx}.'}), 500

    cursor = conn.cursor()
    try:
        motivos = obter_motivos_flag0(cursor, banco_gx, tabela)
        return jsonify({
            'banco_gx': banco_gx,
            'tabela': tabela,
            'flag_0': flag_0,
            'total_motivos': len(motivos),
            'motivos': motivos,
        })
    except Exception as exc:
        logger.exception("Erro ao listar motivos Flag=0")
        return jsonify({'erro': f'Erro ao consultar ocorrências: {exc}'}), 500
    finally:
        cursor.close()
        conn.close()


def exportar_flag0_processamento(process_id):
    """Exporta todos os registros Flag=0 (chave + ocorrência) para Excel."""
    ctx, err = _contexto_flag0(process_id)
    if err:
        if isinstance(err, tuple):
            flash((err[0].get_json() or {}).get('erro', 'Erro ao exportar rejeitados'), 'error')
            return redirect(url_for('importacao.index'))
        return err

    data, banco_gx, tabela, _flag_0 = ctx
    from db.connection import conectar_segunda_base
    from utils.importacao_forn_cli import obter_detalhe_flag0, _validar_identificador_sql

    try:
        banco_gx = _validar_identificador_sql(banco_gx)
        tabela = _validar_identificador_sql(tabela)
    except ValueError as exc:
        flash(str(exc), 'error')
        return redirect(url_for('importacao.index'))

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        flash(f'Não foi possível conectar ao banco {banco_gx}.', 'error')
        return redirect(url_for('importacao.index'))

    cursor = conn.cursor()
    try:
        colunas, rows = obter_detalhe_flag0(cursor, banco_gx, tabela)
        df = pd.DataFrame(rows, columns=colunas) if rows else pd.DataFrame(columns=colunas or ['Ocorrencia'])

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Rejeitados', index=False)
        output.seek(0)

        layout = data.get('layout_nome') or 'layout'
        filename = f"rejeitados_{tabela}_{layout}.xlsx"
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=filename,
        )
    except Exception as exc:
        logger.exception("Erro ao exportar rejeitados")
        flash(f'Erro ao exportar rejeitados: {exc}', 'error')
        return redirect(url_for('importacao.index'))
    finally:
        cursor.close()
        conn.close()


def _json_erro_upload(exc, codigo=400):
    return jsonify({"ok": False, "erro": str(exc)}), codigo


@importacao_bp.route('/api/upload/iniciar', methods=['POST'])
def api_upload_iniciar():
    """Abre sessão de envio em partes (evita HTTP 413 no IIS)."""
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=True)
    if bloqueio:
        return jsonify({"ok": False, "erro": "Acesso negado"}), 403
    usuario_id = session.get("usuario", {}).get("usuario_id")
    dados = request.get_json(silent=True) or {}
    from utils.upload_chunk import UploadChunkError, iniciar_sessao
    try:
        meta = iniciar_sessao(
            usuario_id,
            dados.get("filename") or "arquivo.txt",
            dados.get("tamanho") or 0,
        )
    except UploadChunkError as exc:
        return _json_erro_upload(exc)
    return jsonify({"ok": True, **meta})


@importacao_bp.route('/api/upload/chunk', methods=['POST'])
def api_upload_chunk():
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=True)
    if bloqueio:
        return jsonify({"ok": False, "erro": "Acesso negado"}), 403
    usuario_id = session.get("usuario", {}).get("usuario_id")
    parte = request.files.get("chunk")
    if not parte:
        return _json_erro_upload("Parte do arquivo não recebida.")
    from utils.upload_chunk import UploadChunkError, gravar_chunk
    try:
        info = gravar_chunk(
            request.form.get("upload_id"),
            usuario_id,
            request.form.get("indice"),
            parte.stream,
        )
    except UploadChunkError as exc:
        return _json_erro_upload(exc)
    return jsonify({"ok": True, **info})


@importacao_bp.route('/api/upload/finalizar', methods=['POST'])
def api_upload_finalizar():
    bloqueio = _bloqueio_acesso_envio_arquivos(
        somente_validacao=(request.blueprint == "validador_estrutura"),
    )
    if bloqueio:
        return jsonify({"ok": False, "erro": "Acesso negado"}), 403

    usuario = session.get("usuario") or {}
    usuario_id = usuario.get("usuario_id")
    dados = request.get_json(silent=True) or {}
    layout_id = dados.get("layout_id")
    upload_id = dados.get("upload_id")
    if not layout_id:
        return _json_erro_upload("Selecione um layout.")
    if not upload_id:
        return _json_erro_upload("Sessão de envio inválida.")

    somente_validacao = request.blueprint == "validador_estrutura"
    from utils.upload_chunk import (
        UploadChunkError,
        limpar_sessao,
        montar_arquivo,
        nome_arquivo,
    )
    from utils.validacao_job import iniciar_job_de_caminho
    from utils.arquivo_projeto import salvar_arquivo_importacao
    from utils.importacao_controle import (
        STATUS_AGUARDANDO,
        criar_importacao,
    )

    projeto = session.get("projeto_selecionado") or {}
    nome_layout = ""
    conn_layout = conectar_banco()
    if conn_layout:
        try:
            cur = conn_layout.cursor()
            cur.execute("SELECT NomeLayout FROM Layouts WHERE LayoutID = ?", (layout_id,))
            row = cur.fetchone()
            if row:
                nome_layout = row[0] or ""
        except Exception:
            logger.exception("Falha ao obter nome do layout %s", layout_id)
        finally:
            conn_layout.close()

    job_id = None
    importacao_id = None
    nome_fisico = None
    try:
        logger.info("Upload iniciado: layout=%s arquivo sessão=%s", layout_id, upload_id)
        caminho = montar_arquivo(upload_id, usuario_id)
        filename = nome_arquivo(upload_id, usuario_id)
        logger.info("Arquivo recebido e montado: %s", filename)
        salvo = salvar_arquivo_importacao(
            caminho,
            filename,
            projeto.get("NomeProjeto") or "",
            projeto.get("ProjetoID"),
        )
        nome_fisico = salvo["nome_fisico"]
        logger.info("Arquivo salvo em %s", salvo["caminho"])
        try:
            importacao_id = criar_importacao(
                projeto.get("ProjetoID"),
                projeto.get("NomeProjeto") or "",
                layout_id,
                nome_layout,
                salvo["nome_original"],
                salvo["nome_fisico"],
                salvo["caminho"],
                usuario_id=usuario_id,
                usuario_nome=usuario.get("usuario"),
                somente_validacao=somente_validacao,
                status=STATUS_AGUARDANDO,
                mensagem="Arquivo recebido. Aguardando processamento.",
            )
            logger.info("Importação criada: id=%s", importacao_id)
        except Exception:
            logger.exception("Arquivo salvo, mas falhou o registro de controle — a validação segue")
            importacao_id = None
        job_id = iniciar_job_de_caminho(
            salvo["caminho"],
            usuario_id=usuario_id,
            layout_id=layout_id,
            somente_validacao=somente_validacao,
            projeto=projeto,
            filename=filename,
            usuario_nome=usuario.get("usuario"),
            importacao_id=importacao_id,
            arquivo_persistente=salvo["caminho"],
        )
        if importacao_id:
            from utils.importacao_controle import atualizar_importacao
            atualizar_importacao(importacao_id, JobId=job_id)
    except UploadChunkError as exc:
        return _json_erro_upload(exc)
    except Exception as exc:
        logger.exception("Falha ao finalizar upload em partes")
        if importacao_id:
            try:
                from utils.importacao_controle import marcar_erro
                marcar_erro(importacao_id, str(exc))
            except Exception:
                pass
        return _json_erro_upload(f"Não foi possível iniciar a validação: {exc}")
    finally:
        limpar_sessao(upload_id)

    endpoint = "validador_estrutura.index" if somente_validacao else "importacao.index"
    return jsonify({
        "ok": True,
        "sucesso": True,
        "importacao_id": importacao_id,
        "status": STATUS_AGUARDANDO,
        "arquivo": nome_fisico,
        "job_id": job_id,
        "redirect": url_for(endpoint, job_id=job_id, importacao_id=importacao_id),
    })


@importacao_bp.route('/api/job/<job_id>')
def api_job(job_id):
    return api_status_job(job_id)


@importacao_bp.route('/api/importacoes/<int:importacao_id>/status')
def api_importacao_status(importacao_id):
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=True)
    if bloqueio:
        return jsonify({"ok": False, "erro": "Acesso negado"}), 403
    usuario_id = session.get("usuario", {}).get("usuario_id")
    projeto_id = (session.get("projeto_selecionado") or {}).get("ProjetoID")
    from utils.importacao_controle import (
        carregar_importacao,
        marcar_conclusao,
        marcar_erro,
        serializar_status,
    )
    data = carregar_importacao(importacao_id, usuario_id=usuario_id, projeto_id=projeto_id)
    if not data:
        return jsonify({"ok": False, "erro": "Importação não encontrada."}), 404
    payload = serializar_status(data)
    payload["ok"] = True
    status_up = (payload.get("status") or "").upper()
    job_id = payload.get("job_id") or data.get("JobId")
    if job_id and status_up in ("PROCESSANDO", "AGUARDANDO_PROCESSAMENTO", "RECEBENDO"):
        from utils.validacao_job import carregar_status
        job = carregar_status(job_id, usuario_id) or {}
        state = (job.get("state") or "").lower()
        if state == "done" and job.get("process_id"):
            marcar_conclusao(
                importacao_id,
                com_erros=int(payload.get("registros_com_erro") or 0) > 0,
                Mensagem=job.get("mensagem") or payload.get("mensagem") or "Arquivo validado.",
                ProcessId=job.get("process_id"),
                Percentual=100,
            )
            data = carregar_importacao(importacao_id, usuario_id=usuario_id, projeto_id=projeto_id) or data
            payload = serializar_status(data)
            payload["ok"] = True
            if not payload.get("process_id"):
                payload["process_id"] = job.get("process_id")
            if not payload.get("percentual"):
                payload["percentual"] = 100
        elif state == "error":
            marcar_erro(importacao_id, job.get("mensagem") or "Falha na validação.")
            data = carregar_importacao(importacao_id, usuario_id=usuario_id, projeto_id=projeto_id) or data
            payload = serializar_status(data)
            payload["ok"] = True
    return jsonify(payload)


def api_status_job(job_id):
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=True)
    if bloqueio:
        return jsonify({'erro': 'Acesso negado'}), 403
    usuario_id = session.get("usuario", {}).get("usuario_id")
    from utils.validacao_job import carregar_status
    status = carregar_status(job_id, usuario_id)
    if not status:
        return jsonify({'erro': 'Processamento não encontrado.'}), 404
    return jsonify(status)


@importacao_bp.route('/api/detalhes/<process_id>')
def api_detalhes(process_id):
    return api_detalhes_processamento(process_id)


@importacao_bp.route('/api/preview/<process_id>')
def api_preview(process_id):
    return api_preview_top100_processamento(process_id)


@importacao_bp.route('/api/motivos-flag0/<process_id>')
def api_motivos_flag0(process_id):
    return api_motivos_flag0_processamento(process_id)


@importacao_bp.route('/exportar_flag0/<process_id>')
def exportar_flag0(process_id):
    return exportar_flag0_processamento(process_id)


@importacao_bp.route('/exportar_erros/<process_id>')
def exportar_erros(process_id):
    return exportar_erros_processamento(process_id)


@importacao_bp.route('/exportar_processado/<process_id>')
def exportar_processado(process_id):
    return exportar_processado_processamento(process_id)


@importacao_bp.route('/confirmar_importacao/<process_id>', methods=['POST'])
def confirmar_importacao(process_id):
    """Importa um processamento já validado após o usuário confirmar apesar dos erros."""
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=False)
    if bloqueio:
        return bloqueio

    usuario = session.get("usuario") or {}
    usuario_id = usuario.get("usuario_id")
    try:
        data = carregar_processamento(process_id, usuario_id, carregar_detalhes=False)
    except MemoryError:
        logger.exception("Memória esgotada ao carregar resultado para importação %s", process_id)
        flash(
            "O arquivo é grande demais para importar na memória deste servidor. "
            "Divida-o em partes menores (por exemplo até 50 MB) e envie novamente.",
            "error",
        )
        return redirect(url_for("importacao.index"))
    if not data:
        flash("Resultado do processamento expirado ou não encontrado. Processe o arquivo novamente.", "warning")
        return redirect(url_for("importacao.index"))

    if data.get("importacao_resultado"):
        flash("Este arquivo já foi importado.", "info")
        return redirect(url_for("importacao.index", process_id=process_id))

    from utils.layout_validation import tem_erro_estrutura as _detectar_erro_estrutura
    if data.get("tem_erro_estrutura") or _detectar_erro_estrutura(
        data.get("df_erros"), data.get("amostra_erros"),
    ):
        flash(
            "Erro de estrutura: a importação está bloqueada. "
            "Corrija a quantidade de posições e envie o arquivo novamente.",
            "error",
        )
        return redirect(url_for("importacao.index", process_id=process_id))

    from utils.validacao_job import iniciar_importacao_job
    try:
        job_id = iniciar_importacao_job(
            process_id,
            usuario_id,
            session.get("projeto_selecionado") or {},
            usuario_nome=usuario.get("usuario"),
        )
    except Exception:
        logger.exception("Falha ao iniciar importação em segundo plano")
        flash(
            "Não foi possível iniciar a importação em segundo plano. Tente novamente.",
            "error",
        )
        return redirect(url_for("importacao.index", process_id=process_id))
    flash(
        "A importação continua no servidor. "
        "Não feche esta tela — ela atualiza sozinha.",
        "info",
    )
    return redirect(url_for("importacao.index", job_id=job_id))


@importacao_bp.route('/importar_base/<process_id>', methods=['POST'])
def importar_base(process_id):
    """Compat: redireciona para a confirmação de importação."""
    return confirmar_importacao(process_id)


@importacao_bp.route('/gerar_depara/<process_id>', methods=['POST'])
def gerar_depara(process_id):
    """Redireciona — De/Para é gerado automaticamente na importação."""
    bloqueio = _bloqueio_acesso_envio_arquivos(somente_validacao=False)
    if bloqueio:
        return bloqueio
    flash("O De/Para é gerado automaticamente na importação. Envie o arquivo novamente.", "info")
    return redirect(url_for('importacao.index', process_id=process_id))


# Importar o layout_bp também aqui para organização
from .layout import layout_bp