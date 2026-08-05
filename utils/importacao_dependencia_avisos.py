"""Avisos informativos na validação de estrutura (sem checagem em banco / arquivos anteriores)."""

from utils.importacao_pessoa_mg_dependencia import layout_depende_pessoa_mg
from utils.importacao_produto_mg_dependencia import layout_depende_produto_mg
from utils.importacao_veiculo_mg_dependencia import layout_depende_veiculo_mg
from utils.importacao_ficha_cab_mg_dependencia import layout_depende_ficha_cab_mg

MSG_CPF_FORN_CLI = (
    'Na migração, cada CPF/CNPJ deste arquivo deve estar cadastrado no arquivo principal '
    '1 Forn_cli.txt.'
)

MSG_PRODUTO_REFERENCIA = (
    'Na migração, cada PRODUTO_REFERENCIA (+ CNPJ_EMPRESA) deste arquivo deve estar '
    'cadastrada em Produto_MG (importe antes o layout 7 Produto).'
)

MSG_CHASSI_VEICULO = (
    'Na migração, cada CHASSI deste arquivo deve estar cadastrado no layout Veiculo '
    '(tabela Veiculo_MG).'
)

MSG_CHASSI_FICHA_CAB = (
    'Na migração, cada CHASSI deste arquivo deve existir em Ficha_Cab_MG '
    '(importe antes o layout 13 Fseg_Cab).'
)


def gerar_avisos_dependencia_estrutura(layout_nome, layout_descricao=None, colunas_layout=None):
    """Retorna avisos (não bloqueantes) sobre dependência de arquivo principal."""
    avisos = []
    if layout_depende_pessoa_mg(layout_nome, layout_descricao, colunas_layout):
        avisos.append({
            'Linha': 1,
            'Coluna': '(dependência)',
            'Aviso': MSG_CPF_FORN_CLI,
        })
    if layout_depende_produto_mg(layout_nome, layout_descricao, colunas_layout):
        avisos.append({
            'Linha': 1,
            'Coluna': '(dependência)',
            'Aviso': MSG_PRODUTO_REFERENCIA,
        })
    if layout_depende_veiculo_mg(layout_nome, layout_descricao, colunas_layout):
        avisos.append({
            'Linha': 1,
            'Coluna': '(dependência)',
            'Aviso': MSG_CHASSI_VEICULO,
        })
    if layout_depende_ficha_cab_mg(layout_nome, layout_descricao, colunas_layout):
        avisos.append({
            'Linha': 1,
            'Coluna': '(dependência)',
            'Aviso': MSG_CHASSI_FICHA_CAB,
        })
    return avisos


def mensagens_alerta_dependencia_estrutura(layout_nome, layout_descricao=None, colunas_layout=None):
    return [a['Aviso'] for a in gerar_avisos_dependencia_estrutura(
        layout_nome, layout_descricao, colunas_layout,
    )]
