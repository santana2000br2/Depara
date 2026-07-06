"""
Validação de dependência: layouts Forn_cli secundários exigem CPF/CNPJ em Pessoa_MG.

O arquivo 1 Forn_cli.txt deve ser importado primeiro (gera Pessoa_MG).
"""
import re

from logger import logger
from utils.importacao_forn_cli import (
    TABELA_DESTINO as TABELA_PESSOA_MG,
    _executar,
    _normalizar_nome_layout,
    _quote_col,
    _resolver_coluna,
    _tabela_existe,
    _validar_identificador_sql,
    layout_eh_forn_cli,
)
from utils.importacao_forn_cli_documento import layout_eh_forn_cli_documento
from utils.importacao_forn_cli_enquadramento import layout_eh_forn_cli_enquadramento
from utils.importacao_procedures import CONFIG_PROCEDURES
from utils.layout_validation import (
    _ordenar_colunas_layout,
    cpf_cnpj_eh_placeholder,
    normalizar_texto_campo,
)

LAYOUTS_DEPENDEM_PESSOA_MG = frozenset(
    k for k in CONFIG_PROCEDURES if k != 'forn_cli'
)

# Sufixo no nome do layout → tipo (forn_cli_*)
_SUFIXOS_LAYOUT = {
    'documento': 'forn_cli_documento',
    'endereco': 'forn_cli_endereco',
    'enquadramento': 'forn_cli_enquadramento',
    'telefone': 'forn_cli_telefone',
    'contato': 'forn_cli_contato',
    'conjuge': 'forn_cli_conjuge',
    'dadosbancarios': 'forn_cli_dados_bancarios',
    'dados_bancarios': 'forn_cli_dados_bancarios',
}


def detectar_tipo_layout(nome_layout, descricao=None, colunas=None):
    """Identifica o tipo de layout Forn_cli (principal ou secundário)."""
    if layout_eh_forn_cli(nome_layout, descricao, colunas):
        return 'forn_cli'
    if layout_eh_forn_cli_documento(nome_layout, descricao, colunas):
        return 'forn_cli_documento'
    if layout_eh_forn_cli_enquadramento(nome_layout, descricao, colunas):
        return 'forn_cli_enquadramento'

    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome or nome == 'forn_cli':
            continue
        for sufixo, tipo in _SUFIXOS_LAYOUT.items():
            chave = sufixo.replace('_', '')
            if sufixo in nome or f'forn_cli_{sufixo}' in nome or chave in nome.replace('_', ''):
                return tipo

    if colunas:
        nomes = {
            str(c.get('Descricao') or c).strip().upper()
            for c in colunas
            if (c.get('Descricao') if isinstance(c, dict) else c)
        }
        if 'CPF_CNPJ' in nomes and 'CODIGO_PESSOA' not in nomes and 'NOME' not in nomes:
            if 'INSC_ESTADUAL' in nomes:
                return 'forn_cli_documento'
            if 'TIPO_ENDERECO' in nomes or 'COD_IBGE' in nomes:
                return 'forn_cli_endereco'
            if 'CONTRIBUICAO_ICMS' in nomes:
                return 'forn_cli_enquadramento'
            if 'DDD_FONE1' in nomes or 'NUMERO_FONE1' in nomes:
                return 'forn_cli_telefone'
            if 'NOME_CONTATO' in nomes:
                return 'forn_cli_contato'

    return None


def layout_depende_pessoa_mg(nome_layout, descricao=None, colunas=None):
    tipo = detectar_tipo_layout(nome_layout, descricao, colunas)
    return tipo in LAYOUTS_DEPENDEM_PESSOA_MG if tipo else False


def normalizar_cpf_cnpj_chave(valor):
    """Normaliza CPF/CNPJ para comparação (somente dígitos, zeros à esquerda)."""
    digitos = re.sub(r'\D', '', normalizar_texto_campo(valor))
    if not digitos or cpf_cnpj_eh_placeholder(digitos):
        return None
    if len(digitos) <= 11:
        return digitos.zfill(11)
    if len(digitos) <= 14:
        return digitos.zfill(14)
    return digitos


def _variantes_chave_cpf(chave):
    """Gera variantes da chave (com/sem zeros) para busca flexível."""
    if not chave:
        return set()
    variantes = {chave, chave.lstrip('0') or '0'}
    if len(chave) == 11:
        variantes.add(chave.zfill(11))
    elif len(chave) == 14:
        variantes.add(chave.zfill(14))
    return {v for v in variantes if v}


def carregar_cpfs_pessoa_mg(cursor):
    """
    Carrega CPF/CNPJ existentes em Pessoa_MG (colunas CPF_CNPJ e Pessoa_DocIdentificador).
    Retorna set de chaves normalizadas.
    """
    if not _tabela_existe(cursor, TABELA_PESSOA_MG):
        return None

    col_cpf = _resolver_coluna(cursor, TABELA_PESSOA_MG, 'CPF_CNPJ')
    col_doc = _resolver_coluna(cursor, TABELA_PESSOA_MG, 'Pessoa_DocIdentificador')
    if not col_cpf and not col_doc:
        return set()

    cols = []
    if col_cpf:
        cols.append(_quote_col(col_cpf))
    if col_doc:
        cols.append(_quote_col(col_doc))

    cpfs = set()
    _executar(cursor, f"SELECT DISTINCT {', '.join(cols)} FROM dbo.[{TABELA_PESSOA_MG}]")
    for row in cursor.fetchall():
        for val in row:
            chave = normalizar_cpf_cnpj_chave(val)
            if chave:
                cpfs.update(_variantes_chave_cpf(chave))
    return cpfs


def cpf_existe_em_pessoa_mg(chave, cpfs_pessoa_mg):
    if not chave or cpfs_pessoa_mg is None:
        return False
    return bool(_variantes_chave_cpf(chave) & cpfs_pessoa_mg)


def _indice_cpf_layout(layout_colunas):
    """Layouts dependentes: CPF_CNPJ é o campo 1 (índice 0)."""
    colunas = _ordenar_colunas_layout(layout_colunas)
    for idx, col in enumerate(colunas):
        if (col.get('Descricao') or '').strip().upper() == 'CPF_CNPJ':
            return idx, (col.get('Descricao') or 'CPF_CNPJ').strip()
    return 0, 'CPF_CNPJ'


def _cpf_linha(campos, idx_cpf):
    if len(campos) <= idx_cpf:
        return ''
    return normalizar_texto_campo(campos[idx_cpf])


def validar_linhas_dependem_pessoa_mg(linhas_campos, layout_colunas, cpfs_pessoa_mg, pessoa_mg_existe=True):
    """
    Erros bloqueantes por linha quando CPF/CNPJ não está em Pessoa_MG.
    """
    erros = []
    idx_cpf, rotulo_cpf = _indice_cpf_layout(layout_colunas)

    if not pessoa_mg_existe or cpfs_pessoa_mg is None:
        erros.append({
            'Linha': 1,
            'Coluna': '(Pessoa_MG)',
            'Erro': (
                'Tabela Pessoa_MG não existe no banco do projeto. '
                'Importe primeiro o arquivo 1 Forn_cli.txt.'
            ),
        })
        return erros

    if len(cpfs_pessoa_mg) == 0:
        erros.append({
            'Linha': 1,
            'Coluna': '(Pessoa_MG)',
            'Erro': (
                'Pessoa_MG está vazia. Importe primeiro o arquivo 1 Forn_cli.txt '
                'com os cadastros de pessoa.'
            ),
        })
        return erros

    for linha_idx, campos in enumerate(linhas_campos):
        linha_num = linha_idx + 1
        cpf_bruto = _cpf_linha(campos, idx_cpf)
        chave = normalizar_cpf_cnpj_chave(cpf_bruto)
        if not chave:
            continue

        if not cpf_existe_em_pessoa_mg(chave, cpfs_pessoa_mg):
            lido = cpf_bruto or '(vazio)'
            erros.append({
                'Linha': linha_num,
                'Coluna': rotulo_cpf,
                'Erro': (
                    f"CPF/CNPJ '{lido}' não encontrado em Pessoa_MG. "
                    'Importe o cadastro principal (1 Forn_cli.txt) antes deste layout.'
                ),
            })

    return erros


def validar_dependencia_pessoa_mg(linhas_campos, layout_colunas, layout_nome, layout_descricao=None, banco_gx=None):
    """
    Valida CPF/CNPJ contra Pessoa_MG para layouts secundários.
    Se banco_gx não informado, retorna lista vazia (checagem na importação).
    """
    if not layout_depende_pessoa_mg(layout_nome, layout_descricao, layout_colunas):
        return []

    if not banco_gx:
        logger.info('Dependência Pessoa_MG: banco não informado — validação na importação.')
        return []

    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return [{
            'Linha': 1,
            'Coluna': '(conexão)',
            'Erro': f'Não foi possível conectar ao banco {banco_gx} para validar Pessoa_MG.',
        }]

    cursor = conn.cursor()
    try:
        existe = _tabela_existe(cursor, TABELA_PESSOA_MG)
        cpfs = carregar_cpfs_pessoa_mg(cursor) if existe else None
        return validar_linhas_dependem_pessoa_mg(
            linhas_campos, layout_colunas, cpfs, pessoa_mg_existe=existe,
        )
    finally:
        cursor.close()
        conn.close()


def validar_dataframe_depende_pessoa_mg(cursor, df, layout_nome=None, layout_descricao=None, colunas_layout=None):
    """
    Valida DataFrame antes da importação (layouts secundários).
    Levanta RuntimeError se Pessoa_MG ausente ou CPF não cadastrado.
    """
    if not layout_depende_pessoa_mg(layout_nome, layout_descricao, colunas_layout):
        return

    existe = _tabela_existe(cursor, TABELA_PESSOA_MG)
    cpfs = carregar_cpfs_pessoa_mg(cursor) if existe else None

    linhas = []
    col_cpf = 'CPF_CNPJ'
    if df is not None and not df.empty:
        if col_cpf not in df.columns:
            for c in df.columns:
                if str(c).strip().upper() == 'CPF_CNPJ':
                    col_cpf = c
                    break
        for _, row in df.iterrows():
            linhas.append([str(row.get(col_cpf, '') or '')])

    colunas = colunas_layout or [{'Descricao': 'CPF_CNPJ', 'Posicao': 1}]
    erros = validar_linhas_dependem_pessoa_mg(linhas, colunas, cpfs, pessoa_mg_existe=existe)
    if erros:
        amostra = erros[0]['Erro']
        total = len(erros)
        raise RuntimeError(
            f"Dependência Pessoa_MG: {total} linha(s) com CPF/CNPJ não cadastrado. "
            f"Ex.: {amostra}"
        )

    logger.info("Dependência Pessoa_MG: todos os CPF/CNPJ encontrados em %s", TABELA_PESSOA_MG)


def validar_staging_depende_pessoa_mg(cursor, tabela_staging):
    """Valida staging antes da procedure de extração (layouts secundários)."""
    existe = _tabela_existe(cursor, TABELA_PESSOA_MG)
    cpfs = carregar_cpfs_pessoa_mg(cursor) if existe else None

    if not existe or cpfs is None:
        raise RuntimeError(
            f"Tabela {TABELA_PESSOA_MG} não existe. Importe primeiro o arquivo 1 Forn_cli.txt."
        )
    if len(cpfs) == 0:
        raise RuntimeError(
            f"{TABELA_PESSOA_MG} está vazia. Importe primeiro o arquivo 1 Forn_cli.txt."
        )

    col_cpf = _resolver_coluna(cursor, tabela_staging, 'CPF_CNPJ')
    if not col_cpf:
        raise RuntimeError(f"Coluna CPF_CNPJ não encontrada em {tabela_staging}.")

    cpf_col = _quote_col(col_cpf)
    _executar(cursor, f"SELECT {cpf_col} FROM dbo.[{tabela_staging}]")
    faltantes = 0
    for row in cursor.fetchall():
        chave = normalizar_cpf_cnpj_chave(row[0])
        if chave and not cpf_existe_em_pessoa_mg(chave, cpfs):
            faltantes += 1

    if faltantes:
        raise RuntimeError(
            f"{faltantes} registro(s) em {tabela_staging} com CPF/CNPJ ausente em {TABELA_PESSOA_MG}. "
            "Importe o cadastro principal (1 Forn_cli.txt) antes deste layout."
        )

    logger.info("Staging %s validada contra %s", tabela_staging, TABELA_PESSOA_MG)
