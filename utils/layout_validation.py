# Arquivo utils/layout_validation.py corrigido
import pandas as pd
import logging
from datetime import datetime
import numpy as np
import re

logger = logging.getLogger(__name__)

# Caracteres permitidos em datas após limpeza (dígitos + separadores comuns)
_REGEX_LIXO_DATA = re.compile(r'[^\d/.\-: ]')


def remover_todas_letras(valor):
    """Remove qualquer letra (A-Z, acentuadas, etc.), mantendo números e símbolos."""
    if valor is None:
        return ''
    return ''.join(c for c in str(valor) if not c.isalpha()).strip()


def limpar_valor_data(valor):
    """Remove letras e caracteres inválidos, mantendo só o que pode compor uma data."""
    sem_letras = remover_todas_letras(valor)
    return _REGEX_LIXO_DATA.sub('', sem_letras).strip()


# CPF/CNPJ fictícios inválidos (Forn_cli e demais layouts)
CPF_CNPJ_PLACEHOLDERS = frozenset({
    '00000000000', '11111111111', '22222222222', '33333333333',
    '44444444444', '55555555555', '66666666666', '77777777777',
    '88888888888', '99999999999',
    '00000000000000', '11111111111111',
})

# Cupom consumidor (MovimentoEstoque): somente 11 zeros ou 11 noves
CPF_CNPJ_CONSUMIDOR = frozenset({'00000000000', '99999999999'})


def cpf_cnpj_eh_consumidor(valor):
    """
    CPF de consumidor no MovimentoEstoque: sequência de 11 zeros ou 11 noves.
    Formas curtas: zeros completam à esquerda (00000000 → 00000000000);
    noves completam à esquerda com 9 (9999999 → 99999999999).
    """
    limpo = re.sub(r'\D', '', str(valor or ''))
    if not limpo or not limpo.isdigit() or len(limpo) > 11:
        return False
    if set(limpo) == {'0'}:
        return limpo.zfill(11) == '00000000000'
    if set(limpo) == {'9'}:
        return limpo.rjust(11, '9') == '99999999999'
    return limpo.zfill(11) in CPF_CNPJ_CONSUMIDOR


def cpf_cnpj_canonico_consumidor(valor):
    """Retorna 00000000000 ou 99999999999 quando for CPF consumidor; senão None."""
    if not cpf_cnpj_eh_consumidor(valor):
        return None
    limpo = re.sub(r'\D', '', str(valor or ''))
    if set(limpo) == {'9'}:
        return '99999999999'
    return '00000000000'


def cpf_cnpj_eh_placeholder(valor):
    """CPF/CNPJ fictício inválido (não usar para liberar consumidor no MovimentoEstoque)."""
    limpo = re.sub(r'\D', '', str(valor or ''))
    if not limpo:
        return False
    if limpo in CPF_CNPJ_PLACEHOLDERS:
        return True
    if len(limpo) <= 11 and limpo.zfill(11) in CPF_CNPJ_PLACEHOLDERS:
        return True
    if len(limpo) <= 14 and limpo.zfill(14) in CPF_CNPJ_PLACEHOLDERS:
        return True
    return False


EMAIL_PLACEHOLDERS = frozenset({
    'NT', 'N/A', 'NA', 'NULL', 'NONE', '-', '--', 'S/N', 'SN',
})


def normalizar_texto_campo(valor):
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ''
    if not isinstance(valor, str):
        valor = str(valor)
    valor = re.sub(r'[\x00-\x1f\x7f]', '', valor).strip()
    # Resíduo de § UTF-8 lido como ANSI (C2 fica como Â no fim do campo)
    if valor.endswith('Â'):
        valor = valor[:-1].rstrip()
    return valor


def processar_email(valor_str):
    """
    Normaliza e valida e-mail.
    Retorna (status, valor, mensagem): ok | convertido | vazio | erro
    """
    original = normalizar_texto_campo(valor_str)
    if not original:
        return 'vazio', '', None

    if original.upper() in EMAIL_PLACEHOLDERS:
        return 'vazio', '', f"Email '{original}' sem endereço — definido como vazio."

    candidato = original.lower().replace(' ', '')
    pattern = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'

    if re.match(pattern, candidato):
        return 'ok', original, None

    match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', original)
    if match:
        extraido = match.group(0)
        return 'convertido', extraido, f"Email '{original}' ajustado para '{extraido}'."

    return 'erro', None, f"Email inválido — valor lido: '{original}'"


def _ordenar_colunas_layout(layout_colunas):
    return sorted(layout_colunas, key=lambda c: int(c.get('Posicao') or 0))


def _formatar_tipo_coluna(coluna):
    tipo = coluna.get('TipoDado') or 'texto'
    obrigatorio = bool(coluna.get('Obrigatorio'))
    obrigatorio_txt = 'Sim' if obrigatorio else 'Não'
    return f"Tipo: {tipo}, Obrigatório: {obrigatorio_txt}"


def diagnosticar_divergencia_colunas(num_colunas_arquivo, layout_colunas):
    """
    Monta mensagem detalhada quando a quantidade de colunas do arquivo
    difere do layout, indicando quais colunas faltam ou sobram.
    """
    colunas_ordenadas = _ordenar_colunas_layout(layout_colunas)
    num_colunas_layout = len(colunas_ordenadas)

    linhas = [
        f"O arquivo possui {num_colunas_arquivo} colunas, mas o layout espera {num_colunas_layout} colunas."
    ]

    if num_colunas_arquivo < num_colunas_layout:
        faltantes = colunas_ordenadas[num_colunas_arquivo:]
        linhas.append("")
        linhas.append("Coluna(s) faltando no arquivo (conforme layout):")
        for coluna in faltantes:
            posicao = coluna.get('Posicao', '?')
            descricao = coluna.get('Descricao') or '(sem descrição)'
            linhas.append(
                f"  • Posição {posicao}: \"{descricao}\" ({_formatar_tipo_coluna(coluna)})"
            )

        if num_colunas_arquivo > 0:
            ultima_ok = colunas_ordenadas[num_colunas_arquivo - 1]
            linhas.append("")
            linhas.append(
                f"Última coluna encontrada no arquivo: posição {ultima_ok.get('Posicao')} — "
                f"\"{ultima_ok.get('Descricao') or '(sem descrição)'}\""
            )
            linhas.append(
                f"A próxima coluna esperada pelo layout é a posição {faltantes[0].get('Posicao')} — "
                f"\"{faltantes[0].get('Descricao') or '(sem descrição)'}\"."
            )

    elif num_colunas_arquivo > num_colunas_layout:
        linhas.append("")
        linhas.append("Coluna(s) extra no arquivo (sem definição no layout):")
        for posicao in range(num_colunas_layout + 1, num_colunas_arquivo + 1):
            linhas.append(f"  • Posição {posicao} (coluna presente no arquivo, não prevista no layout)")

        if colunas_ordenadas:
            ultima_layout = colunas_ordenadas[-1]
            linhas.append("")
            linhas.append(
                f"Última coluna definida no layout: posição {ultima_layout.get('Posicao')} — "
                f"\"{ultima_layout.get('Descricao') or '(sem descrição)'}\""
            )

    return "\n".join(linhas)


FORMATO_DATA_LAYOUT = '%d/%m/%Y'

FORMATOS_DATA_ENTRADA = [
    ('%d/%m/%Y', 'DD/MM/AAAA'),
    ('%d/%m/%y', 'DD/MM/AA'),
    ('%Y-%m-%d', 'AAAA-MM-DD'),
    ('%d-%m-%Y', 'DD-MM-AAAA'),
    ('%d-%m-%y', 'DD-MM-AA'),
    ('%d.%m.%Y', 'DD.MM.AAAA'),
    ('%d.%m.%y', 'DD.MM.AA'),
    ('%Y/%m/%d', 'AAAA/MM/DD'),
    ('%Y/%m/%d %H:%M:%S', 'AAAA/MM/DD HH:MM:SS'),
    ('%Y-%m-%d %H:%M:%S', 'AAAA-MM-DD HH:MM:SS'),
    ('%Y%m%d', 'AAAAMMDD'),
]


def tentar_parsear_data(valor_str):
    """Tenta interpretar a data em vários formatos. Retorna (date, rótulo_formato) ou (None, None)."""
    valor_str = (valor_str or '').strip()
    if not valor_str:
        return None, None

    for fmt, rotulo in FORMATOS_DATA_ENTRADA:
        try:
            return datetime.strptime(valor_str, fmt).date(), rotulo
        except ValueError:
            continue

    try:
        dt = pd.to_datetime(valor_str, dayfirst=True, errors='raise')
        if hasattr(dt, 'to_pydatetime'):
            return dt.to_pydatetime().date(), 'formato detectado automaticamente'
        return dt.date(), 'formato detectado automaticamente'
    except (ValueError, TypeError):
        return None, None


def processar_data(valor_str):
    """
    Remove letras, tenta interpretar e normaliza para DD/MM/AAAA.

    Retorna (status, valor_formatado, mensagem):
      - ('ok', valor, None)
      - ('convertido', valor, aviso)
      - ('vazio', '', aviso) — sem data válida após limpeza → null
    """
    valor_original = (valor_str or '').strip()
    if not valor_original:
        return 'vazio', '', None

    limpo = limpar_valor_data(valor_original)
    avisos_partes = []

    if limpo != valor_original:
        avisos_partes.append(f"texto/letras removidos — restou '{limpo or '(vazio)'}'")

    if not limpo or not re.search(r'\d', limpo):
        motivo = (
            f"Data '{valor_original}' sem conteúdo numérico válido após remover letras/texto "
            "— definido como vazio."
        )
        return 'vazio', '', motivo

    data_obj, formato_entrada = tentar_parsear_data(limpo)

    if data_obj is None:
        return (
            'vazio',
            '',
            f"Data '{valor_original}' não reconhecida após limpeza — definido como vazio."
        )

    valor_layout = data_obj.strftime(FORMATO_DATA_LAYOUT)

    try:
        datetime.strptime(limpo, FORMATO_DATA_LAYOUT)
        if avisos_partes:
            return 'convertido', valor_layout, (
                f"Data '{valor_original}' ajustada para '{valor_layout}' ({avisos_partes[0]})."
            )
        return 'ok', valor_layout, None
    except ValueError:
        pass

    if limpo == valor_layout and not avisos_partes:
        return 'ok', valor_layout, None

    detalhe = f" ({formato_entrada})" if formato_entrada else ''
    prefixo = f"Data '{valor_original}'"
    if avisos_partes:
        prefixo += f" — {avisos_partes[0]}"
    aviso = f"{prefixo} convertida{detalhe} para '{valor_layout}'."
    return 'convertido', valor_layout, aviso


TIPOS_SEM_LETRAS = frozenset({'numero', 'valor', 'cpf_cnpj'})


def sanitizar_campo_numerico(valor, tipo_dado):
    """
    Remove letras de campos que devem conter apenas números/símbolos numéricos.
    Se não restar nenhum dígito, retorna vazio (null).
    """
    if tipo_dado not in TIPOS_SEM_LETRAS:
        return valor, None

    original = (valor or '').strip()
    if not original:
        return '', None

    if tipo_dado == 'cpf_cnpj':
        limpo = re.sub(r'\D', '', original)
    elif tipo_dado == 'valor':
        limpo = remover_todas_letras(original)
    else:
        limpo = remover_todas_letras(original)

    if not re.search(r'\d', limpo):
        return '', f"Valor '{original}' sem números após remover letras/texto — definido como vazio."

    if limpo != original:
        return limpo, f"Valor '{original}' ajustado para '{limpo}' (letras/texto removidos)."

    return limpo, None


# Posições no layout cujo vazio impede a migração (Forn_cli: Nome=2, CPF_CNPJ=4)
POSICOES_CAMPO_OBRIGATORIO_MIGRACAO = {
    2: 'NOME',
    4: 'CPF_CNPJ',
}
NOMES_CAMPO_OBRIGATORIO_MIGRACAO = frozenset({'NOME', 'CPF_CNPJ'})

# Forn_cli_Documento: apenas CPF_CNPJ no campo 1
INDICE_FISICO_CAMPO_MIGRACAO_DOCUMENTO = {
    'CPF_CNPJ': 0,
}
NOMES_CAMPO_OBRIGATORIO_MIGRACAO_DOCUMENTO = frozenset({'CPF_CNPJ'})


# Índices físicos no arquivo (0-based): campo 2 = índice 1, campo 4 = índice 3
INDICE_FISICO_CAMPO_MIGRACAO = {
    'NOME': 1,
    'CPF_CNPJ': 3,
}


def _nomes_colunas_layout(layout_colunas):
    nomes = set()
    for c in layout_colunas or []:
        if isinstance(c, dict):
            nomes.add(str(c.get('Descricao') or '').strip().upper())
        else:
            nomes.add(str(c).strip().upper())
    nomes.discard('')
    return nomes


def _layout_eh_forn_cli_documento_colunas(layout_colunas):
    nomes = _nomes_colunas_layout(layout_colunas)
    return (
        'CPF_CNPJ' in nomes
        and 'INSC_ESTADUAL' in nomes
        and 'CODIGO_PESSOA' not in nomes
        and 'NOME' not in nomes
    )


def _layout_eh_forn_cli_principal_colunas(layout_colunas):
    """Layout 1 Forn_cli — CODIGO_PESSOA / NOME no cadastro principal."""
    nomes = _nomes_colunas_layout(layout_colunas)
    if 'CODIGO_PESSOA' in nomes:
        return True
    return 'NOME' in nomes and 'NOME_CONTATO' not in nomes


def _layout_eh_movimento_estoque_colunas(layout_colunas):
    nomes = _nomes_colunas_layout(layout_colunas)
    return (
        'MOVIMENTO_CODIGO' in nomes
        and 'DATA_MOVIMENTO' in nomes
        and 'CPF_CNPJ' in nomes
        and 'CNPJ_EMPRESA' in nomes
        and 'PRECO_MEDIO' not in nomes
        and 'PRODUTO_DESCRICAO' not in nomes
    )


def _layout_eh_fseg_cab_colunas(layout_colunas):
    nomes = _nomes_colunas_layout(layout_colunas)
    nomes = {n.replace('DATA_LIBERAÇÃO', 'DATA_LIBERACAO') for n in nomes}
    if 'PRODUTO_REFERENCIA' in nomes or 'PRODUTO_QUANTIDADE' in nomes:
        return False
    if 'TMO_REFERENCIA' in nomes or 'TMO_QUANTIDADE' in nomes:
        return False
    return (
        'NUMERO_OS' in nomes
        and 'CHASSI' in nomes
        and 'CPF_CNPJ' in nomes
        and 'TIPO_OS_CODIGO' in nomes
        and 'DATA_ABERTURA' in nomes
    )


def _layout_eh_fseg_prd_colunas(layout_colunas):
    nomes = _nomes_colunas_layout(layout_colunas)
    if 'CPF_CNPJ' in nomes and 'DATA_ABERTURA' in nomes:
        return False
    if 'ESTOQUE_CODIGO' in nomes or 'LOC_PRIMARIA' in nomes:
        return False
    if 'TMO_REFERENCIA' in nomes or 'TMO_QUANTIDADE' in nomes:
        return False
    return (
        'NUMERO_OS' in nomes
        and 'CHASSI' in nomes
        and 'PRODUTO_REFERENCIA' in nomes
        and 'PRODUTO_QUANTIDADE' in nomes
        and 'CNPJ_EMPRESA' in nomes
    )


def _layout_eh_fseg_srv_colunas(layout_colunas):
    nomes = _nomes_colunas_layout(layout_colunas)
    if 'CPF_CNPJ' in nomes and 'DATA_ABERTURA' in nomes:
        return False
    if 'PRODUTO_REFERENCIA' in nomes or 'PRODUTO_QUANTIDADE' in nomes:
        return False
    return (
        'NUMERO_OS' in nomes
        and 'CHASSI' in nomes
        and 'TMO_REFERENCIA' in nomes
        and 'TMO_QUANTIDADE' in nomes
        and 'CNPJ_EMPRESA' in nomes
    )


def _layout_eh_veiculo_colunas(layout_colunas):
    nomes = _nomes_colunas_layout(layout_colunas)
    if (
        _layout_eh_fseg_cab_colunas(layout_colunas)
        or _layout_eh_fseg_prd_colunas(layout_colunas)
        or _layout_eh_fseg_srv_colunas(layout_colunas)
    ):
        return False
    if 'NUMERO_OS' in nomes and 'CPF_CNPJ' in nomes:
        return False
    if 'NUMERO_OS' in nomes and ('PRODUTO_REFERENCIA' in nomes or 'TMO_REFERENCIA' in nomes):
        return False
    if 'CODIGO_PRODUTO' in nomes or 'MOVIMENTO_CODIGO' in nomes:
        return False
    return (
        'CODIGO_VEICULO' in nomes
        and 'CHASSI' in nomes
        and 'CNPJ_EMPRESA' in nomes
        and 'VEICULO_NOVO' in nomes
    )


def _layout_eh_prod_locacao_colunas(layout_colunas):
    nomes = _nomes_colunas_layout(layout_colunas)
    if 'MOVIMENTO_CODIGO' in nomes or 'DATA_MOVIMENTO' in nomes:
        return False
    if 'ESTOQUE_CODIGO' in nomes and 'QUANTIDADE' in nomes:
        return False
    return (
        'PRODUTO_REFERENCIA' in nomes
        and 'CNPJ_EMPRESA' in nomes
        and ('LOC_PRIMARIA' in nomes or 'LOC_SECUNDARIA' in nomes)
    )


def _layout_eh_produto_estoque_colunas(layout_colunas):
    nomes = _nomes_colunas_layout(layout_colunas)
    if _layout_eh_prod_locacao_colunas(layout_colunas):
        return False
    if 'MOVIMENTO_CODIGO' in nomes or 'DATA_MOVIMENTO' in nomes:
        return False
    return (
        'ESTOQUE_CODIGO' in nomes
        and 'QUANTIDADE' in nomes
        and 'PRODUTO_REFERENCIA' in nomes
        and 'CNPJ_EMPRESA' in nomes
        and 'PRODUTO_DESCRICAO' not in nomes
    )


def _layout_eh_produto_colunas(layout_colunas):
    """Layout 7 Produto — sem CPF/CNPJ de pessoa; usa CNPJ_EMPRESA."""
    nomes = _nomes_colunas_layout(layout_colunas)
    if 'ESTOQUE_CODIGO' in nomes and 'QUANTIDADE' in nomes and 'VALOR_VENDA' not in nomes:
        return False
    return (
        'CODIGO_PRODUTO' in nomes
        and 'PRODUTO_DESCRICAO' in nomes
        and 'CNPJ_EMPRESA' in nomes
        and 'CPF_CNPJ' not in nomes
        and 'CODIGO_PESSOA' not in nomes
    )


def _layout_eh_financeiro_colunas(layout_colunas):
    nomes = _nomes_colunas_layout(layout_colunas)
    if 'TIPO_FICHARAZAO' in nomes:
        return False
    return (
        'TIPO_MOVFINANCEIRO' in nomes
        and 'CPF_CNPJ' in nomes
        and 'CNPJ_EMPRESA' in nomes
        and 'TITULO_VALOR' in nomes
        and 'TITULO_SALDO' in nomes
    )


def _indices_por_descricao(colunas_ordenadas, chaves):
    indices = {}
    for idx, col in enumerate(colunas_ordenadas):
        desc = (col.get('Descricao') or '').strip().upper()
        if desc in chaves:
            indices[desc] = idx
    return indices


def _regras_migracao_layout(layout_colunas):
    """Retorna índices físicos e rótulos dos campos bloqueantes conforme o layout."""
    colunas_ordenadas = _ordenar_colunas_layout(layout_colunas)

    if _layout_eh_movimento_estoque_colunas(colunas_ordenadas):
        nomes = {'CPF_CNPJ', 'CNPJ_EMPRESA', 'DATA_MOVIMENTO', 'MOVIMENTO_CODIGO'}
        return {
            'tipo': 'movimento_estoque',
            'indices': _indices_por_descricao(colunas_ordenadas, nomes),
            'nomes': nomes,
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'CPF/CNPJ, CNPJ empresa, data ou movimento faltando no arquivo',
        }
    if _layout_eh_financeiro_colunas(colunas_ordenadas):
        nomes = {'CPF_CNPJ', 'CNPJ_EMPRESA', 'TIPO_MOVFINANCEIRO', 'TITULO_SALDO'}
        return {
            'tipo': 'financeiro',
            'indices': _indices_por_descricao(colunas_ordenadas, nomes),
            'nomes': nomes,
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'CPF/CNPJ, CNPJ empresa, tipo movimento ou colunas faltando no arquivo',
        }
    if _layout_eh_fseg_srv_colunas(colunas_ordenadas):
        nomes = {'CHASSI', 'NUMERO_OS', 'TMO_REFERENCIA', 'CNPJ_EMPRESA'}
        return {
            'tipo': 'fseg_srv',
            'indices': _indices_por_descricao(colunas_ordenadas, nomes),
            'nomes': nomes,
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'CHASSI, número OS, TMO referência ou colunas faltando no arquivo',
        }
    if _layout_eh_fseg_prd_colunas(colunas_ordenadas):
        nomes = {'CHASSI', 'NUMERO_OS', 'PRODUTO_REFERENCIA', 'CNPJ_EMPRESA'}
        return {
            'tipo': 'fseg_prd',
            'indices': _indices_por_descricao(colunas_ordenadas, nomes),
            'nomes': nomes,
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'CHASSI, número OS, produto referência ou colunas faltando no arquivo',
        }
    if _layout_eh_fseg_cab_colunas(colunas_ordenadas):
        nomes = {'CHASSI', 'CPF_CNPJ', 'NUMERO_OS', 'CNPJ_EMPRESA'}
        return {
            'tipo': 'fseg_cab',
            'indices': _indices_por_descricao(colunas_ordenadas, nomes),
            'nomes': nomes,
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'CHASSI, CPF/CNPJ, número OS ou colunas faltando no arquivo',
        }
    if _layout_eh_veiculo_colunas(colunas_ordenadas):
        nomes = {'CHASSI'}
        return {
            'tipo': 'veiculo',
            'indices': _indices_por_descricao(colunas_ordenadas, nomes),
            'nomes': nomes,
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'chassi ou colunas faltando no arquivo',
        }
    if _layout_eh_prod_locacao_colunas(colunas_ordenadas):
        nomes = {'PRODUTO_REFERENCIA', 'CNPJ_EMPRESA', 'LOC_PRIMARIA', 'LOC_SECUNDARIA'}
        return {
            'tipo': 'prod_locacao',
            'indices': _indices_por_descricao(colunas_ordenadas, nomes),
            'nomes': nomes,
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'referência, CNPJ empresa, localização ou colunas faltando no arquivo',
        }
    if _layout_eh_produto_estoque_colunas(colunas_ordenadas):
        nomes = {'PRODUTO_REFERENCIA', 'CNPJ_EMPRESA', 'ESTOQUE_CODIGO'}
        return {
            'tipo': 'produto_estoque',
            'indices': _indices_por_descricao(colunas_ordenadas, nomes),
            'nomes': nomes,
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'referência, CNPJ empresa, estoque ou colunas faltando no arquivo',
        }
    if _layout_eh_produto_colunas(colunas_ordenadas):
        nomes = {'CODIGO_PRODUTO', 'PRODUTO_DESCRICAO', 'CNPJ_EMPRESA'}
        return {
            'tipo': 'produto',
            'indices': _indices_por_descricao(colunas_ordenadas, nomes),
            'nomes': nomes,
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'Código do produto, descrição, CNPJ da empresa ou colunas faltando no arquivo',
        }
    if _layout_eh_forn_cli_documento_colunas(colunas_ordenadas):
        return {
            'tipo': 'forn_cli_documento',
            'indices': dict(INDICE_FISICO_CAMPO_MIGRACAO_DOCUMENTO),
            'nomes': set(NOMES_CAMPO_OBRIGATORIO_MIGRACAO_DOCUMENTO),
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'CPF/CNPJ ou colunas faltando no arquivo',
        }
    if _layout_eh_forn_cli_principal_colunas(colunas_ordenadas):
        return {
            'tipo': 'forn_cli',
            'indices': dict(INDICE_FISICO_CAMPO_MIGRACAO),
            'nomes': set(NOMES_CAMPO_OBRIGATORIO_MIGRACAO),
            'cpf_fallback_indice_0': True,
            'mensagem_erros': 'Nome, CPF/CNPJ ou colunas faltando no arquivo',
        }
    if colunas_ordenadas and 'CPF_CNPJ' not in _nomes_colunas_layout(colunas_ordenadas):
        return {
            'tipo': 'generico',
            'indices': {},
            'nomes': set(),
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'campos obrigatórios ou colunas faltando no arquivo',
        }
    return {
        'tipo': 'forn_cli_secundario',
        'indices': dict(INDICE_FISICO_CAMPO_MIGRACAO_DOCUMENTO),
        'nomes': set(NOMES_CAMPO_OBRIGATORIO_MIGRACAO_DOCUMENTO),
        'cpf_fallback_indice_0': False,
        'mensagem_erros': 'CPF/CNPJ ou colunas faltando no arquivo',
    }


def _rotulos_campos_migracao(layout_colunas):
    """Descrições do layout para mensagens de erro de migração."""
    colunas_ordenadas = _ordenar_colunas_layout(layout_colunas)
    regras = _regras_migracao_layout(colunas_ordenadas)
    rotulos = {}
    for chave, idx in regras['indices'].items():
        if idx < len(colunas_ordenadas):
            rotulos[chave] = (colunas_ordenadas[idx].get('Descricao') or chave).strip()
        else:
            rotulos[chave] = chave
    return rotulos, regras


def _cpf_preenchido_migracao(valor):
    """CPF/CNPJ presente para migração: tem dígitos e não é placeholder."""
    digitos = re.sub(r'\D', '', normalizar_texto_campo(valor))
    if not digitos:
        return False
    return not cpf_cnpj_eh_placeholder(valor)


def _cpf_ok_migracao_movimento_estoque(valor):
    """MovimentoEstoque: vazio ou CPF consumidor (11 zeros / 11 noves) é aceito."""
    if not normalizar_texto_campo(valor):
        return True
    if cpf_cnpj_eh_consumidor(valor):
        return True
    return _cpf_preenchido_migracao(valor)


def _cpf_migracao_linha(campos, regras=None):
    """CPF no índice do layout; Forn_cli aceita fallback no campo 1 (CODIGO_PESSOA)."""
    regras = regras or _regras_migracao_layout(None)
    idx_cpf = regras['indices']['CPF_CNPJ']
    candidatos = []
    if len(campos) > idx_cpf:
        candidatos.append(campos[idx_cpf])
    if regras.get('cpf_fallback_indice_0') and len(campos) > 0:
        candidatos.append(campos[0])
    return any(_cpf_preenchido_migracao(c) for c in candidatos)


def _validar_campos_obrigatorios_migracao(linhas_campos, layout_colunas):
    """
    Erros bloqueantes por layout:
    - Forn_cli: Nome (campo 2) e CPF/CNPJ (campo 4)
    - Forn_cli_Documento / secundários (Endereco, etc.): CPF/CNPJ (campo 1)
    - Produto: CODIGO_PRODUTO, PRODUTO_DESCRICAO, CNPJ_EMPRESA
    """
    erros = []
    colunas_ordenadas = _ordenar_colunas_layout(layout_colunas)
    rotulos, regras = _rotulos_campos_migracao(colunas_ordenadas)
    indices = regras['indices']

    if regras.get('tipo') == 'generico':
        return erros

    for linha_idx, campos in enumerate(linhas_campos):
        linha_num = linha_idx + 1

        if 'NOME' in indices:
            idx_nome = indices['NOME']
            nome = normalizar_texto_campo(campos[idx_nome] if len(campos) > idx_nome else '')
            if not nome:
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['NOME'],
                    'Erro': 'Nome obrigatório para migração (campo 2) não preenchido',
                })

        if 'CPF_CNPJ' in indices:
            idx_cpf = indices['CPF_CNPJ']
            cpf_bruto = campos[idx_cpf] if len(campos) > idx_cpf else ''
            if regras.get('tipo') == 'movimento_estoque':
                cpf_ok = _cpf_ok_migracao_movimento_estoque(cpf_bruto)
            else:
                cpf_ok = _cpf_migracao_linha(campos, regras)
            if not cpf_ok:
                lido = normalizar_texto_campo(cpf_bruto) or '(vazio)'
                campo_num = idx_cpf + 1
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['CPF_CNPJ'],
                    'Erro': (
                        f"CPF/CNPJ obrigatório para migração (campo {campo_num}) não preenchido "
                        f"(valor lido: '{lido}', {len(campos)} campos na linha)"
                    ),
                })

        if 'CODIGO_PRODUTO' in indices:
            idx = indices['CODIGO_PRODUTO']
            valor = normalizar_texto_campo(campos[idx] if len(campos) > idx else '')
            if not valor:
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['CODIGO_PRODUTO'],
                    'Erro': (
                        f"Código do produto obrigatório para migração (campo {idx + 1}) não preenchido"
                    ),
                })

        if 'PRODUTO_DESCRICAO' in indices:
            idx = indices['PRODUTO_DESCRICAO']
            valor = normalizar_texto_campo(campos[idx] if len(campos) > idx else '')
            if not valor:
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['PRODUTO_DESCRICAO'],
                    'Erro': (
                        f"Descrição do produto obrigatória para migração (campo {idx + 1}) não preenchida"
                    ),
                })

        if 'CNPJ_EMPRESA' in indices:
            idx = indices['CNPJ_EMPRESA']
            cnpj_bruto = campos[idx] if len(campos) > idx else ''
            if not _cpf_preenchido_migracao(cnpj_bruto):
                lido = normalizar_texto_campo(cnpj_bruto) or '(vazio)'
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['CNPJ_EMPRESA'],
                    'Erro': (
                        f"CNPJ da empresa obrigatório para migração (campo {idx + 1}) não preenchido "
                        f"(valor lido: '{lido}')"
                    ),
                })

        if 'PRODUTO_REFERENCIA' in indices and regras.get('tipo') in ('produto_estoque', 'prod_locacao'):
            idx = indices['PRODUTO_REFERENCIA']
            valor = normalizar_texto_campo(campos[idx] if len(campos) > idx else '')
            if not valor:
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['PRODUTO_REFERENCIA'],
                    'Erro': (
                        f"Referência do produto obrigatória para migração (campo {idx + 1}) não preenchida"
                    ),
                })

        if regras.get('tipo') == 'prod_locacao':
            idx_pri = indices.get('LOC_PRIMARIA')
            idx_sec = indices.get('LOC_SECUNDARIA')
            loc_pri = ''
            loc_sec = ''
            if idx_pri is not None and len(campos) > idx_pri:
                loc_pri = normalizar_texto_campo(campos[idx_pri])
            if idx_sec is not None and len(campos) > idx_sec:
                loc_sec = normalizar_texto_campo(campos[idx_sec])
            if not loc_pri and not loc_sec:
                erros.append({
                    'Linha': linha_num,
                    'Coluna': 'LOC_PRIMARIA / LOC_SECUNDARIA',
                    'Erro': 'Informe ao menos LOC_PRIMARIA ou LOC_SECUNDARIA para migração',
                })

        if 'ESTOQUE_CODIGO' in indices:
            idx = indices['ESTOQUE_CODIGO']
            valor = normalizar_texto_campo(campos[idx] if len(campos) > idx else '')
            if not valor:
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['ESTOQUE_CODIGO'],
                    'Erro': (
                        f"Código de estoque obrigatório para migração (campo {idx + 1}) não preenchido"
                    ),
                })

        if 'DATA_MOVIMENTO' in indices:
            idx = indices['DATA_MOVIMENTO']
            valor = normalizar_texto_campo(campos[idx] if len(campos) > idx else '')
            if not valor:
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['DATA_MOVIMENTO'],
                    'Erro': (
                        f"Data do movimento obrigatória para migração (campo {idx + 1}) não preenchida"
                    ),
                })

        if 'MOVIMENTO_CODIGO' in indices:
            idx = indices['MOVIMENTO_CODIGO']
            valor = normalizar_texto_campo(campos[idx] if len(campos) > idx else '')
            if not valor:
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['MOVIMENTO_CODIGO'],
                    'Erro': (
                        f"Código do movimento obrigatório para migração (campo {idx + 1}) não preenchido"
                    ),
                })

        if 'CHASSI' in indices:
            idx = indices['CHASSI']
            valor = normalizar_texto_campo(campos[idx] if len(campos) > idx else '')
            if not valor:
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['CHASSI'],
                    'Erro': (
                        f"CHASSI obrigatório para migração (campo {idx + 1}) não preenchido"
                    ),
                })

        if 'NUMERO_OS' in indices:
            idx = indices['NUMERO_OS']
            valor = normalizar_texto_campo(campos[idx] if len(campos) > idx else '')
            if not valor:
                erros.append({
                    'Linha': linha_num,
                    'Coluna': rotulos['NUMERO_OS'],
                    'Erro': (
                        f"Número da OS obrigatório para migração (campo {idx + 1}) não preenchido"
                    ),
                })

    return erros


def _chave_duplicidade_produto(campos, indices):
    """Chave: referência + descrição + marca + CNPJ empresa (normalizados)."""
    def valor(chave):
        idx = indices.get(chave)
        if idx is None or len(campos) <= idx:
            return ''
        return normalizar_texto_campo(campos[idx])

    referencia = valor('PRODUTO_REFERENCIA').upper()
    descricao = valor('PRODUTO_DESCRICAO').upper()
    marca = valor('MARCA_CODIGO').upper()
    cnpj = re.sub(r'\D', '', valor('CNPJ_EMPRESA'))

    if not referencia or not descricao or not marca or not cnpj:
        return None
    return referencia, descricao, marca, cnpj


def _validar_duplicidade_produto(linhas_campos, layout_colunas):
    """
    Duplicidade no arquivo: mesma Referência + Descrição + Marca + CNPJ empresa.
    Mantém a 1ª ocorrência; demais linhas geram erro bloqueante.
    """
    colunas_ordenadas = _ordenar_colunas_layout(layout_colunas)
    if not _layout_eh_produto_colunas(colunas_ordenadas):
        return []

    chaves = {'PRODUTO_REFERENCIA', 'PRODUTO_DESCRICAO', 'MARCA_CODIGO', 'CNPJ_EMPRESA'}
    indices = _indices_por_descricao(colunas_ordenadas, chaves)
    if len(indices) < len(chaves):
        return []

    erros = []
    primeira_linha = {}

    for linha_idx, campos in enumerate(linhas_campos):
        chave = _chave_duplicidade_produto(campos, indices)
        if not chave:
            continue

        linha_num = linha_idx + 1
        if chave in primeira_linha:
            erros.append({
                'Linha': linha_num,
                'Coluna': 'PRODUTO_REFERENCIA',
                'Erro': (
                    'Registro duplicado: mesma Referência, Descrição, Marca e CNPJ da empresa '
                    f"da linha {primeira_linha[chave]}."
                ),
            })
        else:
            primeira_linha[chave] = linha_num

    return erros


SEPARADORES_ARQUIVO = ('§', '\xa7', '?')


def _separador_na_linha(linha_texto):
    """Retorna o separador de campo usado na linha (§ padrão; ? em exportações legadas)."""
    if not linha_texto:
        return None
    for sep in SEPARADORES_ARQUIVO:
        if sep in linha_texto:
            return sep
    return None


def _separador_dominante_texto(texto):
    """Escolhe o separador mais usado no arquivo (prioriza § quando existir)."""
    for sep in SEPARADORES_ARQUIVO:
        if sep in texto:
            return sep
    return SEPARADORES_ARQUIVO[0]


def _splitar_por_separador(linha_texto, sep):
    campos = linha_texto.split(sep)
    if campos and campos[-1] == '':
        campos = campos[:-1]
    return [normalizar_texto_campo(c) for c in campos]


def _corrigir_utf8_lido_como_ansi(texto):
    """UTF-8 lido como ANSI/Latin-1 transforma § (C2 A7) em Â§ — o Â gruda no campo (ex.: 24Â)."""
    if not texto:
        return texto
    if 'Â§' in texto:
        texto = texto.replace('Â§', '§')
    return texto


def decodificar_bytes_arquivo(raw):
    """Decodifica o arquivo: UTF-8 primeiro; se falhar, Windows-1252 / Latin-1."""
    if raw is None:
        return ''
    if isinstance(raw, str):
        return _corrigir_utf8_lido_como_ansi(raw)
    if raw.startswith(b'\xef\xbb\xbf'):
        return raw.decode('utf-8-sig')
    try:
        return raw.decode('utf-8')
    except UnicodeDecodeError:
        pass
    for enc in ('cp1252', 'latin-1'):
        try:
            return _corrigir_utf8_lido_como_ansi(raw.decode(enc))
        except UnicodeDecodeError:
            continue
    return _corrigir_utf8_lido_como_ansi(raw.decode('latin-1', errors='replace'))


def _decodificar_arquivo(arquivo):
    if hasattr(arquivo, 'seek'):
        arquivo.seek(0)
    elif hasattr(arquivo, 'stream') and hasattr(arquivo.stream, 'seek'):
        arquivo.stream.seek(0)
    raw = arquivo.read()
    return decodificar_bytes_arquivo(raw)


def _splitar_linha_arquivo(linha_texto, sep_preferido=None):
    """Divide uma linha do arquivo nos campos físicos (§ ou ?)."""
    linha_texto = linha_texto.rstrip('\r\n')
    if not linha_texto.strip():
        return []
    if sep_preferido and sep_preferido in linha_texto:
        return _splitar_por_separador(linha_texto, sep_preferido)
    for sep in SEPARADORES_ARQUIVO:
        if sep in linha_texto:
            return _splitar_por_separador(linha_texto, sep)
    return [normalizar_texto_campo(linha_texto)]


def _texto_usa_separador_campo(texto):
    for sep in SEPARADORES_ARQUIVO:
        if sep in texto:
            return True
    return False


def _campo_parece_codigo_pessoa(valor):
    """Indica início de registro Forn_cli (campo 1 = CPF/CNPJ)."""
    digitos = re.sub(r'\D', '', normalizar_texto_campo(valor))
    if not digitos:
        return False
    if cpf_cnpj_eh_placeholder(digitos):
        return False
    return len(digitos) in (11, 14)


def _campo_parece_chassi_ou_codigo_veiculo(valor):
    """Indica início de registro Veículo (campo 1 = CODIGO_VEICULO / CHASSI)."""
    texto = normalizar_texto_campo(valor)
    if not texto or len(texto) < 8:
        return False
    # VIN/chassi: alfanumérico, sem espaços (ex.: 93XATGA2WJCJ39522)
    if re.fullmatch(r'[A-HJ-NPR-Z0-9]{11,21}', texto, flags=re.IGNORECASE):
        return True
    return False


def _campo_parece_inicio_registro(valor):
    """Detecta início de um novo registro (Forn_cli por CPF/CNPJ ou Veículo por chassi)."""
    return _campo_parece_codigo_pessoa(valor) or _campo_parece_chassi_ou_codigo_veiculo(valor)


def _juntar_linha_texto(buffer, linha, sep=None):
    """Concatena continuação de linha física, reinserindo o separador de campo se necessário."""
    if not buffer:
        return linha
    if not linha:
        return buffer
    sep = sep or _separador_na_linha(buffer) or _separador_na_linha(linha) or SEPARADORES_ARQUIVO[0]
    termina_sep = buffer.endswith(sep)
    inicia_sep = linha.startswith(sep)
    if not termina_sep and not inicia_sep:
        return buffer + sep + linha
    return buffer + linha


def _juntar_linhas_fisicas(linhas_fisicas, sep=None):
    """Une linhas físicas reinserindo o separador quando a quebra cortou entre campos."""
    sep = sep or _separador_dominante_texto('\n'.join(linhas_fisicas))
    buffer = ''
    for ln in linhas_fisicas:
        if not buffer:
            buffer = ln
            continue
        buffer = _juntar_linha_texto(buffer, ln, sep=sep)
    return buffer


def _escolher_corte_registro(campos, min_campos, max_campos, tamanhos_completos=None):
    """
    Define onde cortar quando há mais campos que o layout permite.

    Só corta se o campo na posição candidata parecer início de um novo registro
    (CPF/CNPJ ou chassi). Caso contrário retorna None — o registro extra deve
    ser criticado como "colunas a mais", não virar uma linha fantasma incompleta.
    """
    candidatos = []
    for tam in (tamanhos_completos or ()):
        if tam not in candidatos:
            candidatos.append(tam)
    for tam in (min_campos, max_campos):
        if tam not in candidatos:
            candidatos.append(tam)
    # Do maior para o menor: prefere o registro mais completo antes do próximo CPF
    for tam in sorted((t for t in candidatos if t), reverse=True):
        if len(campos) > tam and _campo_parece_inicio_registro(campos[tam]):
            return tam
    return None


def _registros_de_linhas_fisicas(linhas_fisicas, num_colunas_esperadas, tamanhos_completos=None):
    """
    Monta registros a partir de linhas físicas.

    - Uma linha completa (ex.: Endereco com 12 campos) = um registro.
    - Linhas incompletas são acumuladas até formar um registro (Forn_cli quebrado).
    - Se a próxima linha física já começa um novo registro (CPF/chassi), o buffer
      atual é fechado mesmo que faltem 1–2 campos finais vazios (comum em Veículo).
    - Vários registros na mesma linha são separados quando o próximo inicia com CPF/chassi.
    - Sobras sem início de registro válido NÃO são cortadas (evita "linha com 2 campos").
    """
    # Tolera até 2 campos vazios finais (arquivo costuma omitir trailing §).
    min_campos = max(1, num_colunas_esperadas - 2)
    max_campos = num_colunas_esperadas
    tamanhos_completos = tuple(tamanhos_completos or ())
    sep_arquivo = _separador_dominante_texto('\n'.join(linhas_fisicas))
    resultado = []
    buffer_text = ''

    def _linha_completa(n):
        if tamanhos_completos:
            return n in tamanhos_completos
        return min_campos <= n <= max_campos

    for ln in linhas_fisicas:
        if not ln.strip():
            continue

        campos_nova = _splitar_linha_arquivo(ln, sep_preferido=sep_arquivo)
        if (
            buffer_text
            and campos_nova
            and _campo_parece_inicio_registro(campos_nova[0])
        ):
            campos_buffer = _splitar_linha_arquivo(buffer_text, sep_preferido=sep_arquivo)
            # Já há conteúdo no buffer e a nova linha inicia outro registro:
            # fecha o buffer (telefone com 13 campos / 4 fones não deve juntar com a próxima).
            if campos_buffer:
                resultado.append(campos_buffer)
                buffer_text = ''

        buffer_text = _juntar_linha_texto(buffer_text, ln, sep=sep_arquivo) if buffer_text else ln
        campos = _splitar_linha_arquivo(buffer_text, sep_preferido=sep_arquivo)

        while True:
            if _linha_completa(len(campos)):
                resultado.append(campos)
                buffer_text = ''
                campos = []
                break
            if len(campos) > max_campos:
                split_at = _escolher_corte_registro(
                    campos, min_campos, max_campos, tamanhos_completos,
                )
                if split_at is None:
                    # Um único registro com campos a mais (ex.: Financeiro 29 vs layout 27).
                    resultado.append(campos)
                    buffer_text = ''
                    campos = []
                    break
                resultado.append(campos[:split_at])
                campos = campos[split_at:]
                buffer_text = sep_arquivo.join(campos) if campos else ''
                continue
            break

        if campos:
            buffer_text = sep_arquivo.join(campos)

    if buffer_text.strip():
        resultado.append(_splitar_linha_arquivo(buffer_text, sep_preferido=sep_arquivo))

    return resultado


def _extrair_todos_campos_arquivo(texto):
    """
    Extrai campos do arquivo ignorando quebras de linha físicas.

    No export §, CR/LF no meio do registro não delimitam coluna — só § delimita.
    """
    linhas_fisicas = [ln for ln in re.split(r'[\r\n]+', texto) if ln.strip()]
    if not linhas_fisicas:
        return []
    texto_unido = _juntar_linhas_fisicas(linhas_fisicas)
    return _splitar_linha_arquivo(texto_unido)


def _agrupar_campos_em_registros(todos_campos, num_colunas_esperadas):
    """
    Agrupa campos § em registros completos (fallback para stream contínuo).
    """
    if not todos_campos:
        return []

    min_campos = max(1, num_colunas_esperadas - 2)
    max_campos = num_colunas_esperadas
    total = len(todos_campos)

    if total <= max_campos:
        return [todos_campos]

    registros = []
    inicio = 0

    for i in range(1, total):
        if not _campo_parece_inicio_registro(todos_campos[i]):
            continue
        tamanho = i - inicio
        if min_campos <= tamanho <= max_campos:
            registros.append(todos_campos[inicio:i])
            inicio = i

    if inicio < total:
        registros.append(todos_campos[inicio:])

    return registros


def _ler_arquivo_layout(arquivo, num_colunas_esperadas=None, tamanhos_completos=None):
    """Lê o arquivo (separador § ou ?) — cada linha física vira registro quando completa."""
    texto = _decodificar_arquivo(arquivo)
    linhas_fisicas = [ln for ln in re.split(r'[\r\n]+', texto) if ln.strip()]

    if num_colunas_esperadas and _texto_usa_separador_campo(texto):
        linhas_campos = _registros_de_linhas_fisicas(
            linhas_fisicas, num_colunas_esperadas, tamanhos_completos=tamanhos_completos,
        )
        sep = _separador_dominante_texto(texto)
        linhas_texto = [sep.join(campos) for campos in linhas_campos]
        return linhas_texto, linhas_campos

    linhas_texto = linhas_fisicas
    linhas_campos = [_splitar_linha_arquivo(ln) for ln in linhas_texto]
    return linhas_texto, linhas_campos


def _normalizar_campos_linha(campos, num_colunas_layout):
    """Preenche campos faltantes no final até o tamanho do layout."""
    campos = list(campos)
    if len(campos) > num_colunas_layout:
        return campos[:num_colunas_layout], 'extra'
    if len(campos) < num_colunas_layout:
        campos.extend([''] * (num_colunas_layout - len(campos)))
    return campos, None


def _erros_estrutura_linhas(linhas_campos, num_colunas_layout):
    """
    Erros bloqueantes por linha quando faltam colunas no meio do registro.
    Até 2 colunas finais do layout podem ser omitidas (comum no Veículo: trailing §).
    """
    erros = []
    min_aceitavel = max(1, num_colunas_layout - 2)
    for linha_idx, campos in enumerate(linhas_campos):
        num = len(campos)
        linha_num = linha_idx + 1
        if num > num_colunas_layout:
            erros.append({
                'Linha': linha_num,
                'Coluna': '(estrutura)',
                'Erro': (
                    f"Linha com {num} campos — layout espera {num_colunas_layout}. "
                    "Há colunas a mais no registro."
                ),
            })
        elif num < min_aceitavel:
            erros.append({
                'Linha': linha_num,
                'Coluna': '(estrutura)',
                'Erro': (
                    f"Linha com {num} campos — layout espera {num_colunas_layout}. "
                    "Até duas colunas finais podem ser omitidas (sem § de fechamento)."
                ),
            })
    return erros


def _erros_estrutura_linhas_telefone(linhas_campos, num_base, num_max, eh_telefone=True):
    """Estrutura do telefone: 4/7/10/13/16 campos (1 a 5 telefones)."""
    from utils.importacao_forn_cli_telefone import (
        INSTRUCAO_TELEFONE,
        TELEFONE_TAMANHOS_VALIDOS,
        tamanho_telefone_valido,
    )

    erros = []
    min_aceitavel = min(TELEFONE_TAMANHOS_VALIDOS)
    for linha_idx, campos in enumerate(linhas_campos):
        num = len(campos)
        linha_num = linha_idx + 1
        if num > num_max:
            erros.append({
                'Linha': linha_num,
                'Coluna': '(estrutura)',
                'Erro': (
                    f"Linha com {num} campos — telefone aceita no máximo {num_max}. "
                    + INSTRUCAO_TELEFONE
                ),
            })
        elif not tamanho_telefone_valido(num, num_max):
            erros.append({
                'Linha': linha_num,
                'Coluna': '(estrutura)',
                'Erro': (
                    f"Linha com {num} campos — telefone deve ter "
                    f"{', '.join(str(t) for t in TELEFONE_TAMANHOS_VALIDOS)} "
                    "campos (CPF/CNPJ + grupos de 3: DDD, NUMERO, TIPO). "
                    + INSTRUCAO_TELEFONE
                ),
            })
        elif num < min_aceitavel:
            erros.append({
                'Linha': linha_num,
                'Coluna': '(estrutura)',
                'Erro': (
                    f"Linha com {num} campos — layout espera pelo menos {min_aceitavel} "
                    f"(CPF/CNPJ + 1 telefone)."
                ),
            })
    return erros


def _dataframe_de_linhas(linhas_campos, num_colunas_layout):
    """Monta DataFrame a partir dos campos já divididos por linha."""
    rows = []
    for campos in linhas_campos:
        normalizada, _ = _normalizar_campos_linha(campos, num_colunas_layout)
        rows.append(normalizada)
    return pd.DataFrame(rows, dtype=str).fillna('')


def validar_arquivo_com_layout(
    arquivo, layout_colunas, layout_nome=None, layout_descricao=None,
    banco_gx=None, validar_dependencias_banco=True,
):
    """
    Valida um arquivo com base nas colunas do layout do banco e aplica valores default.
    Layouts secundários validam CPF/CNPJ em Pessoa_MG quando validar_dependencias_banco=True.
    Na validação de estrutura (validar_dependencias_banco=False) gera apenas avisos informativos.
    """
    try:
        if hasattr(arquivo, 'seek'):
            arquivo.seek(0)
        elif hasattr(arquivo, 'stream') and hasattr(arquivo.stream, 'seek'):
            arquivo.stream.seek(0)

        colunas_ordenadas = _ordenar_colunas_layout(layout_colunas)
        num_colunas_layout = len(colunas_ordenadas)

        from utils.importacao_forn_cli_telefone import (
            layout_eh_forn_cli_telefone,
            expandir_colunas_telefone,
            validar_extras_telefone,
            INSTRUCAO_TELEFONE,
            TELEFONE_TAMANHOS_VALIDOS,
            tamanho_telefone_valido,
        )
        eh_telefone = layout_eh_forn_cli_telefone(
            layout_nome, layout_descricao, colunas_ordenadas,
        )
        if eh_telefone:
            colunas_efetivas = expandir_colunas_telefone(colunas_ordenadas)
            num_colunas_max = len(colunas_efetivas)
        else:
            colunas_efetivas = colunas_ordenadas
            num_colunas_max = num_colunas_layout

        # Telefone: 4/7/10/13/16 campos são registros completos (não juntar duas linhas).
        linhas_texto, linhas_campos = _ler_arquivo_layout(
            arquivo,
            num_colunas_max,
            tamanhos_completos=TELEFONE_TAMANHOS_VALIDOS if eh_telefone else None,
        )
        if not linhas_campos:
            return None, None, pd.DataFrame(), "Arquivo vazio ou sem linhas válidas."

        contagens = [len(c) for c in linhas_campos]
        max_cols_arquivo = max(contagens)
        min_cols_arquivo = min(contagens)
        aviso_colunas_preenchidas = ''

        if eh_telefone:
            if max_cols_arquivo > num_colunas_max:
                erro_tel = validar_extras_telefone(
                    num_colunas_layout, max_cols_arquivo, num_colunas_max,
                )
                if erro_tel:
                    return None, None, pd.DataFrame(), erro_tel
            if tamanho_telefone_valido(max_cols_arquivo, num_colunas_max):
                num_colunas_usar = max(max_cols_arquivo, num_colunas_layout)
                num_colunas_usar = min(num_colunas_usar, num_colunas_max)
                colunas_efetivas = colunas_efetivas[:num_colunas_usar]
                n_fones = max(0, (max_cols_arquivo - 1) // 3)
                extras = max(0, max_cols_arquivo - num_colunas_layout)
                aviso_colunas_preenchidas = (
                    f" Layout telefone: arquivo com {max_cols_arquivo} colunas "
                    f"({n_fones} telefone(s); layout-base {num_colunas_layout}"
                    + (f" + {extras} extra(s) FONE4/FONE5" if extras else "")
                    + "). "
                )
            elif max_cols_arquivo > num_colunas_layout:
                erro_tel = validar_extras_telefone(
                    num_colunas_layout, max_cols_arquivo, num_colunas_max,
                )
                return None, None, pd.DataFrame(), erro_tel or (
                    f"O arquivo possui {max_cols_arquivo} colunas. " + INSTRUCAO_TELEFONE
                )
            else:
                num_colunas_usar = num_colunas_layout
                colunas_efetivas = colunas_ordenadas
        elif max_cols_arquivo > num_colunas_layout:
            return None, None, pd.DataFrame(), diagnosticar_divergencia_colunas(
                max_cols_arquivo, layout_colunas,
            )
        else:
            num_colunas_usar = num_colunas_layout
            colunas_efetivas = colunas_ordenadas

        min_campos_aceitavel = max(1, num_colunas_layout - 2)
        linhas_quase_completas = sum(
            1 for n in contagens
            if min_campos_aceitavel <= n <= num_colunas_usar
        )
        if linhas_quase_completas < len(linhas_campos):
            logger.info(
                "Arquivo: linhas com %s a %s campos; layout com %s (efetivo %s). "
                "%s linha(s) com estrutura incompleta.",
                min_cols_arquivo, max_cols_arquivo, num_colunas_layout, num_colunas_usar,
                len(linhas_campos) - linhas_quase_completas,
            )

        if not aviso_colunas_preenchidas and min_campos_aceitavel <= max_cols_arquivo < num_colunas_layout:
            aviso_colunas_preenchidas = (
                " Coluna(s) final(is) ausente(s) em algumas linhas (sem § de fechamento) "
                f"foram consideradas vazias (layout: {num_colunas_layout} campos)."
            )
        elif not aviso_colunas_preenchidas and max_cols_arquivo < min_campos_aceitavel:
            aviso_colunas_preenchidas = (
                f" Linhas válidas ({min_campos_aceitavel}–{num_colunas_layout} campos) foram processadas; "
                f"linhas com menos de {min_campos_aceitavel} campos geram erro por linha."
            )

        df = _dataframe_de_linhas(linhas_campos, num_colunas_usar)
        
        # Renomear colunas conforme layout (inclui extras de telefone, se houver)
        nomes_colunas = [(coluna.get('Descricao') or '').strip() for coluna in colunas_efetivas]
        df.columns = nomes_colunas
        
        # Validar cada coluna conforme regras do layout e aplicar defaults
        # Estrutura: base do layout; extras de telefone só checam grupos completos (já feitos)
        erros = _erros_estrutura_linhas_telefone(
            linhas_campos, num_colunas_layout, num_colunas_usar, eh_telefone,
        ) if eh_telefone else _erros_estrutura_linhas(linhas_campos, num_colunas_layout)
        avisos = []
        processado = df.values.tolist()
        total_linhas = len(processado)
        logger.info(
            "Validação iniciada: %s linhas, %s colunas (layout=%s, efetivo=%s)",
            total_linhas, num_colunas_layout, layout_nome or '?', num_colunas_usar,
        )
        log_interval = max(5000, total_linhas // 10) if total_linhas else 0
        
        _, regras_migracao = _rotulos_campos_migracao(colunas_ordenadas)
        indices_migracao = set(regras_migracao['indices'].values())
        nomes_migracao = regras_migracao['nomes']
        eh_movimento_estoque = regras_migracao.get('tipo') == 'movimento_estoque'

        for linha_idx, linha in enumerate(processado):
            if log_interval and linha_idx > 0 and linha_idx % log_interval == 0:
                logger.info(
                    "Validação em progresso: %s/%s linhas (%.0f%%)",
                    linha_idx, total_linhas, 100 * linha_idx / total_linhas,
                )
            
            for col_idx, coluna in enumerate(colunas_efetivas):
                posicao_layout = int(coluna.get('Posicao') or (col_idx + 1))
                descricao = (coluna.get('Descricao') or '').strip()
                obrigatorio = bool(coluna.get('Obrigatorio'))
                tipo_dado = coluna.get('TipoDado') or 'texto'
                validacao = coluna.get('Validacao', '')

                # col_idx = coluna física no arquivo (1ª § = índice 0)
                if col_idx < len(linha):
                    valor = linha[col_idx]
                else:
                    valor = ''

                if posicao_layout != col_idx + 1:
                    logger.warning(
                        "Layout '%s': Posição cadastrada (%s) difere da ordem no arquivo (%s)",
                        descricao, posicao_layout, col_idx + 1,
                    )
                
                valor = normalizar_texto_campo(valor)

                # Campos numéricos: remove letras; sem dígitos → vazio
                if tipo_dado in TIPOS_SEM_LETRAS and valor:
                    valor, aviso_sanit = sanitizar_campo_numerico(valor, tipo_dado)
                    if aviso_sanit:
                        avisos.append({
                            'Linha': linha_idx + 1,
                            'Coluna': descricao,
                            'Aviso': aviso_sanit
                        })
                    if not valor:
                        processado[linha_idx][col_idx] = ''

                # Datas: remove letras, converte ou define vazio se inválida
                if tipo_dado == 'data' and valor:
                    status_data, valor_data, msg_data = processar_data(valor)
                    if msg_data:
                        avisos.append({
                            'Linha': linha_idx + 1,
                            'Coluna': descricao,
                            'Aviso': msg_data
                        })
                    if status_data == 'vazio':
                        valor = ''
                        processado[linha_idx][col_idx] = ''
                    else:
                        valor = valor_data
                        processado[linha_idx][col_idx] = valor_data
                        continue

                # E-mail: normalizar; inválido vira aviso + vazio (não bloqueia importação)
                if tipo_dado == 'email' and valor:
                    status_email, valor_email, msg_email = processar_email(valor)
                    if status_email == 'erro':
                        avisos.append({
                            'Linha': linha_idx + 1,
                            'Coluna': descricao,
                            'Aviso': (
                                f"{msg_email} — definido como vazio (coluna layout posição {posicao_layout})"
                                if msg_email else
                                f"Email inválido — definido como vazio (coluna layout posição {posicao_layout})"
                            ),
                        })
                        valor = ''
                        processado[linha_idx][col_idx] = ''
                        continue
                    if msg_email and status_email in ('vazio', 'convertido'):
                        avisos.append({
                            'Linha': linha_idx + 1,
                            'Coluna': descricao,
                            'Aviso': msg_email,
                        })
                    if status_email == 'vazio':
                        valor = ''
                        processado[linha_idx][col_idx] = ''
                    else:
                        valor = valor_email
                        processado[linha_idx][col_idx] = valor_email
                        continue
                
                # Aplicar valor default se estiver vazio e houver validação como default
                if not valor and validacao and not validacao.startswith('#'):
                    valor_default = validacao.strip()
                    processado[linha_idx][col_idx] = valor_default
                    valor = valor_default

                # Cupom consumidor (MovimentoEstoque): 00000000000 ou 99999999999
                if (
                    tipo_dado == 'cpf_cnpj' and valor
                    and eh_movimento_estoque and descricao.upper() == 'CPF_CNPJ'
                    and cpf_cnpj_eh_consumidor(valor)
                ):
                    avisos.append({
                        'Linha': linha_idx + 1,
                        'Coluna': descricao,
                        'Aviso': (
                            f"CPF consumidor ('{valor}') — aceito na integração "
                            "(11 zeros ou 11 noves / cupom consumidor)."
                        ),
                    })
                    processado[linha_idx][col_idx] = cpf_cnpj_canonico_consumidor(valor) or converter_valor(valor, tipo_dado)
                    continue

                # Placeholder inválido em CPF/CNPJ → vazio
                if tipo_dado == 'cpf_cnpj' and valor and cpf_cnpj_eh_placeholder(valor):
                    avisos.append({
                        'Linha': linha_idx + 1,
                        'Coluna': descricao,
                        'Aviso': (
                            f"CPF/CNPJ '{valor}' é placeholder inválido "
                            "(ex.: zeros ou dígitos repetidos) — definido como vazio."
                        ),
                    })
                    valor = ''
                    processado[linha_idx][col_idx] = ''
                
                # Campo obrigatório do layout (exceto Nome/CPF) → aviso, não bloqueia migração
                if obrigatorio and not valor:
                    desc_upper = descricao.upper()
                    if col_idx in indices_migracao or desc_upper in nomes_migracao:
                        continue
                    avisos.append({
                        'Linha': linha_idx + 1,
                        'Coluna': descricao,
                        'Aviso': 'Campo obrigatório no layout não preenchido — mantido vazio',
                    })
                    continue

                # Tipo inválido → aviso; tenta converter quando possível
                if valor:
                    erro_validacao = validar_tipo_dado(valor, tipo_dado, validacao, descricao)
                    if erro_validacao:
                        avisos.append({
                            'Linha': linha_idx + 1,
                            'Coluna': descricao,
                            'Aviso': f"{erro_validacao} — valor mantido para conferência",
                        })
                    else:
                        valor_convertido = converter_valor(valor, tipo_dado)
                        processado[linha_idx][col_idx] = valor_convertido

        df_processado = pd.DataFrame(processado, columns=nomes_colunas, dtype=str).fillna('')
        from utils.importacao_forn_cli import _deduplicar_colunas_dataframe
        df_processado = _deduplicar_colunas_dataframe(df_processado)
        logger.info(
            "Validação células concluída: %s linhas, %s aviso(s) até aqui",
            total_linhas, len(avisos),
        )

        erros.extend(_validar_campos_obrigatorios_migracao(linhas_campos, colunas_ordenadas))
        erros.extend(_validar_duplicidade_produto(linhas_campos, colunas_ordenadas))

        if layout_nome:
            if validar_dependencias_banco:
                from utils.importacao_pessoa_mg_dependencia import (
                    avisar_dependencia_pessoa_mg_opcional,
                    pessoa_mg_dependencia_opcional,
                    validar_dependencia_pessoa_mg,
                )
                from utils.importacao_produto_mg_dependencia import validar_dependencia_produto_mg
                from utils.importacao_veiculo_mg_dependencia import validar_dependencia_veiculo_mg
                from utils.importacao_ficha_cab_mg_dependencia import validar_dependencia_ficha_cab_mg
                if pessoa_mg_dependencia_opcional(layout_nome, layout_descricao, colunas_ordenadas):
                    avisos.extend(
                        avisar_dependencia_pessoa_mg_opcional(
                            linhas_campos, colunas_ordenadas, layout_nome, layout_descricao, banco_gx,
                        )
                    )
                else:
                    erros.extend(
                        validar_dependencia_pessoa_mg(
                            linhas_campos, colunas_ordenadas, layout_nome, layout_descricao, banco_gx,
                        )
                    )
                erros.extend(
                    validar_dependencia_produto_mg(
                        linhas_campos, colunas_ordenadas, layout_nome, layout_descricao, banco_gx,
                    )
                )
                erros.extend(
                    validar_dependencia_veiculo_mg(
                        linhas_campos, colunas_ordenadas, layout_nome, layout_descricao, banco_gx,
                    )
                )
                erros.extend(
                    validar_dependencia_ficha_cab_mg(
                        linhas_campos, colunas_ordenadas, layout_nome, layout_descricao, banco_gx,
                    )
                )
            else:
                from utils.importacao_dependencia_avisos import gerar_avisos_dependencia_estrutura
                avisos.extend(
                    gerar_avisos_dependencia_estrutura(
                        layout_nome, layout_descricao, colunas_ordenadas,
                    )
                )

        df_erros = pd.DataFrame(erros) if erros else pd.DataFrame()
        df_avisos = pd.DataFrame(avisos) if avisos else pd.DataFrame()
        
        mensagem = "Arquivo validado com sucesso"
        if aviso_colunas_preenchidas:
            mensagem += "." + aviso_colunas_preenchidas
        if not df_avisos.empty:
            mensagem += f" {len(df_avisos)} aviso(s) de conversão automática (ex.: datas ou números ajustados)."
        
        return df_processado, df_erros, df_avisos, mensagem
        
    except Exception as e:
        logger.error(f"Erro na validação do arquivo: {e}")
        return None, None, pd.DataFrame(), f"Erro na validação: {str(e)}"

def converter_valor(valor, tipo_dado):
    """Converte o valor para o tipo apropriado"""
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        return ''

    try:
        if tipo_dado == 'numero':
            return int(float(str(valor).replace(',', '.')))
        elif tipo_dado == 'valor':
            valor_limpo = str(valor).replace('R$', '').replace(',', '.').replace(' ', '')
            return float(valor_limpo)
        elif tipo_dado == 'data':
            data_obj, _ = tentar_parsear_data(str(valor))
            if data_obj is None:
                return str(valor)
            return data_obj.strftime(FORMATO_DATA_LAYOUT)
        elif tipo_dado == 'cpf_cnpj':
            return re.sub(r'\D', '', str(valor))
        else:
            return valor
    except (ValueError, TypeError):
        return valor

def validar_tipo_dado(valor, tipo_dado, validacao, nome_coluna):
    """Valida o valor conforme o tipo de dado"""
    try:
        # Garantir que valor é string para as validações
        if not isinstance(valor, str):
            valor_str = str(valor)
        else:
            valor_str = valor
        
        valor_str = valor_str.strip()
        
        if tipo_dado == 'numero':
            # Validar número inteiro
            try:
                int(float(valor_str))  # Converte para float primeiro para lidar com decimais
            except ValueError:
                return f"Valor '{valor}' não é um número inteiro válido"
                
        elif tipo_dado == 'valor':
            # Validar número decimal
            try:
                valor_limpo = valor_str.replace(',', '.').replace('R$', '').replace(' ', '')
                float(valor_limpo)
            except ValueError:
                return f"Valor '{valor}' não é um valor decimal válido"
                
        elif tipo_dado == 'data':
            limpo = limpar_valor_data(valor_str)
            if not limpo or not re.search(r'\d', limpo):
                return None
            data_obj, _ = tentar_parsear_data(limpo)
            if data_obj is None:
                return None
            return None
        elif tipo_dado == 'cpf_cnpj':
            valor_limpo = re.sub(r'\D', '', valor_str)
            if not valor_limpo:
                return None
            if cpf_cnpj_eh_placeholder(valor_limpo):
                return None
            if len(valor_limpo) == 11:
                return None
            elif len(valor_limpo) == 14:
                return None
            else:
                return (
                    f"CPF/CNPJ inválido — deve ter 11 (CPF) ou 14 (CNPJ) dígitos. "
                    f"Valor: '{valor}' ({len(valor_limpo)} dígitos)"
                )
                
        elif tipo_dado == 'email':
            return None
        # Validação customizada se existir (começa com # para diferenciar de valor default)
        if validacao and validacao.strip() and validacao.startswith('#'):
            try:
                expr_validacao = validacao[1:]  # Remove o #
                # Contexto seguro para eval
                contexto_seguro = {
                    'valor': valor_str,
                    'len': len,
                    're': re,
                    'str': str,
                    'int': int,
                    'float': float
                }
                if not eval(expr_validacao, contexto_seguro):
                    return f"Valor '{valor}' não atende à validação: {expr_validacao}"
            except Exception as e:
                logger.warning(f"Erro ao executar validação customizada '{validacao}': {e}")
                return f"Erro na validação customizada: {str(e)}"
        
        return None
        
    except Exception as e:
        return f"Erro na validação do {tipo_dado}: {str(e)}"