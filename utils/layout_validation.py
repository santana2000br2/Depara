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


# CPF/CNPJ fictícios comuns — tratar como vazio, não como erro
CPF_CNPJ_PLACEHOLDERS = frozenset({
    '00000000000', '11111111111', '22222222222', '33333333333',
    '44444444444', '55555555555', '66666666666', '77777777777',
    '88888888888', '99999999999',
    '00000000000000', '11111111111111',
})


def cpf_cnpj_eh_placeholder(valor):
    limpo = re.sub(r'\D', '', str(valor or ''))
    return limpo in CPF_CNPJ_PLACEHOLDERS


EMAIL_PLACEHOLDERS = frozenset({
    'NT', 'N/A', 'NA', 'NULL', 'NONE', '-', '--', 'S/N', 'SN',
})


def normalizar_texto_campo(valor):
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return ''
    if not isinstance(valor, str):
        valor = str(valor)
    return re.sub(r'[\x00-\x1f\x7f]', '', valor).strip()


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


def _regras_migracao_layout(layout_colunas):
    """Retorna índices físicos e rótulos dos campos bloqueantes conforme o layout."""
    if _layout_eh_forn_cli_documento_colunas(layout_colunas):
        return {
            'tipo': 'forn_cli_documento',
            'indices': dict(INDICE_FISICO_CAMPO_MIGRACAO_DOCUMENTO),
            'nomes': set(NOMES_CAMPO_OBRIGATORIO_MIGRACAO_DOCUMENTO),
            'cpf_fallback_indice_0': False,
            'mensagem_erros': 'CPF/CNPJ ou colunas faltando no arquivo',
        }
    if _layout_eh_forn_cli_principal_colunas(layout_colunas):
        return {
            'tipo': 'forn_cli',
            'indices': dict(INDICE_FISICO_CAMPO_MIGRACAO),
            'nomes': set(NOMES_CAMPO_OBRIGATORIO_MIGRACAO),
            'cpf_fallback_indice_0': True,
            'mensagem_erros': 'Nome, CPF/CNPJ ou colunas faltando no arquivo',
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
    return not cpf_cnpj_eh_placeholder(digitos)


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
    """
    erros = []
    colunas_ordenadas = _ordenar_colunas_layout(layout_colunas)
    rotulos, regras = _rotulos_campos_migracao(colunas_ordenadas)
    indices = regras['indices']

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
            if not _cpf_migracao_linha(campos, regras):
                cpf_bruto = campos[idx_cpf] if len(campos) > idx_cpf else ''
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

    return erros


SEPARADORES_ARQUIVO = ('§', '\xa7')


def _decodificar_arquivo(arquivo):
    if hasattr(arquivo, 'seek'):
        arquivo.seek(0)
    elif hasattr(arquivo, 'stream') and hasattr(arquivo.stream, 'seek'):
        arquivo.stream.seek(0)
    raw = arquivo.read()
    if isinstance(raw, bytes):
        return raw.decode('latin-1', errors='replace')
    return str(raw)


def _splitar_linha_arquivo(linha_texto):
    """Divide uma linha do arquivo nos campos físicos (§)."""
    linha_texto = linha_texto.rstrip('\r\n')
    if not linha_texto.strip():
        return []
    for sep in SEPARADORES_ARQUIVO:
        if sep in linha_texto:
            return [normalizar_texto_campo(c) for c in linha_texto.split(sep)]
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


def _juntar_linha_texto(buffer, linha):
    """Concatena continuação de linha física, reinserindo § se a quebra cortou entre campos."""
    if not buffer:
        return linha
    if not linha:
        return buffer
    termina_sep = any(buffer.endswith(sep) for sep in SEPARADORES_ARQUIVO)
    inicia_sep = any(linha.startswith(sep) for sep in SEPARADORES_ARQUIVO)
    if not termina_sep and not inicia_sep:
        return buffer + SEPARADORES_ARQUIVO[0] + linha
    return buffer + linha


def _juntar_linhas_fisicas(linhas_fisicas):
    """Une linhas físicas reinserindo § quando a quebra cortou entre campos."""
    buffer = ''
    for ln in linhas_fisicas:
        if not buffer:
            buffer = ln
            continue
        buffer = _juntar_linha_texto(buffer, ln)
    return buffer


def _escolher_corte_registro(campos, min_campos, max_campos):
    """Define onde cortar quando há mais campos que o layout permite."""
    for tam in (min_campos, max_campos):
        if len(campos) > tam and _campo_parece_codigo_pessoa(campos[tam]):
            return tam
    return max_campos


def _registros_de_linhas_fisicas(linhas_fisicas, num_colunas_esperadas):
    """
    Monta registros a partir de linhas físicas.

    - Uma linha completa (ex.: Endereco com 12 campos) = um registro.
    - Linhas incompletas são acumuladas até formar um registro (Forn_cli quebrado).
    - Vários registros na mesma linha são separados quando possível.
    """
    min_campos = num_colunas_esperadas - 1
    max_campos = num_colunas_esperadas
    sep = SEPARADORES_ARQUIVO[0]
    resultado = []
    buffer_text = ''

    for ln in linhas_fisicas:
        if not ln.strip():
            continue
        buffer_text = _juntar_linha_texto(buffer_text, ln) if buffer_text else ln
        campos = _splitar_linha_arquivo(buffer_text)

        while True:
            if min_campos <= len(campos) <= max_campos:
                resultado.append(campos)
                buffer_text = ''
                campos = []
                break
            if len(campos) > max_campos:
                split_at = _escolher_corte_registro(campos, min_campos, max_campos)
                resultado.append(campos[:split_at])
                campos = campos[split_at:]
                buffer_text = sep.join(campos) if campos else ''
                continue
            break

        if campos:
            buffer_text = sep.join(campos)

    if buffer_text.strip():
        resultado.append(_splitar_linha_arquivo(buffer_text))

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

    min_campos = num_colunas_esperadas - 1
    max_campos = num_colunas_esperadas
    total = len(todos_campos)

    if total <= max_campos:
        return [todos_campos]

    registros = []
    inicio = 0

    for i in range(1, total):
        if not _campo_parece_codigo_pessoa(todos_campos[i]):
            continue
        tamanho = i - inicio
        if min_campos <= tamanho <= max_campos:
            registros.append(todos_campos[inicio:i])
            inicio = i

    if inicio < total:
        registros.append(todos_campos[inicio:])

    return registros


def _ler_arquivo_layout(arquivo, num_colunas_esperadas=None):
    """Lê o arquivo § — cada linha física vira registro quando completa."""
    texto = _decodificar_arquivo(arquivo)
    linhas_fisicas = [ln for ln in re.split(r'[\r\n]+', texto) if ln.strip()]

    if num_colunas_esperadas and _texto_usa_separador_campo(texto):
        linhas_campos = _registros_de_linhas_fisicas(linhas_fisicas, num_colunas_esperadas)
        sep = SEPARADORES_ARQUIVO[0]
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
    Somente a última coluna do layout pode ser omitida (26 campos em layout de 27).
    """
    erros = []
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
        elif num < num_colunas_layout - 1:
            erros.append({
                'Linha': linha_num,
                'Coluna': '(estrutura)',
                'Erro': (
                    f"Linha com {num} campos — layout espera {num_colunas_layout}. "
                    "Somente a última coluna pode ser omitida (sem § de fechamento)."
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


def validar_arquivo_com_layout(arquivo, layout_colunas, layout_nome=None, layout_descricao=None, banco_gx=None):
    """
    Valida um arquivo com base nas colunas do layout do banco e aplica valores default.
    Layouts secundários Forn_cli validam CPF/CNPJ em Pessoa_MG quando banco_gx informado.
    """
    try:
        if hasattr(arquivo, 'seek'):
            arquivo.seek(0)
        elif hasattr(arquivo, 'stream') and hasattr(arquivo.stream, 'seek'):
            arquivo.stream.seek(0)

        colunas_ordenadas = _ordenar_colunas_layout(layout_colunas)
        num_colunas_layout = len(colunas_ordenadas)

        linhas_texto, linhas_campos = _ler_arquivo_layout(arquivo, num_colunas_layout)
        if not linhas_campos:
            return None, None, pd.DataFrame(), "Arquivo vazio ou sem linhas válidas."

        contagens = [len(c) for c in linhas_campos]
        max_cols_arquivo = max(contagens)
        min_cols_arquivo = min(contagens)
        aviso_colunas_preenchidas = ''

        if max_cols_arquivo > num_colunas_layout:
            return None, None, pd.DataFrame(), diagnosticar_divergencia_colunas(
                max_cols_arquivo, layout_colunas,
            )

        linhas_so_ultima_ausente = sum(
            1 for n in contagens if n in (num_colunas_layout, num_colunas_layout - 1)
        )
        if linhas_so_ultima_ausente < len(linhas_campos):
            logger.info(
                "Arquivo: linhas com %s a %s campos; layout com %s. "
                "%s linha(s) com estrutura incompleta.",
                min_cols_arquivo, max_cols_arquivo, num_colunas_layout,
                len(linhas_campos) - linhas_so_ultima_ausente,
            )

        if max_cols_arquivo == num_colunas_layout - 1:
            aviso_colunas_preenchidas = (
                " Coluna(s) final(is) ausente(s) em algumas linhas (sem § de fechamento) "
                f"foram consideradas vazias (layout: {num_colunas_layout} campos)."
            )
        elif max_cols_arquivo < num_colunas_layout - 1:
            aviso_colunas_preenchidas = (
                f" Linhas válidas ({num_colunas_layout - 1}–{num_colunas_layout} campos) foram processadas; "
                f"linhas com menos de {num_colunas_layout - 1} campos geram erro por linha."
            )

        df = _dataframe_de_linhas(linhas_campos, num_colunas_layout)
        
        # Renomear colunas conforme layout
        nomes_colunas = [(coluna.get('Descricao') or '').strip() for coluna in colunas_ordenadas]
        df.columns = nomes_colunas
        
        # Validar cada coluna conforme regras do layout e aplicar defaults
        erros = _erros_estrutura_linhas(linhas_campos, num_colunas_layout)
        avisos = []
        df_processado = df.copy()
        
        _, regras_migracao = _rotulos_campos_migracao(colunas_ordenadas)
        indices_migracao = set(regras_migracao['indices'].values())
        nomes_migracao = regras_migracao['nomes']

        for i, linha in enumerate(df.itertuples(index=False)):
            linha_idx = i
            
            for col_idx, coluna in enumerate(colunas_ordenadas):
                posicao_layout = int(coluna.get('Posicao') or (col_idx + 1))
                descricao = (coluna.get('Descricao') or '').strip()
                obrigatorio = bool(coluna.get('Obrigatorio'))
                tipo_dado = coluna['TipoDado']
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
                        df_processado.iloc[linha_idx, col_idx] = ''

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
                        df_processado.iloc[linha_idx, col_idx] = ''
                    else:
                        valor = valor_data
                        df_processado.iloc[linha_idx, col_idx] = valor_data
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
                        df_processado.iloc[linha_idx, col_idx] = ''
                        continue
                    if msg_email and status_email in ('vazio', 'convertido'):
                        avisos.append({
                            'Linha': linha_idx + 1,
                            'Coluna': descricao,
                            'Aviso': msg_email,
                        })
                    if status_email == 'vazio':
                        valor = ''
                        df_processado.iloc[linha_idx, col_idx] = ''
                    else:
                        valor = valor_email
                        df_processado.iloc[linha_idx, col_idx] = valor_email
                        continue
                
                # Aplicar valor default se estiver vazio e houver validação como default
                if not valor and validacao and not validacao.startswith('#'):
                    valor_default = validacao.strip()
                    df_processado.iloc[linha_idx, col_idx] = valor_default
                    valor = valor_default

                # Placeholder ou default inválido em CPF/CNPJ → vazio
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
                    df_processado.iloc[linha_idx, col_idx] = ''
                
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
                        df_processado.iloc[linha_idx, col_idx] = valor_convertido

        erros.extend(_validar_campos_obrigatorios_migracao(linhas_campos, colunas_ordenadas))

        if layout_nome:
            from utils.importacao_pessoa_mg_dependencia import validar_dependencia_pessoa_mg
            erros.extend(
                validar_dependencia_pessoa_mg(
                    linhas_campos, colunas_ordenadas, layout_nome, layout_descricao, banco_gx,
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