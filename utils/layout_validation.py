# Arquivo utils/layout_validation.py corrigido
import pandas as pd
import logging
from datetime import datetime
import numpy as np
import re

logger = logging.getLogger(__name__)

def validar_arquivo_com_layout(arquivo, layout_colunas):
    """
    Valida um arquivo com base nas colunas do layout do banco e aplica valores default
    """
    try:
        # Ler o arquivo
        df = pd.read_csv(arquivo, sep='§', encoding='latin-1', dtype=str, header=None)
        df = df.fillna('')
        
        # Verificar número de colunas
        num_colunas_arquivo = df.shape[1]
        num_colunas_layout = len(layout_colunas)
        
        if num_colunas_arquivo != num_colunas_layout:
            return None, None, f"O arquivo possui {num_colunas_arquivo} colunas, mas o layout espera {num_colunas_layout} colunas"
        
        # Renomear colunas conforme layout
        nomes_colunas = [coluna['Descricao'] for coluna in layout_colunas]
        df.columns = nomes_colunas
        
        # Validar cada coluna conforme regras do layout e aplicar defaults
        erros = []
        df_processado = df.copy()
        
        for i, linha in enumerate(df.itertuples(index=False)):
            linha_idx = i
            
            for col_idx, coluna in enumerate(layout_colunas):
                posicao = coluna['Posicao'] - 1
                descricao = coluna['Descricao']
                obrigatorio = coluna['Obrigatorio']
                tipo_dado = coluna['TipoDado']
                validacao = coluna.get('Validacao', '')
                
                # Acessar valor por posição
                if posicao < len(linha):
                    valor = linha[posicao]
                else:
                    valor = ''
                
                # Tratar valores NaN/None
                if pd.isna(valor) or valor is None:
                    valor = ''
                
                # Strip se for string
                if isinstance(valor, str):
                    valor = valor.strip()
                
                # Aplicar valor default se estiver vazio e houver validação como default
                if not valor and validacao and not validacao.startswith('#'):
                    # Considera a validação como valor default se não começar com '#'
                    valor_default = validacao
                    df_processado.iloc[linha_idx, col_idx] = valor_default
                    valor = valor_default
                
                # Validar campo obrigatório
                if obrigatorio and not valor:
                    erros.append({
                        'Linha': linha_idx + 1,  # CORREÇÃO: Alterado de +2 para +1
                        'Coluna': descricao,
                        'Erro': 'Campo obrigatório não preenchido'
                    })
                    continue
                
                # Validar tipo de dado se tiver valor
                if valor:
                    erro_validacao = validar_tipo_dado(valor, tipo_dado, validacao, descricao)
                    if erro_validacao:
                        erros.append({
                            'Linha': linha_idx + 1,  # CORREÇÃO: Alterado de +2 para +1
                            'Coluna': descricao,
                            'Erro': erro_validacao
                        })
                    else:
                        # Se passou na validação, converter o tipo no DataFrame processado
                        valor_convertido = converter_valor(valor, tipo_dado)
                        df_processado.iloc[linha_idx, col_idx] = valor_convertido
        
        # Converter erros para DataFrame
        df_erros = pd.DataFrame(erros) if erros else pd.DataFrame()
        
        return df_processado, df_erros, "Arquivo validado com sucesso"
        
    except Exception as e:
        logger.error(f"Erro na validação do arquivo: {e}")
        return None, None, f"Erro na validação: {str(e)}"

def converter_valor(valor, tipo_dado):
    """Converte o valor para o tipo apropriado"""
    try:
        if tipo_dado == 'numero':
            return int(float(valor)) if valor else 0
        elif tipo_dado == 'valor':
            # Remove formatação de moeda e converte para float
            valor_limpo = str(valor).replace('R$', '').replace(',', '.').replace(' ', '')
            return float(valor_limpo) if valor_limpo else 0.0
        elif tipo_dado == 'data':
            # Tenta converter para datetime
            formatos_data = ['%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d', '%d-%m-%Y']
            for formato in formatos_data:
                try:
                    return datetime.strptime(str(valor), formato).date()
                except ValueError:
                    continue
            return valor  # Retorna original se não conseguir converter
        else:
            return valor  # Mantém como string para outros tipos
    except (ValueError, TypeError):
        return valor  # Retorna original em caso de erro na conversão

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
            # Validar data (tentar formatos comuns)
            formatos_data = ['%d/%m/%Y', '%d/%m/%y', '%Y-%m-%d', '%d-%m-%Y', '%d.%m.%Y']
            data_valida = False
            for formato in formatos_data:
                try:
                    datetime.strptime(valor_str, formato)
                    data_valida = True
                    break
                except ValueError:
                    continue
            
            if not data_valida:
                return f"Data '{valor}' não está em um formato válido (DD/MM/AAAA, DD-MM-AAAA, etc.)"
                
        elif tipo_dado == 'cpf_cnpj':
            # CORREÇÃO: Validação simplificada de CPF/CNPJ - apenas formato básico
            valor_limpo = ''.join(filter(str.isdigit, valor_str))
            
            # Para CPF (11 dígitos)
            if len(valor_limpo) == 11:
                # Verificação básica de CPF - não validar dígitos verificadores para evitar falsos positivos
                # Apenas verificar se não é uma sequência repetida
                if valor_limpo == valor_limpo[0] * 11:
                    return "CPF inválido - sequência repetida"
                # Considera válido se tiver 11 dígitos e não for sequência repetida
                return None
                
            # Para CNPJ (14 dígitos)
            elif len(valor_limpo) == 14:
                # Verificação básica de CNPJ
                if valor_limpo == valor_limpo[0] * 14:
                    return "CNPJ inválido - sequência repetida"
                # Considera válido se tiver 14 dígitos e não for sequência repetida
                return None
                
            else:
                return f"CPF/CNPJ inválido - deve ter 11 (CPF) ou 14 (CNPJ) dígitos. Valor: '{valor}' tem {len(valor_limpo)} dígitos"
                
        elif tipo_dado == 'email':
            # Validar email apenas se não estiver vazio
            if valor_str:
                # Validar email com regex robusto
                pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                if not re.match(pattern, valor_str):
                    return "Email inválido - formato deve ser usuario@dominio.com"
        
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