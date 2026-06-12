import json
import logging
from typing import Tuple, Dict, Any

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)

LAYOUT_COLUMNS_JSON = """
{
    "Forn_cli": [
        "CODIGO_PESSOA", "NOME", "TIPO", "CPF_CNPJ", "E_MAIL", "DT_ANIVER", "SEGMENTO_OFICINA",
        "SEGMENTO_BALCAO", "SEGMENTO_VENDAS", "ESCOLARIDADE_CODIGO", "ESCOLARIDADE_DESCRICAO",
        "PROFISSAO_CODIGO", "PROFISSÃO_DESCRICAO", "ESTADO_CIVIL_CODIGO", "ESTADO_CIVIL_DESCRICAO",
        "SEXO", "DATA_CADASTRO", "LIM_CREDITO", "LIM_CREDITO_VALIDADE", "CONTATO", "NEGATIVADO",
        "BLOQUEIA_VENDA", "BLOQUEIA_OFICINA", "FAIXA_RENDA", "COD_MONTADORA", "EMAIL_ALTERNATIVO",
        "CODIGO_MYHONDA"
    ],
    "Forn_cli_Endereco": [
        "CPF_CNPJ", "ENDERECO", "NUMERO", "COMPLEMENTO", "CEP", "BAIRRO", "CIDADE", "COD_IBGE",
        "ESTADO", "PAIS", "TIPO_ENDERECO", "TIPO_LOGRADOURO"
    ],
    "Forn_cli_Documento": [
        "CPF_CNPJ", "INSC_ESTADUAL", "INSC_ESTADUAL_UF", "INSC_ESTADUAL_ORGAOEMISSOR",
        "INSC_MUNICIPAL", "INSC_MUNICIPAL_UF", "INSC_MUNICIPAL_ORGAOEMISSOR", "NUMERO_CNAE",
        "NUMERO_CNAE_UF", "NUMERO_CNAE_ORGAOEMISSOR", "INSC_SUFRAMA", "INSC_SUFRAMA_UF",
        "INSC_SUFRAMA_ORGAOEMISSOR", "NUMERO_RG", "NUMERO_RG_UF", "NUMERO_RG_ORGAOEMISSOR",
        "NUMERO_CNH", "NUMERO_CNH_UF", "NUMERO_CNH_ORGAOEMISSOR", "NUMERO_CRC", "NUMERO_CRC_UF",
        "NUMERO_CRC_ORGAOEMISSOR"
    ],
    "Forn_cli_Enquadramento": [
        "CPF_CNPJ", "CONTRIBUICAO_ICMS"
    ],
    "Forn_cli_Telefone": [
        "CPF_CNPJ", "DDD_FONE1", "NUMERO_FONE1", "TIPO_FONE1", "DDD_FONE2", "NUMERO_FONE2",
        "TIPO_FONE2", "DDD_FONE3", "NUMERO_FONE3", "TIPO_FONE3", "DDD_FONE4", "NUMERO_FONE4",
        "TIPO_FONE4", "DDD_FONE5", "NUMERO_FONE5", "TIPO_FONE5"
    ],
    "Forn_cli_Contato": [
        "CPF_CNPJ", "NOME_CONTATO", "CARGO", "DT_ANIVER", "CPF_CNPJ_CONTATO", "EMAIL",
        "EMAIL_ALTERNATIVO", "DDD_FONE1", "NUMERO_FONE1", "TIPO_FONE1", "DDD_FONE2", "NUMERO_FONE2",
        "TIPO_FONE2", "DDD_FONE3", "NUMERO_FONE3", "TIPO_FONE3", "DDD_FONE4", "NUMERO_FONE4",
        "TIPO_FONE4", "DDD_FONE5", "NUMERO_FONE5", "TIPO_FONE5"
    ],
    "Produto": [
        "CODIGO_PRODUTO", "PRODUTO_REFERENCIA", "PRODUTO_DESCRICAO", "VALOR_VENDA", "VALOR_SUGERIDO",
        "VALOR_AQUISICAO", "VALOR_GARANTIA", "CLASSIFISCACAO_FISCAL", "UNIDADE_PRODUTO_CODIGO",
        "UNIDADE_PRODUTO_DESCRICAO", "TIPO_PRODUTO_CODIGO", "TIPO_PRODUTO_DESCRICAO",
        "GRUPO_LUCRATIVIDADE_CODIGO", "GRUPO_LUCRATIVIDADE_DESCRICAO", "GRUPO_PRODUTO_CODIGO",
        "GRUPO_PRODUTO_DESCRICAO", "PROCEDENCIA_CODIGO", "PROCEDENCIA_DESCRICAO", "MARCA_CODIGO",
        "MARCA_DESCRICAO", "PRODUTO_ORIGINAL", "ENVIA_GARANTIA", "PRODUTO_PESO", "CNPJ_EMPRESA",
        "DESCRICAO_DETALHADA", "QTD_FRACIONADA", "QME", "CODIGO_ANP"
    ],
    "Veiculo": [
        "CODIGO_VEICULO", "DESCRICAO", "CHASSI", "ANO_FABRICACAO", "ANO_MODELO", "MODELO_CODIGO",
        "MODELO_DESCRICAO", "COR_EXTERNA_CODIGO", "COR_EXTERNA_DESCRICAO", "COR_INTERNA_CODIGO",
        "COR_INTERNA_DESCRICAO", "CNPJ_EMPRESA", "PLACA", "ESTADO_PLACA", "MUNICIPIO_PLACA",
        "CPF_CNPJ", "KM", "RENAVAM", "DATA_VENDA", "MOTOR_EXTERNO", "MOTOR_INTERNO", "SERIE",
        "DN_VENDEDOR", "VEICULO_NOVO", "VEICULO_MARCA_CODIGO", "VEICULO_MARCA_DESCRICAO",
        "CODIGO_LINHA", "KATASHIKI", "SUFIXO", "SUFIXO_COMERCIAL", "MAQUINA_IMPLEM"
    ],
    "ProdutoEstoque": [
        "CODIGO_PRODUTO", "PRODUTO_REFERENCIA", "CNPJ_EMPRESA", "ESTOQUE_CODIGO", "QUANTIDADE",
        "PRECO_MEDIO", "ESTOQUE_IDEAL", "ESTOQUE_CRITICO", "ESTOQUE_MAXIMO", "LOCALIZACAO"
    ],
    "Financeiro": [
        "CPF_CNPJ", "TIPO_MOVFINANCEIRO", "CNPJ_EMPRESA", "TITULO_NUMERO/TITULO_SERIE",
        "TITULO_PARCELA", "DATA_EMISSAO", "DATA_ENTRADA", "DATA_VENCIMENTO", "TITULO_VALOR",
        "TITULO_SALDO", "AGENTECOBRADOR_CODIGO", "AGENTECOBRADOR_DESCRICAO",
        "CONTAGERENCIAL_CODIGO", "CONTAGERENCIAL_DESCRICAO", "TIPOTITULO_CODIGO",
        "TIPOTITULO_DESCRICAO", "DEPARTAMENTO_CODIGO", "DEPARTAMENTO_DESCRICAO",
        "NATUREZAOPERACAO_CODIGO", "NATUREZAOPERACAO_DESCRICAO", "CODIGO_NOSSONUMERO",
        "CODIGO_BANCO", "CODIGO_AGENCIA", "CODIGO_CONTACORRENTE", "OBSERVACAO", "NSU", "AUTORIZACAO"
    ],
    "Adiantamento": [
        "CPF_CNPJ", "TIPO_MOVFINANCEIRO", "CNPJ_EMPRESA", "TIPO_FICHARAZAO",
        "DESCRICAO_FICHARAZAO", "DATA_MOVIMENTO", "VALOR_ORIGINAL", "VALOR_SALDO", "OBSERVACAO"
    ],
    "Fseg_Cab": [
        "CODIGO_VEICULO", "CPF_CNPJ", "CHASSI", "NUMERO_OS", "DATA_ABERTURA", "KM", "TIPO_OS_CODIGO", "TIPO_OS_DESCRICAO",
        "OBSERVACAO_OS", "CNPJ_EMPRESA", "DATA_LIBERAÇÃO", "USUARIO_CONSULTOR"
    ]
}
"""

LAYOUTS_RULES_JSON = """
{
    "Forn_cli": {
        "CODIGO_PESSOA": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 20},
        "NOME": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 100},
        "TIPO": {"Obrigatorio": true, "Tipo": "Texto", "ValoresPermitidos": ["F", "J"]},
        "CPF_CNPJ": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "E_MAIL": {"Obrigatorio": true, "Tipo": "Email", "TamanhoMax": 100},
        "DT_ANIVER": {"Obrigatorio": true, "Tipo": "Data"},
        "SEGMENTO_OFICINA": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "SEGMENTO_BALCAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "SEGMENTO_VENDAS": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "ESCOLARIDADE_CODIGO": {"Obrigatorio": true, "Tipo": "Numerico"},
        "ESCOLARIDADE_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "PROFISSAO_CODIGO": {"Obrigatorio": true, "Tipo": "Numerico"},
        "PROFISSÃO_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "ESTADO_CIVIL_CODIGO": {"Obrigatorio": true, "Tipo": "Numerico"},
        "ESTADO_CIVIL_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "SEXO": {"Obrigatorio": true, "Tipo": "Texto", "ValoresPermitidos": ["M", "F", "O"]},
        "DATA_CADASTRO": {"Obrigatorio": true, "Tipo": "Data"},
        "LIM_CREDITO": {"Obrigatorio": false, "Tipo": "Numerico"},
        "LIM_CREDITO_VALIDADE": {"Obrigatorio": true, "Tipo": "Data"},
        "CONTATO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "NEGATIVADO": {"Obrigatorio": false, "Tipo": "SimNao"},
        "BLOQUEIA_VENDA": {"Obrigatorio": false, "Tipo": "SimNao"},
        "BLOQUEIA_OFICINA": {"Obrigatorio": false, "Tipo": "SimNao"},
        "FAIXA_RENDA": {"Obrigatorio": false, "Tipo": "FaixaRenda"},
        "COD_MONTADORA": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "EMAIL_ALTERNATIVO": {"Obrigatorio": false, "Tipo": "Email", "TamanhoMax": 100},
        "CODIGO_MYHONDA": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50}
    },
    "Forn_cli_Endereco": {
        "CPF_CNPJ": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "ENDERECO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 150},
        "NUMERO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 20},
        "COMPLEMENTO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "CEP": {"Obrigatorio": true, "Tipo": "CEP"},
        "BAIRRO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 100},
        "CIDADE": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 100},
        "COD_IBGE": {"Obrigatorio": false, "Tipo": "Numerico"},
        "ESTADO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 2, "ValoresPermitidos": ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]},
        "PAIS": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "TIPO_ENDERECO": {"Obrigatorio": true, "Tipo": "Texto", "ValoresPermitidos": ["RESIDENCIAL", "COMERCIAL", "ENTREGA", "COBRANCA"]},
        "TIPO_LOGRADOURO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50}
    },
    "Forn_cli_Documento": {
        "CPF_CNPJ": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "INSC_ESTADUAL": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "INSC_ESTADUAL_UF": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 2},
        "INSC_ESTADUAL_ORGAOEMISSOR": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "INSC_MUNICIPAL": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "INSC_MUNICIPAL_UF": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 2},
        "INSC_MUNICIPAL_ORGAOEMISSOR": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "NUMERO_CNAE": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "NUMERO_CNAE_UF": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 2},
        "NUMERO_CNAE_ORGAOEMISSOR": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "INSC_SUFRAMA": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "INSC_SUFRAMA_UF": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 2},
        "INSC_SUFRAMA_ORGAOEMISSOR": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "NUMERO_RG": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "NUMERO_RG_UF": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 2},
        "NUMERO_RG_ORGAOEMISSOR": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "NUMERO_CNH": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "NUMERO_CNH_UF": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 2},
        "NUMERO_CNH_ORGAOEMISSOR": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "NUMERO_CRC": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "NUMERO_CRC_UF": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 2},
        "NUMERO_CRC_ORGAOEMISSOR": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50}
    },
    "Forn_cli_Enquadramento": {
        "CPF_CNPJ": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "CONTRIBUICAO_ICMS": {"Obrigatorio": true, "Tipo": "Texto", "ValoresPermitidos": ["S", "N"]}
    },
    "Forn_cli_Telefone": {
        "CPF_CNPJ": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "DDD_FONE1": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 3},
        "NUMERO_FONE1": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 10},
        "TIPO_FONE1": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 20},
        "DDD_FONE2": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 3},
        "NUMERO_FONE2": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 10},
        "TIPO_FONE2": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "DDD_FONE3": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 3},
        "NUMERO_FONE3": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 10},
        "TIPO_FONE3": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "DDD_FONE4": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 3},
        "NUMERO_FONE4": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 10},
        "TIPO_FONE4": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "DDD_FONE5": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 3},
        "NUMERO_FONE5": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 10},
        "TIPO_FONE5": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20}
    },
    "Forn_cli_Contato": {
        "CPF_CNPJ": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "NOME_CONTATO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 100},
        "CARGO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "DT_ANIVER": {"Obrigatorio": false, "Tipo": "Data"},
        "CPF_CNPJ_CONTATO": {"Obrigatorio": false, "Tipo": "CPF_CNPJ"},
        "EMAIL": {"Obrigatorio": false, "Tipo": "Email", "TamanhoMax": 100},
        "EMAIL_ALTERNATIVO": {"Obrigatorio": false, "Tipo": "Email", "TamanhoMax": 100},
        "DDD_FONE1": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 3},
        "NUMERO_FONE1": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 10},
        "TIPO_FONE1": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "DDD_FONE2": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 3},
        "NUMERO_FONE2": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 10},
        "TIPO_FONE2": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "DDD_FONE3": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 3},
        "NUMERO_FONE3": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 10},
        "TIPO_FONE3": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "DDD_FONE4": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 3},
        "NUMERO_FONE4": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 10},
        "TIPO_FONE4": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "DDD_FONE5": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 3},
        "NUMERO_FONE5": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 10},
        "TIPO_FONE5": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20}
    },
    "Produto": {
        "CODIGO_PRODUTO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "PRODUTO_REFERENCIA": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "PRODUTO_DESCRICAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 255},
        "VALOR_VENDA": {"Obrigatorio": true, "Tipo": "Numerico"},
        "VALOR_SUGERIDO": {"Obrigatorio": false, "Tipo": "Numerico"},
        "VALOR_AQUISICAO": {"Obrigatorio": true, "Tipo": "Numerico"},
        "VALOR_GARANTIA": {"Obrigatorio": false, "Tipo": "Numerico"},
        "CLASSIFISCACAO_FISCAL": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "UNIDADE_PRODUTO_CODIGO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 10},
        "UNIDADE_PRODUTO_DESCRICAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "TIPO_PRODUTO_CODIGO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 20},
        "TIPO_PRODUTO_DESCRICAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "GRUPO_LUCRATIVIDADE_CODIGO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "GRUPO_LUCRATIVIDADE_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "GRUPO_PRODUTO_CODIGO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 20},
        "GRUPO_PRODUTO_DESCRICAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "PROCEDENCIA_CODIGO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "PROCEDENCIA_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "MARCA_CODIGO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 20},
        "MARCA_DESCRICAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "PRODUTO_ORIGINAL": {"Obrigatorio": false, "Tipo": "SimNao"},
        "ENVIA_GARANTIA": {"Obrigatorio": false, "Tipo": "SimNao"},
        "PRODUTO_PESO": {"Obrigatorio": false, "Tipo": "Numerico"},
        "CNPJ_EMPRESA": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "DESCRICAO_DETALHADA": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 255},
        "QTD_FRACIONADA": {"Obrigatorio": false, "Tipo": "Numerico"},
        "QME": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "CODIGO_ANP": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20}
    },
    "Veiculo": {
        "CODIGO_VEICULO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 255},
        "CHASSI": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 17},
        "ANO_FABRICACAO": {"Obrigatorio": true, "Tipo": "Ano"},
        "ANO_MODELO": {"Obrigatorio": true, "Tipo": "Ano"},
        "MODELO_CODIGO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 20},
        "MODELO_DESCRICAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 100},
        "COR_EXTERNA_CODIGO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 20},
        "COR_EXTERNA_DESCRICAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "COR_INTERNA_CODIGO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "COR_INTERNA_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "CNPJ_EMPRESA": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "PLACA": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 7},
        "ESTADO_PLACA": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 2},
        "MUNICIPIO_PLACA": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "CPF_CNPJ": {"Obrigatorio": false, "Tipo": "CPF_CNPJ"},
        "KM": {"Obrigatorio": false, "Tipo": "Inteiro"},
        "RENAVAM": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 11},
        "DATA_VENDA": {"Obrigatorio": false, "Tipo": "Data"},
        "MOTOR_EXTERNO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "MOTOR_INTERNO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "SERIE": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "DN_VENDEDOR": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "VEICULO_NOVO": {"Obrigatorio": true, "Tipo": "Texto", "ValoresPermitidos": ["N", "U"]},
        "VEICULO_MARCA_CODIGO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 20},
        "VEICULO_MARCA_DESCRICAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "CODIGO_LINHA": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "KATASHIKI": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "SUFIXO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "SUFIXO_COMERCIAL": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "MAQUINA_IMPLEM": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50}
    },
    "ProdutoEstoque": {
        "CODIGO_PRODUTO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "PRODUTO_REFERENCIA": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "CNPJ_EMPRESA": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "ESTOQUE_CODIGO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 20},
        "QUANTIDADE": {"Obrigatorio": true, "Tipo": "Numerico"},
        "PRECO_MEDIO": {"Obrigatorio": true, "Tipo": "Numerico"},
        "ESTOQUE_IDEAL": {"Obrigatorio": true, "Tipo": "Numerico"},
        "ESTOQUE_CRITICO": {"Obrigatorio": true, "Tipo": "Numerico"},
        "ESTOQUE_MAXIMO": {"Obrigatorio": false, "Tipo": "Numerico"},
        "LOCALIZACAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50}
    },
    "Financeiro": {
        "CPF_CNPJ": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "TIPO_MOVFINANCEIRO": {"Obrigatorio": true, "Tipo": "Texto", "ValoresPermitidos": ["R", "P"]},
        "CNPJ_EMPRESA": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "TITULO_NUMERO/TITULO_SERIE": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 100},
        "TITULO_PARCELA": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 10},
        "DATA_EMISSAO": {"Obrigatorio": true, "Tipo": "Data"},
        "DATA_ENTRADA": {"Obrigatorio": true, "Tipo": "Data"},
        "DATA_VENCIMENTO": {"Obrigatorio": true, "Tipo": "Data"},
        "TITULO_VALOR": {"Obrigatorio": true, "Tipo": "Numerico"},
        "TITULO_SALDO": {"Obrigatorio": true, "Tipo": "Numerico"},
        "AGENTECOBRADOR_CODIGO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "AGENTECOBRADOR_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "CONTAGERENCIAL_CODIGO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "CONTAGERENCIAL_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "TIPOTITULO_CODIGO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "TIPOTITULO_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "DEPARTAMENTO_CODIGO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "DEPARTAMENTO_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "NATUREZAOPERACAO_CODIGO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "NATUREZAOPERACAO_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "CODIGO_NOSSONUMERO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "CODIGO_BANCO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "CODIGO_AGENCIA": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "CODIGO_CONTACORRENTE": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 20},
        "OBSERVACAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 255},
        "NSU": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50},
        "AUTORIZACAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 50}
    },
    "Adiantamento": {
        "CPF_CNPJ": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "TIPO_MOVFINANCEIRO": {"Obrigatorio": true, "Tipo": "Texto", "ValoresPermitidos": ["R", "P"]},
        "CNPJ_EMPRESA": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "TIPO_FICHARAZAO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "DESCRICAO_FICHARAZAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "DATA_MOVIMENTO": {"Obrigatorio": true, "Tipo": "Data"},
        "VALOR_ORIGINAL": {"Obrigatorio": false, "Tipo": "Numerico"},
        "VALOR_SALDO": {"Obrigatorio": true, "Tipo": "Numerico"},
        "OBSERVACAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 255}
    },
    "Fseg_Cab": {
        "CODIGO_VEICULO": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "CPF_CNPJ": {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "CHASSI": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 17},
        "NUMERO_OS": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 50},
        "DATA_ABERTURA": {"Obrigatorio": true, "Tipo": "Data"},
        "KM": {"Obrigatorio": true, "Tipo": "Texto", "TamanhoMax": 17},
        "TIPO_OS_CODIGO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 3},
        "TIPO_OS_DESCRICAO": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 100},
        "OBSERVACAO_OS" : {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 255},
        "CNPJ_EMPRESA" : {"Obrigatorio": true, "Tipo": "CPF_CNPJ"},
        "DATA_LIBERAÇÃO" : {"Obrigatorio": true, "Tipo": "Data"},
        "USUARIO_CONSULTOR": {"Obrigatorio": false, "Tipo": "Texto", "TamanhoMax": 255}
    }
}
"""


def load_layout_configs() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Carrega as configurações de colunas e regras de validação dos layouts
    a partir de strings JSON.

    Returns:
        Tuple contendo dois dicionários:
            - layout_columns: Mapeamento de layouts para colunas.
            - layouts_rules: Mapeamento de layouts para regras de validação.

    Raises:
        Exception em caso de erro de sintaxe JSON.
    """
    try:
        layout_columns = json.loads(LAYOUT_COLUMNS_JSON)
        layouts_rules = json.loads(LAYOUTS_RULES_JSON)
        # Validação básica: garantir que todos os layouts em LAYOUT_COLUMNS tenham regras
        missing_rules = [k for k in layout_columns if k not in layouts_rules]
        if missing_rules:
            logging.warning(f"Layouts sem regras de validação: {missing_rules}")
        logging.info("Configurações de layouts carregadas com sucesso do JSON.")
        return layout_columns, layouts_rules
    except json.JSONDecodeError as e:
        logging.error(
            f"Erro ao carregar configurações dos layouts: {e}. Verifique a sintaxe JSON."
        )
        raise
