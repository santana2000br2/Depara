"""
Importação Financeiro → Arquivo_Financeiro_Tratado → Titulo_MG.

Títulos a pagar/receber. Requer CPF/CNPJ em Pessoa_MG (layout 1 Forn_cli)
e Empresa_DePara (CNPJ_EMPRESA). De/Para: AgenteCobrador, ContaGerencial,
TipoTitulo, Departamento, NaturezaOperacao, Banco.
"""
from logger import logger
from utils.importacao_forn_cli import (
    _normalizar_colunas_dataframe,
    _normalizar_nome_layout,
    _validar_identificador_sql,
    garantir_tabela_staging,
    inserir_staging,
    obter_resumo_importacao,
)
from utils.importacao_procedures import (
    executar_procedure_extracao,
    obter_config_procedure,
)

FINANCEIRO_COLUNAS_CHAVE = frozenset({
    'CPF_CNPJ', 'TIPO_MOVFINANCEIRO', 'CNPJ_EMPRESA', 'TITULO_VALOR', 'TITULO_SALDO',
})

# Colunas que as procedures De/Para leem em Titulo_MG (podem faltar no layout do cliente).
COLUNAS_TITULO_MG_DEPARA = [
    ('AGENTECOBRADOR_CODIGO', 'VARCHAR(MAX) NULL'),
    ('AGENTECOBRADOR_DESCRICAO', 'VARCHAR(MAX) NULL'),
    ('CONTAGERENCIAL_CODIGO', 'VARCHAR(MAX) NULL'),
    ('CONTAGERENCIAL_DESCRICAO', 'VARCHAR(MAX) NULL'),
    ('TIPOTITULO_CODIGO', 'VARCHAR(MAX) NULL'),
    ('TIPOTITULO_DESCRICAO', 'VARCHAR(MAX) NULL'),
    ('DEPARTAMENTO_CODIGO', 'VARCHAR(MAX) NULL'),
    ('DEPARTAMENTO_DESCRICAO', 'VARCHAR(MAX) NULL'),
    ('NATUREZAOPERACAO_CODIGO', 'VARCHAR(MAX) NULL'),
    ('NATUREZAOPERACAO_DESCRICAO', 'VARCHAR(MAX) NULL'),
    ('CODIGO_BANCO', 'VARCHAR(MAX) NULL'),
    ('CODIGO_AGENCIA', 'VARCHAR(MAX) NULL'),
    ('CODIGO_CONTACORRENTE', 'VARCHAR(MAX) NULL'),
]

COLUNAS_CONTAGERENCIAL_DEPARA_EXTRA = [
    ('ContaGerencial_Tipo', 'CHAR(1) NULL'),
    ('ContaGerencial_Nivel', 'CHAR(1) NULL'),
]

TIPO_LAYOUT = 'financeiro'
_cfg = obter_config_procedure(TIPO_LAYOUT) or {}
TABELA_STAGING = _cfg.get('staging', 'Arquivo_Financeiro_Tratado')
TABELA_DESTINO = _cfg.get('destino', 'Titulo_MG')


def _garantir_colunas_depara_financeiro(cursor, banco_gx):
    """Garante colunas usadas pelas procedures De/Para (Titulo_MG + ContaGerencial_DePara)."""
    from utils.importacao_forn_cli import garantir_colunas_extra, _tabela_existe

    garantir_colunas_extra(
        cursor, banco_gx,
        tabela_destino=TABELA_DESTINO,
        colunas_extra=COLUNAS_TITULO_MG_DEPARA,
    )
    if _tabela_existe(cursor, 'ContaGerencial_DePara'):
        garantir_colunas_extra(
            cursor, banco_gx,
            tabela_destino='ContaGerencial_DePara',
            colunas_extra=COLUNAS_CONTAGERENCIAL_DEPARA_EXTRA,
        )


def layout_eh_financeiro(nome_layout, descricao=None, colunas=None):
    """Reconhece layout Financeiro (títulos pagar/receber)."""
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        chave = nome.replace('_', '')
        if 'adiantamento' in chave:
            return False
        if chave in ('financeiro', 'titulo', 'titulos') or 'financeiro' in chave:
            return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        # Aceita nome legado combinado no cadastro de layout
        if 'TITULO_NUMERO/TITULO_SERIE' in nomes:
            nomes.add('TITULO_NUMERO')
        if FINANCEIRO_COLUNAS_CHAVE.issubset(nomes) and 'TIPO_FICHARAZAO' not in nomes:
            return True
        if (
            'TIPO_MOVFINANCEIRO' in nomes
            and 'TITULO_VALOR' in nomes
            and 'TITULO_SALDO' in nomes
            and 'CPF_CNPJ' in nomes
            and 'CNPJ_EMPRESA' in nomes
            and 'TIPO_FICHARAZAO' not in nomes
            and 'DATA_MOVIMENTO' not in nomes
        ):
            return True

    return False


def importar_financeiro_para_base(df, banco_gx, banco_wf=None):
    """Importa DataFrame validado para DadosGX (Titulo_MG)."""
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para importação de Financeiro.", {}

    banco_wf = _validar_identificador_sql(banco_wf.strip())

    if df is None or df.empty:
        return False, "Nenhum dado para importar.", {}

    df = _normalizar_colunas_dataframe(df)
    # Layout legado pode usar nome combinado; a procedure usa TITULO_NUMERO
    rename = {}
    for c in df.columns:
        if str(c).strip().upper() == 'TITULO_NUMERO/TITULO_SERIE':
            rename[c] = 'TITULO_NUMERO'
    if rename:
        df = df.rename(columns=rename)

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return False, f"Não foi possível conectar ao banco {banco_gx}.", {}

    cursor = conn.cursor()
    try:
        from utils.importacao_pessoa_mg_dependencia import validar_dataframe_depende_pessoa_mg

        colunas_layout = [{'Descricao': c} for c in df.columns]
        validar_dataframe_depende_pessoa_mg(
            cursor, df,
            layout_nome=TIPO_LAYOUT,
            colunas_layout=colunas_layout,
        )

        colunas_df = [c for c in df.columns if c not in ('IDtabela', 'Flag')]
        garantir_tabela_staging(cursor, banco_gx, colunas_df, TABELA_STAGING)
        total_inserido = inserir_staging(cursor, banco_gx, df, TABELA_STAGING)
        # Persiste a staging antes da procedure (sp_executesql / legado).
        conn.commit()

        executar_procedure_extracao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)
        # Persiste Titulo_MG antes do De/Para (evita perder o destino no rollback).
        conn.commit()

        _garantir_colunas_depara_financeiro(cursor, banco_gx)
        conn.commit()

        from utils.importacao_depara_procedures import executar_depara_pos_importacao
        resumo_depara = executar_depara_pos_importacao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        resumo['inseridos_staging'] = total_inserido
        resumo['procedure'] = _cfg.get('procedure', 'up_01_Extrai_Financeiro_gx')
        resumo['depara'] = resumo_depara
        msg = (
            f"Importação concluída em {banco_gx}.dbo.{TABELA_DESTINO} "
            f"(procedure {resumo['procedure']}): "
            f"{resumo['total']} registro(s), {resumo['flag_1']} importado(s) OK, "
            f"{resumo['flag_0']} rejeitado(s)."
        )
        return True, msg, resumo
    except Exception as e:
        conn.rollback()
        logger.exception("Erro na importação Financeiro")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
