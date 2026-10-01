"""
Importação Adiantamento → Arquivo_Adiantamento_Tratado → FichaRazao_MG.

Adiantamentos a pagar/receber. Requer CPF/CNPJ em Pessoa_MG (layout 1 Forn_cli)
e Empresa_DePara (CNPJ_EMPRESA). De/Para: TipoFichaRazao.
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

ADIANTAMENTO_COLUNAS_CHAVE = frozenset({
    'CPF_CNPJ', 'TIPO_MOVFINANCEIRO', 'CNPJ_EMPRESA',
    'TIPO_FICHARAZAO', 'VALOR_SALDO',
})

# Colunas extras do script de origem (ALTER após o SELECT INTO da FichaRazao_MG).
COLUNAS_ADIANTAMENTO_MG_EXTRA = [
    ('SALDO_Original', 'float NULL'),
    ('Pessoa_DocIdentificador', 'varchar(20) NULL'),
    ('TipoFichaRazao_Codigo', 'int NULL'),
    ('FichaRazao_PessoaCod', 'int NULL'),
    ('FichaRazao_EmpresaCod', 'int NULL'),
    ('Ocorrencia', 'varchar(500) NULL'),
]

TIPO_LAYOUT = 'adiantamento'
_cfg = obter_config_procedure(TIPO_LAYOUT) or {}
TABELA_STAGING = _cfg.get('staging', 'Arquivo_Adiantamento_Tratado')
TABELA_DESTINO = _cfg.get('destino', 'FichaRazao_MG')


def layout_eh_adiantamento(nome_layout, descricao=None, colunas=None):
    """Reconhece layout Adiantamento (não confundir com Financeiro/Título)."""
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        chave = nome.replace('_', '')
        if 'adiantamento' in chave:
            return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        if ADIANTAMENTO_COLUNAS_CHAVE.issubset(nomes):
            return True
        if (
            'TIPO_FICHARAZAO' in nomes
            and 'VALOR_SALDO' in nomes
            and 'CPF_CNPJ' in nomes
            and 'CNPJ_EMPRESA' in nomes
            and 'TITULO_VALOR' not in nomes
            and 'TITULO_SALDO' not in nomes
        ):
            return True

    return False


def importar_adiantamento_para_base(df, banco_gx, banco_wf=None):
    """Importa DataFrame validado para DadosGX (FichaRazao_MG)."""
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para importação de Adiantamento.", {}

    banco_wf = _validar_identificador_sql(banco_wf.strip())

    if df is None or df.empty:
        return False, "Nenhum dado para importar.", {}

    df = _normalizar_colunas_dataframe(df)

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
        conn.commit()

        executar_procedure_extracao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)
        conn.commit()

        from utils.importacao_forn_cli import garantir_colunas_extra
        garantir_colunas_extra(
            cursor, banco_gx,
            tabela_destino=TABELA_DESTINO,
            colunas_extra=COLUNAS_ADIANTAMENTO_MG_EXTRA,
        )
        conn.commit()

        from utils.importacao_depara_procedures import executar_depara_pos_importacao
        resumo_depara = executar_depara_pos_importacao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        resumo['inseridos_staging'] = total_inserido
        resumo['procedure'] = _cfg.get('procedure', 'up_01_Extrai_Adiantamento_gx')
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
        logger.exception("Erro na importação Adiantamento")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
