"""
Importação Forn_cli_Enquadramento → Arquivo_Forn_Cli_Enquadramento_Tratado → PessoaEnquadramento_MG.

Carga Python na staging + procedure legada up_04_Extrai_PessoaEnquadramento_gx.
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

FORN_CLI_ENQUADRAMENTO_COLUNAS_CHAVE = frozenset({
    'CPF_CNPJ', 'CONTRIBUICAO_ICMS',
})

TIPO_LAYOUT = 'forn_cli_enquadramento'
_cfg = obter_config_procedure(TIPO_LAYOUT)
TABELA_STAGING = _cfg['staging']
TABELA_DESTINO = _cfg['destino']


def layout_eh_forn_cli_enquadramento(nome_layout, descricao=None, colunas=None):
    """
    Reconhece layout Forn_cli_Enquadramento.
    Aceita nomes como '4 Forn_cli_Enquadramento', 'Forn_cli_Enquadramento.txt'.
    """
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        if nome == 'forn_cli_enquadramento' or 'forn_cli_enquadramento' in nome:
            return True
        if nome.endswith('_enquadramento') and 'forn_cli' in nome:
            return True
        if 'enquadramento' in nome and 'forn_cli' in nome:
            return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        if (
            'CPF_CNPJ' in nomes
            and 'CONTRIBUICAO_ICMS' in nomes
            and 'CODIGO_PESSOA' not in nomes
            and 'NOME' not in nomes
            and 'INSC_ESTADUAL' not in nomes
        ):
            return True
        if FORN_CLI_ENQUADRAMENTO_COLUNAS_CHAVE.issubset(nomes):
            return True

    return False


def importar_forn_cli_enquadramento_para_base(df, banco_gx, banco_wf=None):
    """
    Importa DataFrame validado para DadosGX.
    Retorna (sucesso, mensagem, resumo_dict).
    """
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())

    if df is None or df.empty:
        return False, "Nenhum dado para importar.", {}

    df = _normalizar_colunas_dataframe(df)

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return False, f"Não foi possível conectar ao banco {banco_gx}.", {}

    cursor = conn.cursor()
    try:
        from utils.importacao_pessoa_mg_dependencia import validar_dataframe_depende_pessoa_mg
        validar_dataframe_depende_pessoa_mg(
            cursor, df,
            layout_nome=TIPO_LAYOUT,
            colunas_layout=[{'Descricao': c} for c in df.columns],
        )

        colunas_df = [c for c in df.columns if c not in ('IDtabela', 'Flag')]
        garantir_tabela_staging(cursor, banco_gx, colunas_df, TABELA_STAGING)
        total_inserido = inserir_staging(cursor, banco_gx, df, TABELA_STAGING)

        executar_procedure_extracao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        resumo['inseridos_staging'] = total_inserido
        resumo['procedure'] = obter_config_procedure(TIPO_LAYOUT)['procedure']
        msg = (
            f"Importação concluída em {banco_gx}.dbo.{TABELA_DESTINO} "
            f"(procedure {resumo['procedure']}): "
            f"{resumo['total']} registro(s), {resumo['flag_1']} importado(s) OK, "
            f"{resumo['flag_0']} rejeitado(s)."
        )
        return True, msg, resumo
    except Exception as e:
        conn.rollback()
        logger.exception("Erro na importação Forn_cli_Enquadramento")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
