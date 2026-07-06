"""
Importação Forn_cli_Endereco → Arquivo_Forn_Cli_Endereco_Tratado → PessoaEndereco_MG.

Carga Python na staging + procedure up_02_Extrai_PessoaEndereco_gx + De/Para.
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

FORN_CLI_ENDERECO_COLUNAS_CHAVE = frozenset({
    'CPF_CNPJ', 'ENDERECO', 'CEP', 'TIPO_ENDERECO',
})

TIPO_LAYOUT = 'forn_cli_endereco'
_cfg = obter_config_procedure(TIPO_LAYOUT)
TABELA_STAGING = _cfg['staging']
TABELA_DESTINO = _cfg['destino']


def layout_eh_forn_cli_endereco(nome_layout, descricao=None, colunas=None):
    """
    Reconhece layout Forn_cli_Endereco.
    Aceita nomes como '2 Forn_cli_Endereco', 'Forn_cli_Endereco.txt'.
    """
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        if nome == 'forn_cli_endereco' or 'forn_cli_endereco' in nome:
            return True
        if nome.endswith('_endereco') and 'forn_cli' in nome:
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
            and ('TIPO_ENDERECO' in nomes or 'COD_IBGE' in nomes)
            and 'CODIGO_PESSOA' not in nomes
            and 'NOME' not in nomes
            and 'INSC_ESTADUAL' not in nomes
        ):
            return True
        if FORN_CLI_ENDERECO_COLUNAS_CHAVE.issubset(nomes):
            return True

    return False


def importar_forn_cli_endereco_para_base(df, banco_gx, banco_wf=None):
    """
    Importa DataFrame validado para DadosGX.
    Retorna (sucesso, mensagem, resumo_dict).
    """
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para De/Para de Endereço.", {}

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
        validar_dataframe_depende_pessoa_mg(
            cursor, df,
            layout_nome=TIPO_LAYOUT,
            colunas_layout=[{'Descricao': c} for c in df.columns],
        )

        colunas_df = [c for c in df.columns if c not in ('IDtabela', 'Flag')]
        garantir_tabela_staging(cursor, banco_gx, colunas_df, TABELA_STAGING)
        total_inserido = inserir_staging(cursor, banco_gx, df, TABELA_STAGING)

        executar_procedure_extracao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        from utils.importacao_depara_procedures import executar_depara_pos_importacao
        resumo_depara = executar_depara_pos_importacao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        resumo['inseridos_staging'] = total_inserido
        resumo['procedure'] = obter_config_procedure(TIPO_LAYOUT)['procedure']
        resumo['depara'] = resumo_depara
        msg = (
            f"Importação concluída em {banco_gx}.dbo.{TABELA_DESTINO} "
            f"(procedure {resumo['procedure']}): "
            f"{resumo['total']} registro(s), {resumo['flag_1']} apto(s) (Flag=1), "
            f"{resumo['flag_0']} com ocorrência(s) (Flag=0)."
        )
        return True, msg, resumo
    except Exception as e:
        conn.rollback()
        logger.exception("Erro na importação Forn_cli_Endereco")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
