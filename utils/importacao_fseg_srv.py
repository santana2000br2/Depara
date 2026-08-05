"""
Importação Fseg_Srv → Arquivo_FSeg_Srv_Tratado → Ficha_Srv_MG.

Itens de serviço (TMO) da OS. Requer:
- CHASSI em Ficha_Cab_MG (layout 13 Fseg_Cab)

De/Para: TipoOS (agrega Cab/Prd/Srv quando existirem).
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

FSEG_SRV_COLUNAS_CHAVE = frozenset({
    'CHASSI', 'NUMERO_OS', 'TMO_REFERENCIA', 'CNPJ_EMPRESA', 'TMO_QUANTIDADE',
})

TIPO_LAYOUT = 'fseg_srv'
_cfg = obter_config_procedure(TIPO_LAYOUT) or {}
TABELA_STAGING = _cfg.get('staging', 'Arquivo_FSeg_Srv_Tratado')
TABELA_DESTINO = _cfg.get('destino', 'Ficha_Srv_MG')


def layout_eh_fseg_srv(nome_layout, descricao=None, colunas=None):
    """Reconhece layout Fseg_Srv (serviços/TMO da OS)."""
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        chave = nome.replace('_', '')
        if 'fsegprd' in chave or 'fichaprd' in chave or 'fsegcab' in chave or 'fichacab' in chave:
            return False
        if chave in ('fsegsrv', 'fichasrv', 'fseg_srv') or 'fsegsrv' in chave or 'fichasrv' in chave:
            return True
        if 'fseg' in chave and 'srv' in chave:
            return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        if FSEG_SRV_COLUNAS_CHAVE.issubset(nomes):
            return True
        if (
            'NUMERO_OS' in nomes
            and 'CHASSI' in nomes
            and 'TMO_REFERENCIA' in nomes
            and 'TMO_QUANTIDADE' in nomes
            and 'CNPJ_EMPRESA' in nomes
            and 'CPF_CNPJ' not in nomes
            and 'DATA_ABERTURA' not in nomes
            and 'PRODUTO_REFERENCIA' not in nomes
        ):
            return True

    return False


def importar_fseg_srv_para_base(df, banco_gx, banco_wf=None):
    """Importa DataFrame validado para DadosGX (Ficha_Srv_MG)."""
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para importação de Fseg_Srv.", {}

    banco_wf = _validar_identificador_sql(banco_wf.strip())

    if df is None or df.empty:
        return False, "Nenhum dado para importar.", {}

    df = _normalizar_colunas_dataframe(df)

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return False, f"Não foi possível conectar ao banco {banco_gx}.", {}

    cursor = conn.cursor()
    try:
        from utils.importacao_ficha_cab_mg_dependencia import validar_dataframe_depende_ficha_cab_mg

        colunas_layout = [{'Descricao': c} for c in df.columns]
        validar_dataframe_depende_ficha_cab_mg(
            cursor, df,
            layout_nome=TIPO_LAYOUT,
            colunas_layout=colunas_layout,
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
        resumo['procedure'] = _cfg.get('procedure', 'up_01_Extrai_Fseg_Srv_gx')
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
        logger.exception("Erro na importação Fseg_Srv")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
