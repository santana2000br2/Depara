"""
Importação Fseg_Cab → Arquivo_FSeg_Cab_Tratado → Ficha_Cab_MG.

Cabeçalho de OS (ficha de seguimento). Requer Pessoa_MG (CPF/CNPJ) e
Veiculo_MG (CHASSI). De/Para: TipoOS (pode agregar também Ficha_Prd/Srv quando existirem).
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

FSEG_CAB_COLUNAS_CHAVE = frozenset({
    'CHASSI', 'NUMERO_OS', 'CPF_CNPJ', 'CNPJ_EMPRESA', 'TIPO_OS_CODIGO',
})

TIPO_LAYOUT = 'fseg_cab'
_cfg = obter_config_procedure(TIPO_LAYOUT) or {}
TABELA_STAGING = _cfg.get('staging', 'Arquivo_FSeg_Cab_Tratado')
TABELA_DESTINO = _cfg.get('destino', 'Ficha_Cab_MG')


def layout_eh_fseg_cab(nome_layout, descricao=None, colunas=None):
    """Reconhece layout 13 Fseg_Cab (cabeçalho de OS)."""
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        chave = nome.replace('_', '')
        if 'fsegprd' in chave or 'fsegsrv' in chave or 'fichaprd' in chave or 'fichasrv' in chave:
            return False
        if chave in ('fsegcab', 'fichacab', 'fseg_cab') or 'fsegcab' in chave or 'fichacab' in chave:
            return True
        if 'fseg' in chave and 'cab' in chave:
            return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        # Normaliza acento em DATA_LIBERACAO
        nomes = {n.replace('DATA_LIBERAÇÃO', 'DATA_LIBERACAO') for n in nomes}
        if FSEG_CAB_COLUNAS_CHAVE.issubset(nomes):
            return True
        if (
            'NUMERO_OS' in nomes
            and 'CHASSI' in nomes
            and 'CPF_CNPJ' in nomes
            and 'TIPO_OS_CODIGO' in nomes
            and 'DATA_ABERTURA' in nomes
            and 'CODIGO_PRODUTO' not in nomes
            and 'VEICULO_NOVO' not in nomes
            and 'PRODUTO_REFERENCIA' not in nomes
            and 'PRODUTO_QUANTIDADE' not in nomes
        ):
            return True

    return False


def importar_fseg_cab_para_base(df, banco_gx, banco_wf=None):
    """Importa DataFrame validado para DadosGX (Ficha_Cab_MG)."""
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para importação de Fseg_Cab.", {}

    banco_wf = _validar_identificador_sql(banco_wf.strip())

    if df is None or df.empty:
        return False, "Nenhum dado para importar.", {}

    df = _normalizar_colunas_dataframe(df)
    # Layout legado pode trazer acento; a procedure usa DATA_LIBERACAO
    rename = {}
    for c in df.columns:
        if str(c).strip().upper() in ('DATA_LIBERAÇÃO', 'DATA_LIBERACAO'):
            rename[c] = 'DATA_LIBERACAO'
    if rename:
        df = df.rename(columns=rename)

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return False, f"Não foi possível conectar ao banco {banco_gx}.", {}

    cursor = conn.cursor()
    try:
        from utils.importacao_pessoa_mg_dependencia import validar_dataframe_depende_pessoa_mg
        from utils.importacao_veiculo_mg_dependencia import validar_dataframe_depende_veiculo_mg

        colunas_layout = [{'Descricao': c} for c in df.columns]
        validar_dataframe_depende_pessoa_mg(
            cursor, df,
            layout_nome=TIPO_LAYOUT,
            colunas_layout=colunas_layout,
        )
        validar_dataframe_depende_veiculo_mg(
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
        resumo['procedure'] = _cfg.get('procedure', 'up_01_Extrai_Fseg_Cab_gx')
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
        logger.exception("Erro na importação Fseg_Cab")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
