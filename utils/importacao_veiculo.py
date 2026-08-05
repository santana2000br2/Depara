"""
Importação Veiculo → Arquivo_Veiculo_Tratado → Veiculo_MG.

Carga Python na staging + procedure de extração + 7 De/Para (modelo, cores, ano, UF, município, marca).
"""
from logger import logger
from utils.importacao_forn_cli import (
    _executar,
    _normalizar_colunas_dataframe,
    _normalizar_nome_layout,
    _quote_col,
    _quote_db,
    _quote_table,
    _resolver_coluna,
    _tabela_existe,
    _validar_identificador_sql,
    garantir_colunas_extra,
    garantir_tabela_staging,
    inserir_staging,
    obter_resumo_importacao,
)
from utils.importacao_procedures import (
    executar_procedure_extracao,
    obter_config_procedure,
)

VEICULO_COLUNAS_CHAVE = frozenset({
    'CODIGO_VEICULO', 'CHASSI', 'CNPJ_EMPRESA', 'VEICULO_NOVO',
    'MODELO_CODIGO', 'COR_EXTERNA_CODIGO', 'VEICULO_MARCA_CODIGO',
})

TIPO_LAYOUT = 'veiculo'
_cfg = obter_config_procedure(TIPO_LAYOUT) or {}
TABELA_STAGING = _cfg.get('staging', 'Arquivo_Veiculo_Tratado')
TABELA_DESTINO = _cfg.get('destino', 'Veiculo_MG')

# Colunas extras exigidas pelas procedures De/Para em tabelas De/Para já existentes.
# Garantidas via Python ANTES das procedures: assim a coluna existe na compilação
# do lote (evita "Invalid column name" quando a procedure faz ALTER + uso no mesmo lote).
COLUNAS_EXTRA_DEPARA = {
    'ModeloVeiculo_DePara': [
        ('MARCA_CODIGO', 'nvarchar(510) NULL'),
        ('CODIGO_LINHA', 'nvarchar(510) NULL'),
    ],
}


def _garantir_colunas_depara(cursor, banco_gx):
    """Adiciona colunas extras nas tabelas De/Para existentes antes das procedures."""
    for tabela, colunas in COLUNAS_EXTRA_DEPARA.items():
        if _tabela_existe(cursor, tabela):
            garantir_colunas_extra(
                cursor, banco_gx, tabela_destino=tabela, colunas_extra=colunas,
            )


# Colunas de origem das tabelas De/Para que recebem dados de veículo (marca/modelo/
# cor/ano/UF/município). São estreitas no cadastro original e truncariam valores mais
# largos vindos do arquivo — alargadas para nvarchar(510) (padrão do script original).
LARGURAS_DEPARA = {
    'ModeloVeiculo_DePara': ['mod_cd', 'mod_ds'],
    'Marca_DePara': ['marc_cd', 'marc_ds'],
    'CorExterna_DePara': ['cor_cdext', 'cor_ds'],
    'CorInterna_DePara': ['cor_cd', 'cor_ds'],
    'VeiculoAno_DePara': ['ve_fabmod'],
    'Estado_DePara': ['uf_cd', 'uf_nm'],
    'Municipio_DePara': ['cg_cidade'],
}

_LARGURA_ALVO = 'nvarchar(510)'


def _alargar_colunas_depara(cursor, banco_gx):
    """Alarga colunas de origem das tabelas De/Para para evitar truncamento."""
    db = _quote_db(banco_gx)
    for tabela, colunas in LARGURAS_DEPARA.items():
        if not _tabela_existe(cursor, tabela):
            continue
        tbl = _quote_table(tabela)
        for coluna in colunas:
            real = _resolver_coluna(cursor, tabela, coluna)
            if not real:
                continue
            try:
                _executar(
                    cursor,
                    f"ALTER TABLE {db}.dbo.{tbl} ALTER COLUMN {_quote_col(real)} {_LARGURA_ALVO} NULL",
                )
            except Exception as e:
                logger.warning("Não foi possível alargar %s.%s: %s", tabela, coluna, e)


def layout_eh_veiculo(nome_layout, descricao=None, colunas=None):
    """Reconhece layout Veiculo (cadastro de veículos)."""
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        chave = nome.replace('_', '')
        if chave == 'veiculo' or (nome.startswith('veiculo') and 'veiculoano' not in chave):
            return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get('Descricao') or '').strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        nomes.discard('')
        if 'NUMERO_OS' in nomes and 'CPF_CNPJ' in nomes:
            return False
        if VEICULO_COLUNAS_CHAVE.issubset(nomes):
            return True
        if (
            'CODIGO_VEICULO' in nomes
            and 'CHASSI' in nomes
            and 'CNPJ_EMPRESA' in nomes
            and 'CODIGO_PRODUTO' not in nomes
            and 'MOVIMENTO_CODIGO' not in nomes
        ):
            return True

    return False


def importar_veiculo_para_base(df, banco_gx, banco_wf=None):
    """
    Importa DataFrame validado para DadosGX (Veiculo_MG).
    Retorna (sucesso, mensagem, resumo_dict).
    """
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    if not banco_wf:
        return False, "BancoHomo (BancoWF) é obrigatório para importação de Veículo.", {}

    banco_wf = _validar_identificador_sql(banco_wf.strip())

    if df is None or df.empty:
        return False, "Nenhum dado para importar.", {}

    df = _normalizar_colunas_dataframe(df)

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return False, f"Não foi possível conectar ao banco {banco_gx}.", {}

    cursor = conn.cursor()
    try:
        colunas_df = [c for c in df.columns if c not in ('IDtabela', 'Flag')]
        garantir_tabela_staging(cursor, banco_gx, colunas_df, TABELA_STAGING)
        total_inserido = inserir_staging(cursor, banco_gx, df, TABELA_STAGING)

        executar_procedure_extracao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        _garantir_colunas_depara(cursor, banco_gx)
        _alargar_colunas_depara(cursor, banco_gx)

        from utils.importacao_depara_procedures import executar_depara_pos_importacao
        resumo_depara = executar_depara_pos_importacao(cursor, TIPO_LAYOUT, banco_gx, banco_wf)

        conn.commit()
        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        resumo['inseridos_staging'] = total_inserido
        resumo['procedure'] = _cfg.get('procedure', 'up_01_Extrai_Veiculo_gx')
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
        logger.exception("Erro na importação Veiculo")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
