"""
Execução das stored procedures De/Para (legado manual → pós-importação automática).

Scripts em procedure/depara/ — instalar no banco DadosGX do projeto.
"""
from logger import logger
from utils.importacao_forn_cli import _executar, _tabela_existe, _validar_identificador_sql
from utils.importacao_procedures import procedure_existe

SEM_DEPARA = 'S/DePara'

# Layout → procedures a executar após importação bem-sucedida
DEPARA_POR_LAYOUT = {
    'forn_cli': [
        {
            'procedure': 'up_01_Pessoa_DePara_SegmentoMercado',
            'chave': 'segmento_mercado',
            'tabelas': [('SegmentoMercado_DePara', 'SegmentoMercado_Codigo')],
        },
        {
            'procedure': 'up_02_Pessoa_DePara_Escolaridade',
            'chave': 'escolaridade',
            'tabelas': [('Escolaridade_DePara', 'Escolaridade_Codigo')],
        },
        {
            'procedure': 'up_03_Pessoa_DePara_Profissao',
            'chave': 'profissao',
            'tabelas': [('Profissao_DePara', 'Profissao_Codigo')],
        },
        {
            'procedure': 'up_04_Pessoa_DePara_EstadoCivil',
            'chave': 'estado_civil',
            'tabelas': [('EstadoCivil_DePara', 'EstadoCivil_Codigo')],
        },
    ],
    'forn_cli_endereco': [
        {
            'procedure': 'up_05_Pessoa_DePara_Municipio',
            'chave': 'municipio',
            'tabelas': [('Municipio_DePara', 'Municipio_Codigo')],
        },
        {
            'procedure': 'up_06_Pessoa_DePara_TipoLogradouro',
            'chave': 'tipo_logradouro',
            'tabelas': [('TipoLogradouro_DePara', 'TipoLogradouro_Codigo')],
        },
        {
            'procedure': 'up_07_Pessoa_DePara_Estado_Pais',
            'chave': 'estado',
            'tabelas': [('Estado_DePara', 'Estado_Codigo')],
        },
        {
            'procedure': 'up_07_Pessoa_DePara_Estado_Pais',
            'chave': 'pais',
            'tabelas': [('Pais_DePara', 'Pais_Codigo')],
            'somente_stats': True,
        },
    ],
    'forn_cli_dados_bancarios': [
        {
            'procedure': 'up_08_Pessoa_DePara_Banco',
            'chave': 'banco',
            'tabelas': [('Banco_DePara', 'Banco_Codigo')],
        },
    ],
}


def obter_depara_layout(tipo_layout):
    return DEPARA_POR_LAYOUT.get(tipo_layout, [])


def layout_tem_depara(tipo_layout):
    return bool(obter_depara_layout(tipo_layout))


def _stats_tabela_depara(cursor, tabela, col_codigo):
    if not _tabela_existe(cursor, tabela):
        return {'total': 0, 'vinculados_wf': 0, 'pendentes': 0, 'inseridos': 0, 'atualizados_wf': 0}

    cursor.execute(f"SELECT COUNT(*) FROM dbo.[{tabela}]")
    total = cursor.fetchone()[0] or 0

    cursor.execute(
        f"SELECT COUNT(*) FROM dbo.[{tabela}] WHERE [{col_codigo}] = ?",
        (SEM_DEPARA,),
    )
    pendentes = cursor.fetchone()[0] or 0

    vinculados = total - pendentes
    return {
        'total': total,
        'vinculados_wf': vinculados,
        'pendentes': pendentes,
        'inseridos': 0,
        'atualizados_wf': vinculados,
    }


def _executar_procedure_depara(cursor, nome_procedure, banco_gx, banco_wf):
    if not procedure_existe(cursor, nome_procedure):
        raise RuntimeError(
            f"Procedure dbo.{nome_procedure} não encontrada. "
            "Instale os scripts em procedure/depara/ no banco DadosGX."
        )
    logger.info("Executando De/Para dbo.%s (@BancoDadosGX=%s, @BancoWF=%s)", nome_procedure, banco_gx, banco_wf)
    _executar(cursor, f"EXEC dbo.{nome_procedure} ?, ?", (banco_gx, banco_wf))


def executar_depara_pos_importacao(cursor, tipo_layout, banco_gx, banco_wf):
    """
    Executa procedures De/Para configuradas para o layout importado.
    Retorna resumo por tabela (compatível com formatar_resumo_depara).
    """
    configs = obter_depara_layout(tipo_layout)
    if not configs:
        return {}

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    banco_wf = _validar_identificador_sql(banco_wf.strip())

    resumo = {'via_procedure': True, 'procedures_executadas': []}
    executadas = set()

    for cfg in configs:
        proc = cfg['procedure']
        chave = cfg['chave']

        if not cfg.get('somente_stats') and proc not in executadas:
            _executar_procedure_depara(cursor, proc, banco_gx, banco_wf)
            executadas.add(proc)
            resumo['procedures_executadas'].append(proc)

        stats = {'total': 0, 'vinculados_wf': 0, 'pendentes': 0, 'inseridos': 0, 'atualizados_wf': 0}
        for tabela, col_codigo in cfg['tabelas']:
            parcial = _stats_tabela_depara(cursor, tabela, col_codigo)
            stats['total'] += parcial['total']
            stats['vinculados_wf'] += parcial['vinculados_wf']
            stats['pendentes'] += parcial['pendentes']
            stats['atualizados_wf'] += parcial['vinculados_wf']

        resumo[chave] = stats

    logger.info("De/Para concluído para layout %s: %s", tipo_layout, resumo.get('procedures_executadas'))
    return resumo
