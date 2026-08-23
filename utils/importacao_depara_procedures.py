"""
Execução das stored procedures De/Para (legado manual → pós-importação automática).

Scripts em procedure/depara/ — instalar no banco DadosGX do projeto.
"""
from logger import logger
from utils.importacao_forn_cli import (
    _executar,
    _executar_procedure,
    _tabela_existe,
    _validar_identificador_sql,
)
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
    'produto': [
        {
            'procedure': 'up_01_Produto_DePara_Unidade',
            'chave': 'unidade',
            'tabelas': [('Unidade_DePara', 'Unidade_Codigo')],
        },
        {
            'procedure': 'up_02_Produto_DePara_TipoProduto',
            'chave': 'tipo_produto',
            'tabelas': [('TipoProduto_DePara', 'TipoProduto_Codigo')],
        },
        {
            'procedure': 'up_03_Produto_DePara_GrupoLucratividade',
            'chave': 'grupo_lucratividade',
            'tabelas': [('GrupoLucratividade_DePara', 'GrupoLucratividade_Codigo')],
        },
        {
            'procedure': 'up_04_Produto_DePara_GrupoProduto',
            'chave': 'grupo_produto',
            'tabelas': [('GrupoProduto_DePara', 'GrupoProduto_Codigo')],
        },
        {
            'procedure': 'up_05_Produto_DePara_Procedencia',
            'chave': 'procedencia',
            'tabelas': [('Procedencia_DePara', 'Procedencia_Codigo')],
        },
        {
            'procedure': 'up_06_Produto_DePara_TabelaPreco',
            'chave': 'tabela_preco',
            'tabelas': [('TabelaPreco_DePara', 'TabelaPreco_Codigo')],
        },
    ],
    'produto_estoque': [
        {
            'procedure': 'up_01_ProdutoEstoque_DePara_Estoque',
            'chave': 'estoque',
            'tabelas': [('Estoque_DePara', 'Estoque_Codigo')],
        },
    ],
    'movimento_estoque': [
        {
            'procedure': 'up_01_MovimentoEstoque_DePara_NaturezaOperacao',
            'chave': 'natureza_operacao',
            'tabelas': [('NaturezaOperacao_DePara', 'NaturezaOperacao_Codigo')],
        },
        {
            'procedure': 'up_02_MovimentoEstoque_DePara_Estoque',
            'chave': 'estoque',
            'tabelas': [('Estoque_DePara', 'Estoque_Codigo')],
        },
        {
            'procedure': 'up_03_MovimentoEstoque_DePara_Departamento',
            'chave': 'departamento',
            'tabelas': [('Departamento_Depara', 'Departamento_Codigo')],
        },
    ],
    'veiculo': [
        {
            'procedure': 'up_01_Veiculo_DePara_ModeloVeiculo',
            'chave': 'modelo_veiculo',
            'tabelas': [('ModeloVeiculo_DePara', 'ModeloVeiculo_Codigo')],
        },
        {
            'procedure': 'up_02_Veiculo_DePara_CorExterna',
            'chave': 'cor_externa',
            'tabelas': [('CorExterna_DePara', 'Cor_Codigo')],
        },
        {
            'procedure': 'up_03_Veiculo_DePara_CorInterna',
            'chave': 'cor_interna',
            'tabelas': [('CorInterna_DePara', 'Cor_Codigo')],
        },
        {
            'procedure': 'up_04_Veiculo_DePara_VeiculoAno',
            'chave': 'veiculo_ano',
            'tabelas': [('VeiculoAno_DePara', 'VeiculoAno_Codigo')],
        },
        {
            'procedure': 'up_05_Veiculo_DePara_Estado',
            'chave': 'estado',
            'tabelas': [('Estado_DePara', 'Estado_Codigo')],
        },
        {
            'procedure': 'up_06_Veiculo_DePara_Municipio',
            'chave': 'municipio',
            'tabelas': [('Municipio_DePara', 'Municipio_Codigo')],
        },
        {
            'procedure': 'up_07_Veiculo_DePara_Marca',
            'chave': 'marca',
            'tabelas': [('Marca_DePara', 'Marca_Codigo')],
        },
    ],
    'fseg_cab': [
        {
            'procedure': 'up_01_Fseg_DePara_TipoOS',
            'chave': 'tipo_os',
            'tabelas': [('TipoOS_DePara', 'TipoOS_Codigo')],
        },
    ],
    'fseg_prd': [
        {
            'procedure': 'up_01_Fseg_DePara_TipoOS',
            'chave': 'tipo_os',
            'tabelas': [('TipoOS_DePara', 'TipoOS_Codigo')],
        },
    ],
    'fseg_srv': [
        {
            'procedure': 'up_01_Fseg_DePara_TipoOS',
            'chave': 'tipo_os',
            'tabelas': [('TipoOS_DePara', 'TipoOS_Codigo')],
        },
    ],
    'financeiro': [
        {
            'procedure': 'up_01_Financeiro_DePara_AgenteCobrador',
            'chave': 'agente_cobrador',
            'tabelas': [('AgenteCobrador_DePara', 'AgenteCobrador_Codigo')],
        },
        {
            'procedure': 'up_02_Financeiro_DePara_ContaGerencial',
            'chave': 'conta_gerencial',
            'tabelas': [('ContaGerencial_DePara', 'ContaGerencial_Codigo')],
        },
        {
            'procedure': 'up_03_Financeiro_DePara_TipoTitulo',
            'chave': 'tipo_titulo',
            'tabelas': [('TipoTitulo_DePara', 'TipoTitulo_Codigo')],
        },
        {
            'procedure': 'up_04_Financeiro_DePara_Departamento',
            'chave': 'departamento',
            'tabelas': [('Departamento_Depara', 'Departamento_Codigo')],
        },
        {
            'procedure': 'up_05_Financeiro_DePara_NaturezaOperacao',
            'chave': 'natureza_operacao',
            'tabelas': [('NaturezaOperacao_DePara', 'NaturezaOperacao_Codigo')],
        },
        {
            'procedure': 'up_06_Financeiro_DePara_Banco',
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
    # Evita travar o único worker do IIS (HTTP 500 no site inteiro).
    timeout_anterior = getattr(cursor, 'timeout', None)
    try:
        cursor.timeout = 90
        _executar_procedure(cursor, f"EXEC dbo.{nome_procedure} ?, ?", (banco_gx, banco_wf))
    finally:
        if timeout_anterior is not None:
            cursor.timeout = timeout_anterior


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
            try:
                _executar_procedure_depara(cursor, proc, banco_gx, banco_wf)
                executadas.add(proc)
                resumo['procedures_executadas'].append(proc)
            except Exception as exc:
                logger.exception(
                    "De/Para dbo.%s falhou — a extração do arquivo é mantida: %s",
                    proc, exc,
                )
                resumo.setdefault('erros', []).append(f"{proc}: {exc}")
                executadas.add(proc)

        stats = {'total': 0, 'vinculados_wf': 0, 'pendentes': 0, 'inseridos': 0, 'atualizados_wf': 0}
        for tabela, col_codigo in cfg['tabelas']:
            parcial = _stats_tabela_depara(cursor, tabela, col_codigo)
            stats['total'] += parcial['total']
            stats['vinculados_wf'] += parcial['vinculados_wf']
            stats['pendentes'] += parcial['pendentes']
            stats['atualizados_wf'] += parcial['vinculados_wf']

        resumo[chave] = stats

    logger.info("De/Para concluído para layout %s: %s", tipo_layout, resumo.get('procedures_executadas'))

    # Marca no dashboard a data em que o bloco passou a ter De/Para disponível
    try:
        from utils.bloco_disponivel import registrar_apos_depara
        registrar_apos_depara(tipo_layout)
    except Exception as exc:
        logger.warning("Não foi possível registrar data de disponibilidade do bloco: %s", exc)

    return resumo
