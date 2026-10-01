"""Constantes compartilhadas dos blocos De/Para (escopos do dashboard)."""

ESCOPO_PARA_CATEGORIA = {
    "PESSOA": [
        "cond_pag", "escol", "enquadramento", "estado", "estadocivil", "municipio", "pais",
        "profissao", "segmentomercado", "tipologradouro",
    ],
    "PRODUTOS": [
        "clasmontadora", "grupolucratividade", "grupoproduto", "pessoacodfabricante",
        "procedencia", "tabelapreco", "tipoproduto", "unidade",
    ],
    "VEICULOS": [
        "combustivel", "corexterna", "corinterna", "marca", "modeloveiculo",
        "opcional", "setorservico", "tipoos", "tiposervico", "tmo", "veiculoano",
    ],
    "FINANCEIRO": [
        "agentecobrador", "banco", "contagerencial", "tipocobranca",
        "tipocreditodebito", "tipodocumento", "tipoficharazao", "tipotitulo",
    ],
    "CONTABILIDADE": [
        "centroresultado", "historicopadrao", "planoconta", "subconta",
        "tipolote", "tiposubconta",
    ],
    "GERAL": ["departamento", "estoque", "naturezaoperacao", "equipe", "usuario_depara"],
}

CATEGORIAS_NOMES = {
    "PESSOA": "Pessoa",
    "PRODUTOS": "Produto",
    "VEICULOS": "Veículos",
    "FINANCEIRO": "Financeiro",
    "CONTABILIDADE": "Contabilidade",
    "GERAL": "Geral",
}

CATEGORIA_PARA_ESCOPO = {
    categoria: escopo
    for escopo, categorias in ESCOPO_PARA_CATEGORIA.items()
    for categoria in categorias
}

BLOCOS_NOTIFICACAO = tuple(CATEGORIAS_NOMES.keys())

# Layout importado → blocos De/Para que passam a ficar disponíveis
LAYOUT_PARA_BLOCOS = {
    "forn_cli": ("PESSOA",),
    "forn_cli_endereco": ("PESSOA",),
    "forn_cli_dados_bancarios": ("FINANCEIRO",),
    "produto": ("PRODUTOS",),
    "produto_estoque": ("GERAL",),
    "movimento_estoque": ("GERAL",),
    "veiculo": ("VEICULOS",),
    "fseg_cab": ("VEICULOS",),
    "fseg_prd": ("VEICULOS",),
    "fseg_srv": ("VEICULOS",),
    "financeiro": ("FINANCEIRO", "GERAL"),
    "adiantamento": ("FINANCEIRO",),
}


def blocos_do_layout(tipo_layout):
    """Retorna os escopos/blocos afetados por um tipo de layout de importação."""
    if not tipo_layout:
        return ()
    return LAYOUT_PARA_BLOCOS.get(str(tipo_layout).strip().lower(), ())
