"""Regras de acesso por tipo de projeto (Arquivo X Workflow / Importação Liberada)."""


def projeto_eh_arquivo_workflow(projeto):
    if not projeto:
        return False
    if projeto.get('TipoArquivoWorkflow'):
        return True
    return projeto.get('TipoProjeto') == 'arquivo_workflow'


def acesso_envio_arquivos(projeto):
    """Menu Envio de arquivos (validador/importação) — projetos Arquivo X Workflow."""
    return projeto_eh_arquivo_workflow(projeto)


def importacao_completa_liberada(projeto):
    """Importação com carga no banco + De/Para só quando liberada (Arquivo X Workflow)."""
    if not projeto_eh_arquivo_workflow(projeto):
        return True
    return bool(projeto.get('ImportacaoLiberada'))
