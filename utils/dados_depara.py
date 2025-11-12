from db.connection import conectar_segunda_base
from logger import logger


def obter_dados_tabela(banco_usuario, nome_tabela, campo_codigo):
    """
    Função genérica para obter dados de qualquer tabela DePara
    """
    if not banco_usuario:
        logger.error(f"Banco do usuário não informado para a tabela {nome_tabela}")
        return {"qtd": 0, "qtdPendente": 0, "percentualConclusao": 0}

    # DEBUG: Log para verificar qual banco e tabela estão sendo acessados
    logger.debug(f"Conectando ao banco {banco_usuario} para tabela {nome_tabela}")

    conexao = conectar_segunda_base(banco_usuario)
    if not conexao:
        logger.error(f"Falha na conexão com o banco {banco_usuario} para a tabela {nome_tabela}")
        return {"qtd": 0, "qtdPendente": 0, "percentualConclusao": 0}

    cursor = None
    try:
        cursor = conexao.cursor()

        # CORREÇÃO: Tratar caso em que fetchone() retorna None
        try:
            # Contar total de registros
            cursor.execute(f"SELECT COUNT(*) FROM {nome_tabela}")
            result = cursor.fetchone()
            qtd = result[0] if result is not None else 0
            logger.debug(f"Tabela {nome_tabela} encontrada com {qtd} registros")
        except Exception as e:
            logger.error(f"Tabela {nome_tabela} não encontrada no banco {banco_usuario}: {e}")
            return {"qtd": 0, "qtdPendente": 0, "percentualConclusao": 0}

        # Contar registros pendentes (S/DePara, NULL ou vazio)
        try:
            cursor.execute(
                f"SELECT COUNT(*) FROM {nome_tabela} WHERE {campo_codigo} = 'S/DePara' OR {campo_codigo} IS NULL OR {campo_codigo} = ''"
            )
            result_pendentes = cursor.fetchone()
            qtdPendente = result_pendentes[0] if result_pendentes is not None else 0
        except Exception as e:
            logger.error(f"Erro ao contar pendentes na tabela {nome_tabela}, campo {campo_codigo}: {e}")
            qtdPendente = 0

        # Calcular percentual
        percentualConclusao = ((qtd - qtdPendente) / qtd * 100) if qtd > 0 else 0

        # DEBUG: Log do resultado
        logger.debug(f"Tabela {nome_tabela}: total={qtd}, pendentes={qtdPendente}, percentual={percentualConclusao}%")

        return {
            "qtd": qtd,
            "qtdPendente": qtdPendente,
            "percentualConclusao": round(percentualConclusao, 1),
        }
    except Exception as e:
        logger.error(f"Erro ao calcular dados {nome_tabela}: {e}")
        return {"qtd": 0, "qtdPendente": 0, "percentualConclusao": 0}
    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception as e:
            logger.error(f"Erro ao fechar cursor: {e}")
        try:
            conexao.close()
        except Exception as e:
            logger.error(f"Erro ao fechar conexão: {e}")


# Funções específicas para cada tabela (mantenha todas as funções existentes)
def dados_condicao_pagamento(banco_usuario):
   return obter_dados_tabela(
        banco_usuario, "CondicaoPagamento_DePara", "CondicaoPagamento_Codigo"
   )
    
 
def dados_escolaridade(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "Escolaridade_DePara", "Escolaridade_Codigo"
    )


def dados_estado(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Estado_DePara", "Estado_Codigo")


def dados_estadocivil(banco_usuario):
    return obter_dados_tabela(banco_usuario, "EstadoCivil_DePara", "EstadoCivil_Codigo")


def dados_municipio(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Municipio_DePara", "Municipio_Codigo")


def dados_pais(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Pais_DePara", "Pais_Codigo")


def dados_profissao(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Profissao_DePara", "Profissao_Codigo")


def dados_segmentomercado(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "SegmentoMercado_DePara", "SegmentoMercado_Codigo"
    )


def dados_tipologradouro(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "TipoLogradouro_DePara", "TipoLogradouro_Codigo"
    )


def dados_departamento(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "Departamento_DePara", "Departamento_Codigo"
    )


def dados_estoque(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Estoque_DePara", "Estoque_Codigo")


def dados_naturezaoperacao(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "NaturezaOperacao_DePara", "NaturezaOperacao_Codigo"
    )


def dados_equipe(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Equipe_DePara", "Equipe_Codigo")


def dados_usuario_depara(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Usuario_DePara", "Usuario_Codigo")


def dados_clasmontadora(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "ClasMontadora_DePara", "ClasMontadora_Codigo"
    )


def dados_grupolucratividade(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "GrupoLucratividade_DePara", "GrupoLucratividade_Codigo"
    )


def dados_grupoproduto(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "GrupoProduto_DePara", "GrupoProduto_Codigo"
    )


def dados_pessoacodfabricante(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "PessoaCodFabricante_DePara", "PessoaCodFabricante_Codigo"
    )


def dados_procedencia(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Procedencia_DePara", "Procedencia_Codigo")


def dados_tabelapreco(banco_usuario):
    return obter_dados_tabela(banco_usuario, "TabelaPreco_DePara", "TabelaPreco_Codigo")


def dados_tipoproduto(banco_usuario):
    return obter_dados_tabela(banco_usuario, "TipoProduto_DePara", "TipoProduto_Codigo")


def dados_unidade(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Unidade_DePara", "Unidade_Codigo")


def dados_combustivel(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Combustivel_DePara", "Combustivel_Codigo")


def dados_corexterna(banco_usuario):
    return obter_dados_tabela(banco_usuario, "CorExterna_DePara", "Cor_Codigo")


def dados_corinterna(banco_usuario):
    return obter_dados_tabela(banco_usuario, "CorInterna_DePara", "Cor_Codigo")


def dados_marca(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Marca_DePara", "Marca_Codigo")


def dados_modeloveiculo(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "ModeloVeiculo_DePara", "ModeloVeiculo_Codigo"
    )


def dados_opcional(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Opcional_DePara", "Opcional_Codigo")


def dados_setorservico(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "SetorServico_DePara", "SetorServico_Codigo"
    )


def dados_tipoos(banco_usuario):
    return obter_dados_tabela(banco_usuario, "TipoOS_DePara", "TipoOS_Codigo")


def dados_tiposervico(banco_usuario):
    return obter_dados_tabela(banco_usuario, "TipoServico_DePara", "TipoServico_Codigo")


def dados_tmo(banco_usuario):
    return obter_dados_tabela(banco_usuario, "TMO_DePara", "TMO_Codigo")


def dados_veiculoano(banco_usuario):
    return obter_dados_tabela(banco_usuario, "VeiculoAno_DePara", "VeiculoAno_Codigo")


def dados_agentecobrador(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "AgenteCobrador_DePara", "AgenteCobrador_Codigo"
    )


def dados_banco(banco_usuario):
    return obter_dados_tabela(banco_usuario, "Banco_DePara", "Banco_Codigo")


def dados_contagerencial(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "ContaGerencial_DePara", "ContaGerencial_Codigo"
    )


def dados_tipocobranca(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "TipoCobranca_DePara", "TipoCobranca_Codigo"
    )


def dados_tipocreditodebito(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "TipoCreditoDebito_DePara", "TipoCreditoDebito_Codigo"
    )


def dados_tipodocumento(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "TipoDocumento_DePara", "TipoDocumento_Codigo"
    )


def dados_tipoficharazao(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "TipoFichaRazao_DePara", "TipoFichaRazao_Codigo"
    )


def dados_tipotitulo(banco_usuario):
    return obter_dados_tabela(banco_usuario, "TipoTitulo_DePara", "TipoTitulo_Codigo")


def dados_centroresultado(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "CentroResultado_DePara", "CentroResultado_Codigo"
    )


def dados_historicopadrao(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "HistoricoPadrao_DePara", "HistoricoPadrao_Codigo"
    )


def dados_planoconta(banco_usuario):
    return obter_dados_tabela(banco_usuario, "PlanoConta_DePara", "PlanoConta_Codigo")


def dados_subconta(banco_usuario):
    return obter_dados_tabela(banco_usuario, "SubConta_DePara", "SubConta_Codigo")


def dados_tipolote(banco_usuario):
    return obter_dados_tabela(banco_usuario, "TipoLote_DePara", "TipoLote_Codigo")


def dados_tiposubconta(banco_usuario):
    return obter_dados_tabela(
        banco_usuario, "TipoSubConta_DePara", "TipoSubConta_Codigo"
    )
    
