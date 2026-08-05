import pandas as pd
import time
import logging
import re
import os
from utils.data_validation import validar_dados


def detectar_layout(filename, layouts_rules_map):
    """
    Detecta o layout do arquivo baseado no nome do arquivo
    """
    if not filename:
        logging.warning("Nome do arquivo está vazio")
        return None
        
    filename_lower = filename.lower()
    logging.info(f"🔍 Detectando layout para arquivo: {filename_lower}")
    
    # CORREÇÃO: Estratégias de detecção mais específicas
    estrategias = [
        # Estratégia 1: Busca direta por layout no nome do arquivo
        lambda f, l: l.lower() in f,
        
        # Estratégia 2: Busca por partes do layout (mais flexível)
        lambda f, l: all(part in f for part in l.lower().split('_')),
        
        # Estratégia 3: Busca por "forn" + "cli" para Forn_cli
        lambda f, l: 'forn' in f and 'cli' in f and l.lower() == 'forn_cli',
        
        # Estratégia 4: Busca por "forn_cli" sem underscore
        lambda f, l: l.lower().replace('_', '') in f.replace('_', ''),
    ]
    
    # Layouts ordenados por prioridade
    layouts_prioridade = ['Forn_cli', 'Forn_cli_Endereco', 'Forn_cli_Documento', 
                         'Produto', 'Veiculo', 'ProdutoEstoque', 'ProdLocacao', 'MovimentoEstoque', 'Financeiro']
    
    for layout in layouts_prioridade:
        for i, estrategia in enumerate(estrategias):
            if estrategia(filename_lower, layout):
                logging.info(f"✅ Layout '{layout}' detectado usando estratégia {i+1} para '{filename}'")
                return layout
    
    # Se não encontrou, tentar com todos os layouts
    for layout in layouts_rules_map.keys():
        for i, estrategia in enumerate(estrategias):
            if estrategia(filename_lower, layout):
                logging.info(f"✅ Layout '{layout}' detectado usando estratégia {i+1} para '{filename}'")
                return layout
    
    logging.warning(f"❌ Nenhum layout detectado para o arquivo: '{filename}'")
    logging.warning(f"📋 Layouts disponíveis: {list(layouts_rules_map.keys())}")
    return None


def processar_arquivo(file, layout, layout_rules, layout_columns):
    file.seek(0)
    try:
        logging.info(f"📖 Lendo arquivo com separador '§'")
        df = pd.read_csv(file, sep="§", encoding="latin-1", header=None, dtype=str)
        
        if df is None or df.empty:
            raise ValueError("Erro ao ler o arquivo ou o arquivo está vazio.")
            
        logging.info(f"✅ Arquivo lido com {len(df)} linhas e {len(df.columns)} colunas")
        
    except Exception as e:
        logging.error(f"❌ Erro ao ler arquivo com separador '§': {e}")
        return (
            None,
            pd.DataFrame(),
            "error",
            f"Erro ao ler o arquivo com separador '§': {str(e)}",
        )

    # Verificar número de colunas e preencher colunas faltantes
    num_cols_expected = len(layout_columns)
    num_cols_actual = df.shape[1]
    
    logging.info(f"📊 Colunas esperadas: {num_cols_expected}, encontradas: {num_cols_actual}")
    
    if num_cols_actual < num_cols_expected:
        logging.warning(
            f"⚠️ Arquivo possui {num_cols_actual} colunas, mas o layout '{layout}' espera {num_cols_expected}. Preenchendo colunas faltantes."
        )
        # Adicionar colunas faltantes com strings vazias
        for i in range(num_cols_actual, num_cols_expected):
            df[f"Column{i}"] = ""
    elif num_cols_actual > num_cols_expected:
        logging.warning(
            f"⚠️ Arquivo possui {num_cols_actual} colunas, mas o layout '{layout}' espera {num_cols_expected}. Ignorando colunas extras."
        )
        df = df.iloc[:, :num_cols_expected]  # Manter apenas as colunas esperadas

    # Renomear colunas para corresponder ao layout
    df.columns = layout_columns[: df.shape[1]]
    logging.info(f"✅ Colunas renomeadas: {list(df.columns)}")

    # Aplicar validação linha por linha
    logging.info("🔍 Iniciando validação dos dados...")
    df_errors = []
    
    for i, (index, row) in enumerate(df.iterrows()):
        erros = validar_dados(row, layout_rules, validar_nao_obrigatorios_flag=True)
        for erro in erros:
            df_errors.append(
                {
                    "Linha": i + 1,  # Linha 1-based para o usuário
                    "Coluna": (
                        erro.split("Campo '")[1].split("'")[0]
                        if "Campo '" in erro
                        else "N/A"
                    ),
                    "Erro": erro,
                }
            )
        
        # Log a cada 1000 linhas para acompanhar progresso
        if (i + 1) % 1000 == 0:
            logging.info(f"📋 Validadas {i + 1} linhas...")

    df_errors = pd.DataFrame(df_errors)
    
    if not df_errors.empty:
        status = "error"
        message = f"Erros encontrados durante a validação. Total: {len(df_errors)} erros."
        logging.warning(f"❌ {message}")
    else:
        status = "success"
        message = "Arquivo processado com sucesso."
        logging.info(f"✅ {message}")

    return df, df_errors, status, message


def run_process_file_wrapper(
    file, layout_columns_map, layouts_rules_map, validar_nao_obrigatorios_flag=True
):
    start = time.time()
    
    logging.info(f"🚀 Iniciando processamento do arquivo: {file.filename}")
    layout = detectar_layout(file.filename, layouts_rules_map)

    if not layout:
        logging.warning(f"❌ Layout não detectado para o arquivo: {file.filename}")
        elapsed = time.time() - start
        return (
            None,
            pd.DataFrame(),
            pd.DataFrame(
                [
                    {
                        "Linha": 0,
                        "Coluna": "N/A",
                        "Erro": f"Layout não detectado para o arquivo **{file.filename}**. Verifique se o nome do arquivo contém o nome do layout (ex: 'Forn_cli').",
                    }
                ]
            ),
            "warning",
            f"⚠️ Layout não detectado para o arquivo: **{file.filename}**.",
            elapsed,
        )

    layout_columns = layout_columns_map.get(layout, [])
    layout_rules = layouts_rules_map.get(layout, {})

    if not layout_columns or not layout_rules:
        logging.error(f"❌ Configuração de layout inválida para: {layout}")
        elapsed = time.time() - start
        return (
            None,
            pd.DataFrame(),
            pd.DataFrame(
                [
                    {
                        "Linha": 0,
                        "Coluna": "N/A",
                        "Erro": f"Configuração de layout inválida para **{layout}**.",
                    }
                ]
            ),
            "error",
            f"❌ Configuração de layout inválida para: **{layout}**.",
            elapsed,
        )

    logging.info(f"✅ Layout '{layout}' configurado com {len(layout_columns)} colunas e {len(layout_rules)} regras")
    
    df, df_errors, status, message = processar_arquivo(
        file, layout, layout_rules, layout_columns
    )

    if df is None:
        elapsed = time.time() - start
        return (
            layout,
            pd.DataFrame(),
            pd.DataFrame(
                [
                    {
                        "Linha": 0,
                        "Coluna": "N/A",
                        "Erro": f"Erro ao processar o arquivo: {message}",
                    }
                ]
            ),
            "error",
            f"❌ Erro ao processar o arquivo: {message}",
            elapsed,
        )

    elapsed = time.time() - start
    logging.info(f"⏰ Processamento concluído em {elapsed:.2f} segundos")
    
    return layout, df, df_errors, status, message, elapsed