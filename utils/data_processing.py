import pandas as pd
import time
import logging
import re
from utils.data_validation import validar_dados


def detectar_layout(filename, layouts_rules_map):
    filename_lower = filename.lower()
    # Ordenar layouts por comprimento decrescente para priorizar correspondências mais específicas
    sorted_layouts = sorted(layouts_rules_map.keys(), key=len, reverse=True)
    for layout in sorted_layouts:
        if layout.lower() in filename_lower:
            logging.debug(f"Layout detectado: '{layout}' para o arquivo '{filename}'")
            return layout
    logging.warning(f"Nenhum layout detectado para o arquivo: '{filename}'")
    return None


def processar_arquivo(file, layout, layout_rules, layout_columns):
    file.seek(0)
    try:
        df = pd.read_csv(file, sep="§", encoding="latin-1", header=None, dtype=str)
        if df is None or df.empty:
            raise ValueError("Erro ao ler o arquivo ou o arquivo está vazio.")
    except Exception as e:
        logging.error(f"Erro ao ler arquivo com separador '§': {e}")
        return (
            None,
            pd.DataFrame(),
            "error",
            f"Erro ao ler o arquivo com separador '§'.",
        )

    # Verificar número de colunas e preencher colunas faltantes
    num_cols_expected = len(layout_columns)
    num_cols_actual = df.shape[1]
    if num_cols_actual < num_cols_expected:
        logging.warning(
            f"Arquivo possui {num_cols_actual} colunas, mas o layout '{layout}' espera {num_cols_expected}. Preenchendo colunas faltantes com strings vazias."
        )
        # Adicionar colunas faltantes com strings vazias
        for i in range(num_cols_actual, num_cols_expected):
            df[f"Column{i}"] = ""
    elif num_cols_actual > num_cols_expected:
        logging.warning(
            f"Arquivo possui {num_cols_actual} colunas, mas o layout '{layout}' espera {num_cols_expected}. Ignorando colunas extras."
        )
        df = df.iloc[:, :num_cols_expected]  # Manter apenas as colunas esperadas

    # Renomear colunas para corresponder ao layout
    df.columns = layout_columns[: df.shape[1]]
    logging.debug(f"Colunas do DataFrame após renomeação: {list(df.columns)}")

    # Aplicar validação linha por linha
    df_errors = []
    for index, row in df.iterrows():
        erros = validar_dados(row, layout_rules, validar_nao_obrigatorios_flag=True)
        for erro in erros:
            df_errors.append(
                {
                    "Linha": index + 1,  # Linha 1-based para o usuário
                    "Coluna": (
                        erro.split("Campo '")[1].split("'")[0]
                        if "Campo '" in erro
                        else "N/A"
                    ),
                    "Erro": erro,
                }
            )

    df_errors = pd.DataFrame(df_errors)
    if not df_errors.empty:
        status = "error"
        message = "Erros encontrados durante a validação."
    else:
        status = "success"
        message = "Arquivo processado com sucesso."

    return df, df_errors, status, message


def run_process_file_wrapper(
    file, layout_columns_map, layouts_rules_map, validar_nao_obrigatorios_flag=True
):
    start = time.time()
    layout = detectar_layout(file.filename, layouts_rules_map)

    if not layout:
        logging.warning(f"Layout não detectado para o arquivo: {file.filename}")
        elapsed = time.time() - start
        return (
            None,
            pd.DataFrame(),
            pd.DataFrame(
                [
                    {
                        "Linha": 0,
                        "Coluna": "N/A",
                        "Erro": f"Layout não detectado para o arquivo **{file.filename}**.",
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
        logging.error(f"Configuração de layout inválida para: {layout}")
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
    return layout, df, df_errors, status, message, elapsed
