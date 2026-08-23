"""Particularidades e importação do layout Forn_cli_Telefone."""

from __future__ import annotations

import re

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

TELEFONE_MAX_FONES = 5
# CPF/CNPJ + grupos de 3 (DDD, NUMERO, TIPO) — 1 a 5 telefones
TELEFONE_TAMANHOS_VALIDOS = tuple(1 + 3 * n for n in range(1, TELEFONE_MAX_FONES + 1))

TIPO_LAYOUT = "forn_cli_telefone"
_cfg = obter_config_procedure(TIPO_LAYOUT)
TABELA_STAGING = _cfg["staging"]
TABELA_DESTINO = _cfg["destino"]


def tamanho_telefone_valido(num_campos, num_max=None):
    """True se o registro tem 4, 7, 10, 13 ou 16 campos (1–5 telefones)."""
    if num_max is None:
        num_max = TELEFONE_TAMANHOS_VALIDOS[-1]
    return num_campos in TELEFONE_TAMANHOS_VALIDOS and num_campos <= num_max

INSTRUCAO_TELEFONE = (
    "O layout-base tem 10 campos (CPF_CNPJ + FONE1 + FONE2 + FONE3). "
    "Para telefones adicionais, replique a sequência dos campos 8, 9 e 10 "
    "(DDD_FONE3, NUMERO_FONE3, TIPO_FONE3): "
    "campos 11, 12 e 13 = FONE4; campos 14, 15 e 16 = FONE5 "
    "(máximo de 5 telefones)."
)


def layout_eh_forn_cli_telefone(nome_layout, descricao=None, colunas=None):
    """Reconhece layout Forn_cli_Telefone / 5 Forn_cli_Telefone.txt."""
    for texto in (nome_layout, descricao):
        nome = _normalizar_nome_layout(texto)
        if not nome:
            continue
        if nome == "forn_cli_telefone" or "forn_cli_telefone" in nome:
            return True
        if nome.endswith("_telefone") and "forn_cli" in nome:
            return True

    if colunas:
        nomes = set()
        for c in colunas:
            if isinstance(c, dict):
                nomes.add(str(c.get("Descricao") or "").strip().upper())
            else:
                nomes.add(str(c).strip().upper())
        if "DDD_FONE1" in nomes and "NUMERO_FONE1" in nomes and "CPF_CNPJ" in nomes:
            if "NOME" not in nomes:  # distingue do Forn_cli principal
                return True
    return False


def _indice_fone(descricao: str) -> int | None:
    m = re.search(r"FONE\s*(\d+)", (descricao or "").upper())
    if not m:
        return None
    return int(m.group(1))


def maior_indice_fone_layout(colunas_ordenadas) -> int:
    maior = 0
    for col in colunas_ordenadas or []:
        if isinstance(col, dict):
            desc = col.get("Descricao") or ""
        else:
            desc = str(col)
        idx = _indice_fone(desc)
        if idx:
            maior = max(maior, idx)
    return maior


def expandir_colunas_telefone(colunas_ordenadas):
    """
    Acrescenta grupos DDD/NUMERO/TIPO opcionais até FONE5, se o layout
    cadastrado parar antes (ex.: só até FONE3).
    """
    colunas = [dict(c) if isinstance(c, dict) else {"Descricao": str(c)} for c in (colunas_ordenadas or [])]
    maior = maior_indice_fone_layout(colunas)
    if maior <= 0:
        return colunas

    pos = max(int(c.get("Posicao") or 0) for c in colunas) if colunas else 0
    for n in range(maior + 1, TELEFONE_MAX_FONES + 1):
        for sufixo in ("DDD_FONE", "NUMERO_FONE", "TIPO_FONE"):
            pos += 1
            colunas.append({
                "Posicao": pos,
                "Descricao": f"{sufixo}{n}",
                "Obrigatorio": False,
                "Validacao": "",
                "TipoDado": "texto",
                "_telefone_extra": True,
            })
    return colunas


def validar_extras_telefone(num_base: int, num_arquivo: int, num_max: int):
    """
    Valida se as colunas a mais formam grupos completos de 3 (DDD/NUMERO/TIPO).
    Retorna mensagem de erro ou None se OK.
    """
    if num_arquivo <= num_base:
        return None
    if num_arquivo > num_max:
        return (
            f"O arquivo possui {num_arquivo} colunas, mas o layout de telefone "
            f"aceita no máximo {num_max} (até {TELEFONE_MAX_FONES} telefones). "
            + INSTRUCAO_TELEFONE
        )
    extras = num_arquivo - num_base
    if extras % 3 != 0:
        return (
            f"O arquivo possui {num_arquivo} colunas — as colunas extras de telefone "
            "devem vir em grupos de 3 (DDD_FONEn, NUMERO_FONEn, TIPO_FONEn). "
            + INSTRUCAO_TELEFONE
        )
    return None


def importar_forn_cli_telefone_para_base(df, banco_gx, banco_wf=None, ddd_padrao="11"):
    """
    Importa DataFrame validado para DadosGX.
    Staging → up_05_Extrai_PessoaTelefone_gx → PessoaTelefone_MG.
    """
    from db.connection import conectar_segunda_base

    banco_gx = _validar_identificador_sql(banco_gx.strip())
    ddd = str(ddd_padrao or "11").strip()[:2] or "11"

    if df is None or df.empty:
        return False, "Nenhum dado para importar.", {}

    df = _normalizar_colunas_dataframe(df)

    conn = conectar_segunda_base(banco_gx)
    if not conn:
        return False, f"Não foi possível conectar ao banco {banco_gx}.", {}

    cursor = conn.cursor()
    try:
        from utils.importacao_pessoa_mg_dependencia import validar_dataframe_depende_pessoa_mg
        validar_dataframe_depende_pessoa_mg(
            cursor, df,
            layout_nome=TIPO_LAYOUT,
            colunas_layout=[{"Descricao": c} for c in df.columns],
        )

        colunas_df = [c for c in df.columns if c not in ("IDtabela", "Flag")]
        garantir_tabela_staging(cursor, banco_gx, colunas_df, TABELA_STAGING)
        total_inserido = inserir_staging(cursor, banco_gx, df, TABELA_STAGING)
        conn.commit()

        executar_procedure_extracao(
            cursor, TIPO_LAYOUT, banco_gx, banco_wf, ddd_padrao=ddd,
        )
        conn.commit()

        resumo = obter_resumo_importacao(cursor, banco_gx, TABELA_DESTINO)
        resumo["inseridos_staging"] = total_inserido
        resumo["procedure"] = obter_config_procedure(TIPO_LAYOUT)["procedure"]
        msg = (
            f"Importação concluída em {banco_gx}.dbo.{TABELA_DESTINO} "
            f"(procedure {resumo['procedure']}): "
            f"{resumo['total']} registro(s), {resumo['flag_1']} importado(s) OK, "
            f"{resumo['flag_0']} rejeitado(s)."
        )
        return True, msg, resumo
    except Exception as e:
        conn.rollback()
        logger.exception("Erro na importação Forn_cli_Telefone")
        return False, f"Erro na importação: {e}", {}
    finally:
        cursor.close()
        conn.close()
