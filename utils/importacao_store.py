"""Persistência em disco dos resultados de importação (IIS recicla memória entre requests)."""

import json
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from utils.arquivo_processado import ArquivoProcessado, CSV_ENCODING, CSV_SEP, eh_arquivo_processado

logger = logging.getLogger(__name__)

STORE_ROOT = Path(__file__).resolve().parent.parent / "temp" / "importacao_exports"
MAX_AGE_HOURS = 24


def _colunas_layout_para_uso(colunas):
    """Meta grava só os nomes; o restante do código espera dicts com Descricao."""
    result = []
    for i, c in enumerate(colunas or [], start=1):
        if isinstance(c, dict):
            result.append(c)
            continue
        nome = str(c or "").strip()
        if nome:
            result.append({"Descricao": nome, "Posicao": i})
    return result


def _process_dir(process_id: str) -> Path:
    return STORE_ROOT / process_id


def _meta_path(process_id: str) -> Path:
    return _process_dir(process_id) / "meta.json"


def limpar_antigos(max_age_hours: int = MAX_AGE_HOURS) -> None:
    if not STORE_ROOT.exists():
        return

    limite = datetime.now() - timedelta(hours=max_age_hours)
    for pasta in STORE_ROOT.iterdir():
        if not pasta.is_dir():
            continue
        meta_file = pasta / "meta.json"
        try:
            if meta_file.exists():
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                criado = datetime.fromisoformat(meta["timestamp"])
                if criado < limite:
                    shutil.rmtree(pasta, ignore_errors=True)
            elif datetime.fromtimestamp(pasta.stat().st_mtime) < limite:
                shutil.rmtree(pasta, ignore_errors=True)
        except Exception as exc:
            logger.warning("Falha ao limpar exportação %s: %s", pasta.name, exc)


def _amostra_erros(df_erros, limite=8):
    amostra = []
    if df_erros is None or getattr(df_erros, "empty", True):
        return amostra
    for _, row in df_erros.head(limite).iterrows():
        amostra.append({
            "Linha": row.get("Linha", ""),
            "Coluna": row.get("Coluna", ""),
            "Erro": row.get("Erro", ""),
        })
    return amostra


def _flag_erro_estrutura(df_erros, amostra=None):
    from utils.layout_validation import tem_erro_estrutura
    return bool(tem_erro_estrutura(df_erros, amostra))


def salvar_processamento(
    process_id, usuario_id, layout_nome, df_processado, df_erros, df_avisos,
    layout_id=None, layout_descricao=None, colunas_layout=None, mensagem=None,
):
    limpar_antigos()
    pasta = _process_dir(process_id)
    pasta.mkdir(parents=True, exist_ok=True)

    colunas_nomes = []
    if colunas_layout:
        colunas_nomes = [
            c.get('Descricao') if isinstance(c, dict) else str(c)
            for c in colunas_layout
        ]

    meta = {
        "process_id": process_id,
        "usuario_id": usuario_id,
        "layout_nome": layout_nome,
        "layout_id": layout_id,
        "layout_descricao": layout_descricao,
        "colunas_layout": colunas_nomes,
        "timestamp": datetime.now().isoformat(),
        "total_erros": int(getattr(df_erros, "attrs", {}).get("total") or len(df_erros)),
        "total_avisos": int(getattr(df_avisos, "attrs", {}).get("total") or len(df_avisos)),
        "formato": "csv",
        "total_linhas": int(len(df_processado) if df_processado is not None else 0),
        "colunas_processado": list(df_processado.columns) if df_processado is not None else [],
        "mensagem": mensagem or "",
        "amostra_erros": _amostra_erros(df_erros),
        "tem_erro_estrutura": _flag_erro_estrutura(df_erros),
    }
    _meta_path(process_id).write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")

    dest_csv = pasta / "processado.csv"
    if eh_arquivo_processado(df_processado):
        origem = Path(df_processado.caminho)
        if origem.resolve() != dest_csv.resolve():
            shutil.copy2(origem, dest_csv)
            try:
                origem.unlink()
            except OSError:
                pass
            df_processado.caminho = dest_csv
    else:
        df_processado.to_csv(
            dest_csv, sep=CSV_SEP, encoding=CSV_ENCODING, index=False,
        )
    df_erros.to_pickle(pasta / "erros.pkl")
    df_avisos.to_pickle(pasta / "avisos.pkl")


def salvar_importacao_resultado(process_id, usuario_id, importacao_resultado):
    """Persiste resumo da importação automática no meta do processamento."""
    pasta = _process_dir(process_id)
    meta_file = _meta_path(process_id)
    if not meta_file.exists():
        return

    try:
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return

    if str(meta.get("usuario_id")) != str(usuario_id):
        return

    # Só o necessário para UI/API de Flag=0 (sem objetos não serializáveis)
    resumo = (importacao_resultado or {}).get("resumo") or {}
    meta["importacao_resultado"] = {
        "sucesso": bool((importacao_resultado or {}).get("sucesso")),
        "mensagem": (importacao_resultado or {}).get("mensagem") or "",
        "banco_gx": (importacao_resultado or {}).get("banco_gx") or "",
        "tabela_destino": (importacao_resultado or {}).get("tabela_destino") or "",
        "tipo": (importacao_resultado or {}).get("tipo") or "",
        "procedure": (importacao_resultado or {}).get("procedure") or "",
        "resumo_texto": (importacao_resultado or {}).get("resumo_texto") or "",
        "depara_texto": (importacao_resultado or {}).get("depara_texto") or "",
        "flag_0": int(resumo.get("flag_0") or 0),
        "flag_1": int(resumo.get("flag_1") or 0),
        "total": int(resumo.get("total") or 0),
        "soma_estoque": (importacao_resultado or {}).get("soma_estoque") or resumo.get("soma_estoque") or [],
    }
    pasta.mkdir(parents=True, exist_ok=True)
    meta_file.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")


def carregar_processamento(process_id, usuario_id, carregar_detalhes=True):
    pasta = _process_dir(process_id)
    meta_file = _meta_path(process_id)

    if not meta_file.exists():
        return None

    try:
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None

    if str(meta.get("usuario_id")) != str(usuario_id):
        return None

    try:
        csv_path = pasta / "processado.csv"
        pkl_path = pasta / "processado.pkl"
        colunas = meta.get("colunas_processado") or meta.get("colunas_layout") or []
        total_linhas = int(meta.get("total_linhas") or 0)
        if csv_path.exists():
            df_proc = ArquivoProcessado(csv_path, colunas, total_linhas)
        elif pkl_path.exists():
            df_proc = pd.read_pickle(pkl_path)
        else:
            df_proc = pd.DataFrame()

        if carregar_detalhes:
            df_erros = pd.read_pickle(pasta / "erros.pkl")
            df_avisos = pd.read_pickle(pasta / "avisos.pkl")
        else:
            df_erros = pd.DataFrame()
            df_avisos = pd.DataFrame()
        if meta.get("total_erros") is not None:
            df_erros.attrs["total"] = int(meta["total_erros"])
        if meta.get("total_avisos") is not None:
            df_avisos.attrs["total"] = int(meta["total_avisos"])

        return {
            "layout_nome": meta.get("layout_nome", "layout"),
            "layout_id": meta.get("layout_id"),
            "layout_descricao": meta.get("layout_descricao"),
            "colunas_layout": _colunas_layout_para_uso(meta.get("colunas_layout") or []),
            "timestamp": pd.Timestamp(meta.get("timestamp", datetime.now().isoformat())),
            "df_processado": df_proc,
            "df_erros": df_erros,
            "df_avisos": df_avisos,
            "importacao_resultado": meta.get("importacao_resultado"),
            "mensagem": meta.get("mensagem") or "",
            "total_erros": int(meta.get("total_erros") or 0),
            "total_avisos": int(meta.get("total_avisos") or 0),
            "total_linhas": total_linhas,
            "amostra_erros": meta.get("amostra_erros") or [],
            "tem_erro_estrutura": bool(
                meta.get("tem_erro_estrutura")
                or _flag_erro_estrutura(df_erros, meta.get("amostra_erros") or [])
            ),
        }
    except Exception as exc:
        logger.error("Erro ao carregar exportação %s: %s", process_id, exc)
        return None
