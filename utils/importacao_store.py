"""Persistência em disco dos resultados de importação (IIS recicla memória entre requests)."""

import json
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

STORE_ROOT = Path(__file__).resolve().parent.parent / "temp" / "importacao_exports"
MAX_AGE_HOURS = 24


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


def salvar_processamento(
    process_id, usuario_id, layout_nome, df_processado, df_erros, df_avisos,
    layout_id=None, layout_descricao=None, colunas_layout=None,
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
    }
    _meta_path(process_id).write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")

    df_processado.to_pickle(pasta / "processado.pkl")
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
    }
    pasta.mkdir(parents=True, exist_ok=True)
    meta_file.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")


def carregar_processamento(process_id, usuario_id):
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
        return {
            "layout_nome": meta.get("layout_nome", "layout"),
            "layout_id": meta.get("layout_id"),
            "layout_descricao": meta.get("layout_descricao"),
            "colunas_layout": meta.get("colunas_layout") or [],
            "timestamp": pd.Timestamp(meta.get("timestamp", datetime.now().isoformat())),
            "df_processado": pd.read_pickle(pasta / "processado.pkl"),
            "df_erros": pd.read_pickle(pasta / "erros.pkl"),
            "df_avisos": pd.read_pickle(pasta / "avisos.pkl"),
            "importacao_resultado": meta.get("importacao_resultado"),
        }
    except Exception as exc:
        logger.error("Erro ao carregar exportação %s: %s", process_id, exc)
        return None
