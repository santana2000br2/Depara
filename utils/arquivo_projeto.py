"""Armazena arquivos de importação em E:\\Arquivos dos Projetos\\{projeto}\\..."""

from __future__ import annotations

import re
import shutil
from datetime import datetime
from pathlib import Path

from config import Config
from logger import logger

_INVALIDOS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_PONTOS = re.compile(r"\.{2,}")


class CaminhoInvalidoError(ValueError):
    pass


def _raiz() -> Path:
    return Path(Config.ARQUIVOS_PROJETOS_ROOT).resolve()


def sanitizar_nome_pasta(nome: str, fallback: str = "Projeto") -> str:
    texto = _PONTOS.sub(".", str(nome or "").strip())
    texto = texto.replace("/", " ").replace("\\", " ")
    texto = _INVALIDOS.sub("_", texto)
    texto = re.sub(r"\s+", " ", texto).strip(" .")
    if not texto:
        texto = fallback
    return texto[:80]


def sanitizar_nome_arquivo(nome: str) -> str:
    base = Path(str(nome or "arquivo.txt")).name
    base = _PONTOS.sub(".", base)
    base = _INVALIDOS.sub("_", base).strip(" .")
    if not base or base in {".", ".."}:
        base = "arquivo.txt"
    if len(base) > 180:
        sufixo = Path(base).suffix[:12]
        base = base[: 180 - len(sufixo)] + sufixo
    return base


def _garantir_dentro_da_raiz(caminho: Path) -> Path:
    raiz = _raiz()
    resolvido = caminho.resolve()
    try:
        resolvido.relative_to(raiz)
    except ValueError as exc:
        raise CaminhoInvalidoError("Caminho fora de E:\\Arquivos dos Projetos.") from exc
    return resolvido


def pasta_projeto(nome_projeto: str, projeto_id=None) -> Path:
    fallback = f"Projeto_{projeto_id}" if projeto_id else "Projeto"
    pasta = sanitizar_nome_pasta(nome_projeto, fallback=fallback)
    destino = _raiz() / pasta
    destino.mkdir(parents=True, exist_ok=True)
    return _garantir_dentro_da_raiz(destino)


def pasta_importacao(nome_projeto: str, projeto_id=None, quando: datetime | None = None) -> Path:
    agora = quando or datetime.now()
    destino = (
        pasta_projeto(nome_projeto, projeto_id)
        / "Importacoes"
        / f"{agora:%Y}"
        / f"{agora:%m}"
    )
    destino.mkdir(parents=True, exist_ok=True)
    return _garantir_dentro_da_raiz(destino)


def nome_arquivo_fisico(nome_original: str, quando: datetime | None = None) -> str:
    agora = quando or datetime.now()
    original = sanitizar_nome_arquivo(nome_original)
    return f"{agora:%Y%m%d_%H%M%S}_{original}"


def salvar_arquivo_importacao(
    origem,
    nome_original: str,
    nome_projeto: str,
    projeto_id=None,
    quando: datetime | None = None,
) -> dict:
    """
    Copia o arquivo montado para a pasta do projeto.
    Nome físico: AAAAMMDD_HHMMSS_nomeOriginal.ext (não sobrescreve).
    """
    agora = quando or datetime.now()
    pasta = pasta_importacao(nome_projeto, projeto_id, agora)
    nome_orig = sanitizar_nome_arquivo(nome_original)
    nome_fisico = nome_arquivo_fisico(nome_orig, agora)
    destino = pasta / nome_fisico
    seq = 2
    while destino.exists():
        stem = Path(nome_fisico).stem
        sufixo = Path(nome_fisico).suffix
        destino = pasta / f"{stem}_{seq}{sufixo}"
        seq += 1
        if seq > 999:
            raise CaminhoInvalidoError("Não foi possível gerar um nome único para o arquivo.")
    destino = _garantir_dentro_da_raiz(destino)

    origem_path = Path(origem)
    shutil.copy2(str(origem_path), str(destino))
    logger.info(
        "Arquivo salvo: projeto=%s arquivo=%s caminho=%s",
        nome_projeto, nome_orig, destino,
    )
    return {
        "nome_original": nome_orig,
        "nome_fisico": destino.name,
        "caminho": str(destino),
        "pasta_projeto": str(pasta_projeto(nome_projeto, projeto_id)),
    }
