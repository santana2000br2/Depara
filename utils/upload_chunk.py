"""Upload em partes para não estourar o limite do IIS (HTTP 413)."""

from __future__ import annotations

import json
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UPLOAD_ROOT = ROOT / "temp" / "upload_chunks"

# Cada POST fica bem abaixo do teto do IIS (inclusive o default de ~28 MB).
CHUNK_SIZE = 8 * 1024 * 1024
MAX_TOTAL = 1073741824  # 1 GB
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)


class UploadChunkError(ValueError):
    pass


def _pasta(upload_id: str) -> Path:
    if not upload_id or not _UUID_RE.match(str(upload_id)):
        raise UploadChunkError("Sessão de envio inválida.")
    return UPLOAD_ROOT / str(upload_id)


def _ler_meta(pasta: Path) -> dict:
    meta_path = pasta / "meta.json"
    if not meta_path.exists():
        raise UploadChunkError("Sessão de envio expirada. Envie o arquivo novamente.")
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise UploadChunkError("Sessão de envio corrompida. Envie o arquivo novamente.") from exc


def _gravar_meta(pasta: Path, meta: dict) -> None:
    pasta.mkdir(parents=True, exist_ok=True)
    (pasta / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False),
        encoding="utf-8",
    )


def _conferir_usuario(meta: dict, usuario_id) -> None:
    if str(meta.get("usuario_id")) != str(usuario_id):
        raise UploadChunkError("Sessão de envio não pertence a este usuário.")


def iniciar_sessao(usuario_id, filename, tamanho) -> dict:
    try:
        tamanho = int(tamanho or 0)
    except (TypeError, ValueError) as exc:
        raise UploadChunkError("Tamanho do arquivo inválido.") from exc
    if tamanho <= 0:
        raise UploadChunkError("Arquivo vazio.")
    if tamanho > MAX_TOTAL:
        raise UploadChunkError("Arquivo maior que 1 GB.")

    nome = Path(str(filename or "arquivo.txt")).name
    if not nome.lower().endswith((".txt", ".csv")):
        nome = f"{nome}.txt"

    total_chunks = max(1, (tamanho + CHUNK_SIZE - 1) // CHUNK_SIZE)
    upload_id = str(uuid.uuid4())
    pasta = _pasta(upload_id)
    pasta.mkdir(parents=True, exist_ok=True)
    meta = {
        "upload_id": upload_id,
        "usuario_id": usuario_id,
        "filename": nome,
        "tamanho": tamanho,
        "chunk_size": CHUNK_SIZE,
        "total_chunks": total_chunks,
        "recebidos": [],
        "created": datetime.now().isoformat(),
    }
    _gravar_meta(pasta, meta)
    return {
        "upload_id": upload_id,
        "chunk_size": CHUNK_SIZE,
        "total_chunks": total_chunks,
        "tamanho": tamanho,
    }


def gravar_chunk(upload_id, usuario_id, indice, stream) -> dict:
    pasta = _pasta(upload_id)
    meta = _ler_meta(pasta)
    _conferir_usuario(meta, usuario_id)

    try:
        indice = int(indice)
    except (TypeError, ValueError) as exc:
        raise UploadChunkError("Índice da parte inválido.") from exc

    total = int(meta.get("total_chunks") or 0)
    if indice < 0 or indice >= total:
        raise UploadChunkError("Parte fora da sequência do arquivo.")

    dest = pasta / f"part_{indice:05d}.bin"
    escrito = 0
    with dest.open("wb") as out:
        while True:
            bloco = stream.read(1024 * 1024)
            if not bloco:
                break
            escrito += len(bloco)
            if escrito > CHUNK_SIZE:
                dest.unlink(missing_ok=True)
                raise UploadChunkError("Parte maior que o permitido.")
            out.write(bloco)

    if escrito <= 0:
        dest.unlink(missing_ok=True)
        raise UploadChunkError("Parte vazia.")

    recebidos = set(int(x) for x in (meta.get("recebidos") or []))
    recebidos.add(indice)
    meta["recebidos"] = sorted(recebidos)
    _gravar_meta(pasta, meta)
    return {
        "indice": indice,
        "recebidos": len(recebidos),
        "total_chunks": total,
    }


def montar_arquivo(upload_id, usuario_id) -> Path:
    pasta = _pasta(upload_id)
    meta = _ler_meta(pasta)
    _conferir_usuario(meta, usuario_id)

    total = int(meta.get("total_chunks") or 0)
    recebidos = set(int(x) for x in (meta.get("recebidos") or []))
    faltando = [i for i in range(total) if i not in recebidos]
    if faltando:
        raise UploadChunkError(
            f"Envio incompleto: faltam {len(faltando)} parte(s) do arquivo."
        )

    destino = pasta / "completo.txt"
    esperado = int(meta.get("tamanho") or 0)
    escrito = 0
    with destino.open("wb") as out:
        for i in range(total):
            parte = pasta / f"part_{i:05d}.bin"
            if not parte.exists():
                raise UploadChunkError("Uma das partes do arquivo não foi encontrada.")
            with parte.open("rb") as inp:
                shutil.copyfileobj(inp, out, length=1024 * 1024)
            escrito += parte.stat().st_size

    if esperado and escrito != esperado:
        destino.unlink(missing_ok=True)
        raise UploadChunkError(
            "O arquivo montado não confere com o tamanho original. Envie novamente."
        )
    return destino


def nome_arquivo(upload_id, usuario_id) -> str:
    pasta = _pasta(upload_id)
    meta = _ler_meta(pasta)
    _conferir_usuario(meta, usuario_id)
    return meta.get("filename") or "arquivo.txt"


def limpar_sessao(upload_id) -> None:
    try:
        pasta = _pasta(upload_id)
    except UploadChunkError:
        return
    shutil.rmtree(pasta, ignore_errors=True)
