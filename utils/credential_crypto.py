"""Criptografia em repouso para senhas de conexão (projeto / SMTP) e login.

Usa Fernet (AES). Chave: CREDENTIALS_KEY no .env (url-safe base64 32 bytes).
Se ausente, deriva de SECRET_KEY (funciona, mas o ideal é chave dedicada).

Valores legados em texto claro continuam legíveis (decrypt transparente).

Para UsuarioNome (login): grava cifra + UsuarioNomeHash (HMAC) para busca no login.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re

from cryptography.fernet import Fernet, InvalidToken

from config import Config
from logger import logger

# Prefixo explícito + tokens Fernet nativos (gAAAAA...)
_PREFIX = "enc1:"
_FERNET_RE = re.compile(r"^gAAAAA[A-Za-z0-9_\-]+=*$")
_fernet = None


def gerar_chave_credentials() -> str:
    """Gera uma CREDENTIALS_KEY nova (para colocar no .env)."""
    return Fernet.generate_key().decode("ascii")


def _build_fernet() -> Fernet:
    raw = (os.getenv("CREDENTIALS_KEY") or "").strip()
    if raw:
        try:
            return Fernet(raw.encode("ascii") if isinstance(raw, str) else raw)
        except Exception as exc:
            logger.error("CREDENTIALS_KEY inválida: %s", exc)
            raise RuntimeError("CREDENTIALS_KEY inválida no .env") from exc

    logger.warning(
        "CREDENTIALS_KEY não definida; derivando de SECRET_KEY. "
        "Defina CREDENTIALS_KEY no .env para produção."
    )
    digest = hashlib.sha256((Config.SECRET_KEY or "fallback").encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        _fernet = _build_fernet()
    return _fernet


def _hmac_key_bytes() -> bytes:
    """Mesma fonte da chave Fernet, usada só para índice de busca do login."""
    raw = (os.getenv("CREDENTIALS_KEY") or "").strip()
    if raw:
        try:
            return base64.urlsafe_b64decode(raw.encode("ascii"))
        except Exception:
            return hashlib.sha256(raw.encode("utf-8")).digest()
    return hashlib.sha256((Config.SECRET_KEY or "fallback").encode("utf-8")).digest()


def esta_criptografado(valor) -> bool:
    if not valor or not isinstance(valor, str):
        return False
    if valor.startswith(_PREFIX):
        return True
    return bool(_FERNET_RE.match(valor.strip()))


def criptografar_segredo(valor: str | None) -> str | None:
    """Criptografa senha/login para gravar no banco. None/vazio permanece None/vazio."""
    if valor is None:
        return None
    texto = str(valor)
    if not texto:
        return ""
    if esta_criptografado(texto):
        return texto
    token = _get_fernet().encrypt(texto.encode("utf-8")).decode("ascii")
    return f"{_PREFIX}{token}"


def descriptografar_segredo(valor: str | None) -> str | None:
    """Devolve texto claro. Valores legados sem cifra são retornados como estão."""
    if valor is None:
        return None
    texto = str(valor)
    if not texto:
        return ""
    if not esta_criptografado(texto):
        return texto

    token = texto[len(_PREFIX):] if texto.startswith(_PREFIX) else texto
    try:
        return _get_fernet().decrypt(token.encode("ascii")).decode("utf-8")
    except InvalidToken:
        logger.error("Falha ao descriptografar segredo (chave errada ou dado corrompido)")
        raise
    except Exception as exc:
        logger.error("Erro ao descriptografar segredo: %s", exc)
        raise


def normalizar_login(nome: str | None) -> str:
    return (nome or "").strip().lower()


def hash_login_busca(nome: str | None) -> str:
    """HMAC-SHA256 do login normalizado — usado no WHERE (não reversível)."""
    normalizado = normalizar_login(nome)
    if not normalizado:
        return ""
    return hmac.new(
        _hmac_key_bytes(),
        normalizado.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def preparar_login_para_banco(nome_claro: str | None) -> tuple[str | None, str]:
    """Retorna (UsuarioNome cifrado, UsuarioNomeHash) para INSERT/UPDATE."""
    if nome_claro is None or not str(nome_claro).strip():
        return None, ""
    claro = str(nome_claro).strip()
    return criptografar_segredo(claro), hash_login_busca(claro)


def revelar_login_banco(valor_banco: str | None) -> str:
    """Descriptografa UsuarioNome do banco (ou devolve legado em claro)."""
    return descriptografar_segredo(valor_banco) or ""
