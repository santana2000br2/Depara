"""Validação Google reCAPTCHA v2 no servidor."""

from __future__ import annotations

import urllib.parse
import urllib.request
import json

from config import Config
from logger import logger


def recaptcha_habilitado() -> bool:
    return bool(
        (Config.RECAPTCHA_SITE_KEY or "").strip()
        and (Config.RECAPTCHA_SECRET_KEY or "").strip()
    )


def validar_recaptcha(token: str | None, remote_ip: str | None = None) -> bool:
    """Valida o token g-recaptcha-response com a API do Google."""
    if not recaptcha_habilitado():
        return True

    token = (token or "").strip()
    if not token:
        return False

    payload = {
        "secret": Config.RECAPTCHA_SECRET_KEY,
        "response": token,
    }
    if remote_ip:
        payload["remoteip"] = remote_ip

    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://www.google.com/recaptcha/api/siteverify",
        data=data,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        ok = bool(body.get("success"))
        if not ok:
            logger.warning("reCAPTCHA rejeitado: %s", body.get("error-codes"))
        return ok
    except Exception as exc:
        logger.error("Falha ao validar reCAPTCHA: %s", exc)
        # Em falha de rede, não libera o login
        return False
