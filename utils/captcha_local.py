"""Captcha local com imagem (texto distorcido) — sem dependência externa."""

from __future__ import annotations

import base64
import io
import random
import secrets

from flask import session
from PIL import Image, ImageDraw, ImageFont, ImageFilter


_SESSION_ANSWER = "_captcha_answer"
_SESSION_TOKEN = "_captcha_token"

# Evita confusão visual: sem 0/O, 1/I/l
_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def _fonte(tamanho: int = 36):
    candidatos = [
        "C:/Windows/Fonts/arialbd.ttf",
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/consolab.ttf",
        "C:/Windows/Fonts/consola.ttf",
    ]
    for path in candidatos:
        try:
            return ImageFont.truetype(path, tamanho)
        except OSError:
            continue
    return ImageFont.load_default()


def _gerar_imagem(texto: str) -> str:
    """Gera PNG em data-URI com ruído e caracteres inclinados."""
    largura, altura = 180, 60
    img = Image.new("RGB", (largura, altura), (245, 247, 250))
    draw = ImageDraw.Draw(img)
    fonte = _fonte(34)

    # Linhas de fundo
    for _ in range(6):
        draw.line(
            (
                random.randint(0, largura),
                random.randint(0, altura),
                random.randint(0, largura),
                random.randint(0, altura),
            ),
            fill=(
                random.randint(160, 200),
                random.randint(160, 200),
                random.randint(160, 200),
            ),
            width=1,
        )

    # Pontos de ruído
    for _ in range(80):
        draw.point(
            (random.randint(0, largura - 1), random.randint(0, altura - 1)),
            fill=(
                random.randint(100, 180),
                random.randint(100, 180),
                random.randint(100, 180),
            ),
        )

    espaco = largura // (len(texto) + 1)
    for i, ch in enumerate(texto):
        char_img = Image.new("RGBA", (40, 50), (0, 0, 0, 0))
        char_draw = ImageDraw.Draw(char_img)
        cor = (
            random.randint(20, 80),
            random.randint(20, 80),
            random.randint(20, 100),
        )
        char_draw.text((4, 4), ch, font=fonte, fill=cor + (255,))
        angulo = random.randint(-28, 28)
        char_img = char_img.rotate(angulo, resample=Image.BICUBIC, expand=True)
        x = 8 + i * espaco + random.randint(-3, 3)
        y = random.randint(2, 12)
        img.paste(char_img, (x, y), char_img)

    img = img.filter(ImageFilter.SMOOTH)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{b64}"


def gerar_desafio_captcha() -> dict:
    """Gera imagem captcha e guarda a resposta na sessão."""
    texto = "".join(secrets.choice(_CHARS) for _ in range(5))
    token = secrets.token_hex(8)
    session[_SESSION_ANSWER] = texto.upper()
    session[_SESSION_TOKEN] = token
    return {
        "imagem": _gerar_imagem(texto),
        "token": token,
    }


def validar_captcha_local(resposta: str | None, token: str | None) -> bool:
    """Valida a resposta do formulário contra a sessão e invalida o desafio."""
    esperado = session.pop(_SESSION_ANSWER, None)
    token_sessao = session.pop(_SESSION_TOKEN, None)

    if not esperado or not token_sessao:
        return False
    if not token or secrets.compare_digest(str(token), str(token_sessao)) is False:
        return False

    resposta_norm = (resposta or "").strip().upper()
    if not resposta_norm:
        return False
    return secrets.compare_digest(resposta_norm, str(esperado).upper())
