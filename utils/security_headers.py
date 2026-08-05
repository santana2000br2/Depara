"""Hardening HTTP global (OWASP): headers, HSTS, HTTPS, CSP com nonce, OPTIONS."""

from __future__ import annotations

import os
import re
import secrets

from flask import Flask, g, redirect, request


_SCRIPT_OPEN_RE = re.compile(r"<script(\s|>)", re.IGNORECASE)
_HTML_CT_PREFIXES = ("text/html", "application/xhtml+xml")


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "sim")


def _request_is_https() -> bool:
    if request.is_secure:
        return True
    proto = (
        request.headers.get("X-Forwarded-Proto")
        or request.environ.get("HTTP_X_FORWARDED_PROTO")
        or ""
    ).split(",")[0].strip().lower()
    if proto == "https":
        return True
    if request.headers.get("X-Forwarded-Ssl", "").lower() == "on":
        return True
    return False


def _build_csp(nonce: str) -> str:
    # Scripts: nonce nos <script> inline + hosts CDN.
    # Styles: NÃO usar nonce em style-src-elem — o Tailwind Play CDN injeta
    # <style> dinamicamente sem nonce; com nonce o browser ignora 'unsafe-inline'
    # e o layout das telas De/Para quebra.
    # unsafe-eval: exigido pelo cdn.tailwindcss.com.
    # COEP omitido: incompatível com reCAPTCHA/CDNs.
    return "; ".join(
        [
            "default-src 'self'",
            "base-uri 'self'",
            "object-src 'none'",
            "frame-ancestors 'self'",
            "form-action 'self'",
            (
                "script-src-elem 'self' 'nonce-{nonce}' "
                "https://cdnjs.cloudflare.com https://cdn.tailwindcss.com "
                "https://www.google.com https://www.gstatic.com "
                "https://cdn.jsdelivr.net 'unsafe-eval'"
            ).format(nonce=nonce),
            "script-src-attr 'unsafe-inline'",
            (
                "style-src-elem 'self' 'unsafe-inline' "
                "https://cdnjs.cloudflare.com https://cdn.jsdelivr.net"
            ),
            "style-src-attr 'unsafe-inline'",
            "img-src 'self' data: https:",
            "font-src 'self' data: https://cdnjs.cloudflare.com",
            (
                "connect-src 'self' https://cdn.tailwindcss.com "
                "https://cdnjs.cloudflare.com https://cdn.jsdelivr.net "
                "https://www.google.com https://www.gstatic.com"
            ),
            "frame-src 'self' https://www.google.com https://www.gstatic.com",
            "worker-src 'self' blob:",
            "upgrade-insecure-requests",
        ]
    )


def _inject_nonce_into_html(html: str, nonce: str) -> str:
    """Injeta nonce apenas em <script> (inline e externos).

    Não altera <style>: o Tailwind CDN cria estilos em runtime sem nonce.
    """

    def _script(match: re.Match) -> str:
        tail = match.group(1)
        chunk = match.group(0)
        if "nonce=" in chunk.lower():
            return chunk
        return f'<script nonce="{nonce}"{tail}'

    return _SCRIPT_OPEN_RE.sub(_script, html)


def init_security(app: Flask) -> None:
    """Registra middleware único de segurança na aplicação Flask."""

    # Em produção (IIS: FLASK_ENV=production) força HTTPS por padrão.
    # Desligue com FORCE_HTTPS=false se o redirect 301 já for exclusivo do IIS
    # e o backend receber HTTP interno sem X-Forwarded-Proto confiável.
    is_production = os.environ.get("FLASK_ENV", "").strip().lower() == "production"
    force_https = _env_flag("FORCE_HTTPS", default=is_production)

    block_options = _env_flag("BLOCK_HTTP_OPTIONS", True)
    enable_hsts = _env_flag("ENABLE_HSTS", True)

    @app.before_request
    def _security_before_request():
        g.csp_nonce = secrets.token_urlsafe(16)

        if block_options and request.method == "OPTIONS":
            # Sem CORS na aplicação — OPTIONS automático só aumenta superfície de ataque.
            return ("", 405)

        if not force_https:
            return None

        # Evita loop quando o redirect 301 já é feito no IIS/ARR.
        if request.headers.get("X-Forwarded-Proto", "").lower() == "https":
            return None
        if _request_is_https():
            return None

        # Localhost/dev: não forçar HTTPS (cookies Secure + HTTP quebram login).
        host = (request.host or "").split(":")[0].lower()
        if host in ("127.0.0.1", "localhost", "::1"):
            return None

        url = request.url
        if url.startswith("http://"):
            return redirect("https://" + url[len("http://") :], code=301)
        return None

    @app.after_request
    def _security_after_request(response):
        nonce = getattr(g, "csp_nonce", None) or secrets.token_urlsafe(16)

        # CSP + nonce em HTML
        content_type = (response.content_type or "").split(";")[0].strip().lower()
        if content_type.startswith(_HTML_CT_PREFIXES) and response.direct_passthrough is False:
            try:
                data = response.get_data(as_text=True)
                response.set_data(_inject_nonce_into_html(data, nonce))
            except Exception:
                # Não quebrar resposta binária/incomum
                pass

        response.headers["Content-Security-Policy"] = _build_csp(nonce)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["Permissions-Policy"] = (
            "accelerometer=(), camera=(), geolocation=(), gyroscope=(), "
            "magnetometer=(), microphone=(), payment=(), usb=(), "
            "interest-cohort=(), browsing-topics=()"
        )
        response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
        response.headers["Cross-Origin-Resource-Policy"] = "same-origin"
        # Cross-Origin-Embedder-Policy omitido: incompatível com reCAPTCHA e CDNs.

        response.headers["X-XSS-Protection"] = "0"
        response.headers.pop("X-Powered-By", None)
        response.headers.pop("Server", None)

        if enable_hsts and _request_is_https():
            response.headers["Strict-Transport-Security"] = (
                "max-age=31536000; includeSubDomains"
            )

        return response

    @app.context_processor
    def _inject_csp_nonce():
        return {"csp_nonce": getattr(g, "csp_nonce", "")}
