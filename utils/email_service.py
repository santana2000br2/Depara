"""Envio de e-mails via SMTP (stdlib)."""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import Config
from logger import logger


def _smtp_padrao_env():
    return {
        "smtp_host": Config.SMTP_HOST,
        "smtp_port": Config.SMTP_PORT,
        "smtp_usuario": Config.SMTP_USER,
        "smtp_senha": Config.SMTP_PASSWORD,
        "email_remetente": Config.SMTP_FROM,
        "usar_tls": Config.SMTP_USE_TLS,
        "ativo": bool(Config.SMTP_HOST),
    }


def resolver_config_smtp(projeto_config=None):
    """Usa config do projeto; se incompleta, cai no .env."""
    projeto_config = projeto_config or {}
    env_config = _smtp_padrao_env()

    host = (projeto_config.get("smtp_host") or "").strip() or env_config["smtp_host"]
    if not host:
        return None

    return {
        "smtp_host": host,
        "smtp_port": int(projeto_config.get("smtp_port") or env_config["smtp_port"] or 587),
        "smtp_usuario": (projeto_config.get("smtp_usuario") or "").strip() or env_config["smtp_usuario"],
        "smtp_senha": projeto_config.get("smtp_senha") or env_config["smtp_senha"],
        "email_remetente": (
            (projeto_config.get("email_remetente") or "").strip() or env_config["email_remetente"]
        ),
        "usar_tls": projeto_config.get("usar_tls", env_config["usar_tls"]),
        "ativo": projeto_config.get("ativo", True),
    }


def enviar_email(smtp_config, destinatarios, assunto, corpo_texto, corpo_html=None):
    if not smtp_config or not smtp_config.get("ativo", True):
        raise ValueError("Configuração SMTP inativa ou não informada")

    if isinstance(destinatarios, str):
        destinatarios = [e.strip() for e in destinatarios.split(",") if e.strip()]

    if not destinatarios:
        raise ValueError("Nenhum destinatário informado")

    remetente = smtp_config.get("email_remetente") or smtp_config.get("smtp_usuario")
    if not remetente:
        raise ValueError("E-mail remetente não configurado")

    host = smtp_config["smtp_host"]
    port = int(smtp_config.get("smtp_port") or 587)
    usuario = smtp_config.get("smtp_usuario")
    senha = smtp_config.get("smtp_senha")
    usar_tls = smtp_config.get("usar_tls", True)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = assunto
    msg["From"] = remetente
    msg["To"] = ", ".join(destinatarios)
    msg.attach(MIMEText(corpo_texto, "plain", "utf-8"))
    if corpo_html:
        msg.attach(MIMEText(corpo_html, "html", "utf-8"))

    try:
        if port == 465:
            with smtplib.SMTP_SSL(host, port, timeout=30) as server:
                if usuario and senha:
                    server.login(usuario, senha)
                server.sendmail(remetente, destinatarios, msg.as_string())
        else:
            with smtplib.SMTP(host, port, timeout=30) as server:
                server.ehlo()
                if usar_tls:
                    server.starttls()
                    server.ehlo()
                if usuario and senha:
                    server.login(usuario, senha)
                server.sendmail(remetente, destinatarios, msg.as_string())

        logger.info(f"E-mail enviado para {destinatarios}: {assunto}")
        return True
    except Exception as exc:
        logger.error(f"Falha ao enviar e-mail: {exc}")
        raise
