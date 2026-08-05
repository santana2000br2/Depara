from flask import Blueprint, render_template, redirect, url_for, session, flash, request, jsonify

from logger import logger
from utils.depara_notificacao import verificar_todos_blocos_projeto
from utils.email_config import (
    obter_smtp_config,
    salvar_smtp_config,
    validar_lista_emails,
)
from utils.email_service import enviar_email, resolver_config_smtp

email_bp = Blueprint("email", __name__)


def _verificar_admin_pagina():
    if "usuario" not in session:
        flash("Faça login para acessar esta página.", "error")
        return redirect(url_for("auth.login"))

    if not session["usuario"].get("adm"):
        flash("Acesso negado. Apenas administradores podem acessar E-mail.", "error")
        return redirect(url_for("dashboard.dashboard"))

    return None


def _verificar_admin_api():
    if "usuario" not in session or not session["usuario"].get("adm"):
        return False, (jsonify({"success": False, "message": "Acesso negado"}), 403)
    return True, None


@email_bp.route("/")
@email_bp.route("/index")
def index():
    bloqueio = _verificar_admin_pagina()
    if bloqueio:
        return bloqueio

    usuario = session["usuario"]
    projeto = session.get("projeto_selecionado") or {}

    return render_template(
        "email.html",
        usuario=usuario,
        projeto_selecionado=projeto,
    )


@email_bp.route("/api/config", methods=["GET"])
def api_obter_config():
    ok, resposta = _verificar_admin_api()
    if not ok:
        return resposta

    smtp = obter_smtp_config(None) or {}
    smtp["smtp_senha_configurada"] = bool(smtp.pop("smtp_senha_configurada", False) or smtp.get("smtp_senha"))
    smtp["smtp_senha"] = ""

    return jsonify({
        "success": True,
        "smtp": smtp,
    })


@email_bp.route("/api/config", methods=["POST"])
def api_salvar_config():
    ok, resposta = _verificar_admin_api()
    if not ok:
        return resposta

    dados = request.get_json() or {}

    try:
        smtp = dados.get("smtp") or {}
        if not smtp.get("smtp_host", "").strip():
            return jsonify({"success": False, "message": "Informe o servidor SMTP."}), 400
        if not smtp.get("email_remetente", "").strip():
            return jsonify({"success": False, "message": "Informe o e-mail remetente."}), 400
        ok_emails, emails = validar_lista_emails(smtp.get("destinatarios", ""))
        if not ok_emails or not emails:
            return jsonify({
                "success": False,
                "message": "Informe pelo menos um destinatário válido.",
            }), 400

        salvar_smtp_config(None, smtp)

        return jsonify({"success": True, "message": "Configuração global salva com sucesso."})
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        logger.error(f"Erro ao salvar config de e-mail: {exc}")
        return jsonify({"success": False, "message": f"Erro ao salvar: {exc}"}), 500


@email_bp.route("/api/testar", methods=["POST"])
def api_testar_email():
    ok, resposta = _verificar_admin_api()
    if not ok:
        return resposta

    config_email = obter_smtp_config(None) or {}
    ok, emails = validar_lista_emails(config_email.get("destinatarios", ""))
    if not ok or not emails:
        return jsonify({
            "success": False,
            "message": "Cadastre pelo menos um destinatário antes de testar.",
        }), 400

    smtp_config = resolver_config_smtp(config_email)
    if not smtp_config:
        return jsonify({
            "success": False,
            "message": "SMTP não configurado. Preencha os dados do servidor ou configure SMTP_HOST no .env.",
        }), 400

    assunto = "[DE x PARA] E-mail de teste — Configuração global"
    texto = (
        f"Este é um e-mail de teste do sistema DE x PARA.\n\n"
        "Configuração válida para todos os projetos.\n"
        "Se você recebeu esta mensagem, a configuração SMTP está funcionando."
    )

    try:
        enviar_email(smtp_config, emails, assunto, texto)
        return jsonify({
            "success": True,
            "message": f"E-mail de teste enviado para {len(emails)} destinatário(s).",
        })
    except Exception as exc:
        return jsonify({"success": False, "message": f"Falha no envio: {exc}"}), 500


@email_bp.route("/api/verificar-notificacoes", methods=["POST"])
def api_verificar_notificacoes():
    ok, resposta = _verificar_admin_api()
    if not ok:
        return resposta

    projeto = session.get("projeto_selecionado")
    if not projeto:
        return jsonify({
            "success": False,
            "message": "Selecione um projeto para verificar os blocos concluídos.",
        }), 400
    projeto_id = projeto.get("ProjetoID")
    banco_usuario = projeto.get("DadosGX")
    if not banco_usuario:
        return jsonify({"success": False, "message": "Projeto sem banco DadosGX configurado."}), 400

    resultados = verificar_todos_blocos_projeto(
        projeto_id,
        banco_usuario,
        projeto.get("NomeProjeto", "Projeto"),
    )
    enviados = [r for r in resultados if r.get("enviado")]

    return jsonify({
        "success": True,
        "enviados": enviados,
        "resultados": resultados,
        "message": (
            f"{len(enviados)} notificação(ões) enviada(s)."
            if enviados
            else "Nenhum bloco concluído pendente de notificação."
        ),
    })
