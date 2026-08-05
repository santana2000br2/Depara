from flask import Blueprint, render_template, session, redirect, url_for, flash

from logger import logger
from utils.projeto_acesso import acesso_envio_arquivos
from utils.historico_envio_arquivo import listar_historico_envio

historico_envio_bp = Blueprint("historico_envio", __name__)


def _base_vars(**kwargs):
    from routes.importacao import base_template_vars
    return base_template_vars(**kwargs)


@historico_envio_bp.route("/")
def index():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    projeto = session.get("projeto_selecionado")
    if not projeto:
        flash("Selecione um projeto para ver o histórico de envios.", "warning")
        return redirect(url_for("auth.trocar_projeto"))

    if not acesso_envio_arquivos(projeto):
        flash(
            "Histórico de envios disponível apenas para projetos Arquivo X Workflow.",
            "warning",
        )
        return redirect(url_for("dashboard.dashboard"))

    try:
        historico = listar_historico_envio(projeto.get("ProjetoID"), limite=300)
    except Exception as exc:
        logger.exception("Erro ao carregar histórico de envios")
        flash(f"Erro ao carregar histórico: {exc}", "error")
        historico = []

    return render_template(
        "importacao/historico_envios.html",
        **_base_vars(
            historico=historico,
            projeto_nome=projeto.get("NomeProjeto"),
        ),
    )
