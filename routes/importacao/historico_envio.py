from io import BytesIO

from flask import (
    Blueprint,
    render_template,
    session,
    redirect,
    url_for,
    flash,
    jsonify,
    request,
    send_file,
)
import pandas as pd

from logger import logger
from utils.projeto_acesso import acesso_envio_arquivos
from utils.historico_envio_arquivo import (
    listar_historico_envio,
    obter_envio_historico,
    listar_erros_historico_envio,
    contar_erros_historico_envio,
)

historico_envio_bp = Blueprint("historico_envio", __name__)


def _base_vars(**kwargs):
    from routes.importacao import base_template_vars
    return base_template_vars(**kwargs)


def _projeto_autorizado():
    if "usuario" not in session:
        return None, redirect(url_for("auth.login"))

    projeto = session.get("projeto_selecionado")
    if not projeto:
        flash("Selecione um projeto para ver o histórico de envios.", "warning")
        return None, redirect(url_for("auth.trocar_projeto"))

    if not acesso_envio_arquivos(projeto):
        flash(
            "Histórico de envios disponível apenas para projetos Arquivo X Workflow.",
            "warning",
        )
        return None, redirect(url_for("dashboard.dashboard"))

    return projeto, None


@historico_envio_bp.route("/")
def index():
    projeto, bloqueio = _projeto_autorizado()
    if bloqueio:
        return bloqueio

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


@historico_envio_bp.route("/api/<int:historico_id>/erros")
def api_erros(historico_id):
    """JSON com erros/avisos gravados daquele envio."""
    projeto, bloqueio = _projeto_autorizado()
    if bloqueio:
        if request.accept_mimetypes.accept_json:
            return jsonify({"erro": "Acesso negado"}), 403
        return bloqueio

    envio = obter_envio_historico(historico_id, projeto.get("ProjetoID"))
    if not envio:
        return jsonify({"erro": "Envio não encontrado neste projeto."}), 404

    tipo = (request.args.get("tipo") or "").strip().lower() or None
    if tipo and tipo not in ("erro", "aviso"):
        return jsonify({"erro": "Tipo inválido. Use erro ou aviso."}), 400

    contagem = contar_erros_historico_envio(historico_id, projeto.get("ProjetoID"))
    itens = listar_erros_historico_envio(
        historico_id,
        projeto.get("ProjetoID"),
        tipo=tipo,
        limite=500,
    )
    return jsonify({
        "envio": envio,
        "contagem": contagem,
        "total_retornado": len(itens),
        "itens": itens,
    })


@historico_envio_bp.route("/exportar/<int:historico_id>/erros")
def exportar_erros(historico_id):
    """Excel com todos os erros/avisos do envio."""
    projeto, bloqueio = _projeto_autorizado()
    if bloqueio:
        return bloqueio

    envio = obter_envio_historico(historico_id, projeto.get("ProjetoID"))
    if not envio:
        flash("Envio não encontrado neste projeto.", "warning")
        return redirect(url_for("historico_envio.index"))

    itens = listar_erros_historico_envio(
        historico_id,
        projeto.get("ProjetoID"),
        limite=10000,
    )
    if not itens:
        flash("Não há erros/avisos detalhados gravados para este envio.", "info")
        return redirect(url_for("historico_envio.index"))

    df = pd.DataFrame([
        {
            "Tipo": i.get("Tipo"),
            "Linha": i.get("Linha"),
            "Coluna": i.get("Coluna"),
            "Mensagem": i.get("Mensagem"),
        }
        for i in itens
    ])
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Erros_Avisos")
    buffer.seek(0)

    nome = (envio.get("NomeArquivo") or "envio").replace("/", "_")[:40]
    filename = f"historico_{historico_id}_{nome}_erros.xlsx"
    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
