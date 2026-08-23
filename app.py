from flask import Flask, redirect, url_for, jsonify, request, flash, render_template
from config import Config
import sys
import os
from flask import session
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.middleware.proxy_fix import ProxyFix

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar blueprints principais
from routes.auth import auth_bp
from routes.dashboard import dashboard_bp
from routes.usuarios import usuarios_bp
from routes.projetos import projetos_bp
from routes.empresas import empresas_bp
from routes.escopos import escopos_bp

# Importar blueprints de depara
from routes.condicao_pagamento import condicao_pagamento_bp
from routes.escolaridade import escolaridade_bp
from routes.enquadramento import enquadramento_bp
from routes.estado import estado_bp
from routes.estadocivil import estadocivil_bp
from routes.municipio import municipio_bp
from routes.pais import pais_bp
from routes.profissao import profissao_bp
from routes.segmentomercado import segmentomercado_bp
from routes.tipologradouro import tipologradouro_bp
from routes.departamento import departamento_bp
from routes.estoque import estoque_bp
from routes.naturezaoperacao import naturezaoperacao_bp
from routes.equipe import equipe_bp
from routes.usuario_depara import usuario_depara_bp
from routes.clasmontadora import clasmontadora_bp
from routes.grupolucratividade import grupolucratividade_bp
from routes.grupoproduto import grupoproduto_bp
from routes.pessoacodfabricante import pessoacodfabricante_bp
from routes.procedencia import procedencia_bp
from routes.tabelapreco import tabelapreco_bp
from routes.tipoproduto import tipoproduto_bp
from routes.unidade import unidade_bp
from routes.combustivel import combustivel_bp
from routes.corexterna import corexterna_bp
from routes.corinterna import corinterna_bp
from routes.marca import marca_bp
from routes.modeloveiculo import modeloveiculo_bp
from routes.opcional import opcional_bp
from routes.setorservico import setorservico_bp
from routes.tipoos import tipoos_bp
from routes.tiposervico import tiposervico_bp
from routes.tmo import tmo_bp
from routes.veiculoano import veiculoano_bp
from routes.agentecobrador import agentecobrador_bp
from routes.banco import banco_bp
from routes.contagerencial import contagerencial_bp
from routes.tipocobranca import tipocobranca_bp
from routes.tipocreditodebito import tipocreditodebito_bp
from routes.tipodocumento import tipodocumento_bp
from routes.tipoficharazao import tipoficharazao_bp
from routes.tipotitulo import tipotitulo_bp
from routes.centroresultado import centroresultado_bp
from routes.historicopadrao import historicopadrao_bp
from routes.planoconta import planoconta_bp
from routes.subconta import subconta_bp
from routes.tipolote import tipolote_bp
from routes.tiposubconta import tiposubconta_bp

# Importar blueprints de importação
from routes.importacao import importacao_bp
from routes.importacao.layout import layout_bp
from routes.importacao.historico_envio import historico_envio_bp
from routes.validador_estrutura import validador_estrutura_bp
from routes.envio_arquivo import envio_arquivo_bp
from routes.email import email_bp

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = app.config.get("SECRET_KEY", "chave-secreta-padrao")

# IIS/ARR/proxy: confia em X-Forwarded-Proto/For/Host para HTTPS e HSTS corretos.
# x_*=1 assume um hop de proxy na frente (ajuste via PROXY_FIX_X_FOR se necessário).
_proxy_hops = int(os.environ.get("PROXY_FIX_X_FOR", "1") or "1")
app.wsgi_app = ProxyFix(
    app.wsgi_app,
    x_for=_proxy_hops,
    x_proto=_proxy_hops,
    x_host=_proxy_hops,
    x_port=_proxy_hops,
    x_prefix=_proxy_hops,
)

# Hardening HTTP global (headers, HSTS, CSP/nonce, OPTIONS, HTTPS)
from utils.security_headers import init_security
init_security(app)

# Reduz drasticamente o tamanho do HTML gerado nas tabelas grandes de De/Para
app.jinja_env.trim_blocks = True
app.jinja_env.lstrip_blocks = True

import logging
_startup_logger = logging.getLogger('auth')
_startup_logger.info(
    "Limite de upload ativo: MAX_CONTENT_LENGTH=%s bytes (%.1f MB)",
    app.config.get('MAX_CONTENT_LENGTH'),
    (app.config.get('MAX_CONTENT_LENGTH') or 0) / (1024 * 1024),
)

# Registrar os blueprints - AUTH PRIMEIRO
app.register_blueprint(auth_bp, url_prefix="/auth")

# Depois os outros blueprints
app.register_blueprint(dashboard_bp, url_prefix="/dashboard")
app.register_blueprint(usuarios_bp, url_prefix="/usuarios")
app.register_blueprint(projetos_bp, url_prefix="/projetos")
app.register_blueprint(empresas_bp, url_prefix="/empresas")
app.register_blueprint(escopos_bp, url_prefix="/escopos")

# Registrar blueprints de depara
app.register_blueprint(condicao_pagamento_bp, url_prefix="/condicao_pagamento")
app.register_blueprint(escolaridade_bp, url_prefix="/escolaridade")
app.register_blueprint(enquadramento_bp, url_prefix="/enquadramento")
app.register_blueprint(estado_bp, url_prefix="/estado")
app.register_blueprint(estadocivil_bp, url_prefix="/estadocivil")
app.register_blueprint(municipio_bp, url_prefix="/municipio")
app.register_blueprint(pais_bp, url_prefix="/pais")
app.register_blueprint(profissao_bp, url_prefix="/profissao")
app.register_blueprint(segmentomercado_bp, url_prefix="/segmentomercado")
app.register_blueprint(tipologradouro_bp, url_prefix="/tipologradouro")
app.register_blueprint(departamento_bp, url_prefix="/departamento")
app.register_blueprint(estoque_bp, url_prefix="/estoque")
app.register_blueprint(naturezaoperacao_bp, url_prefix="/naturezaoperacao")
app.register_blueprint(equipe_bp, url_prefix="/equipe")
app.register_blueprint(usuario_depara_bp, url_prefix="/usuario_depara")
app.register_blueprint(clasmontadora_bp, url_prefix="/clasmontadora")
app.register_blueprint(grupolucratividade_bp, url_prefix="/grupolucratividade")
app.register_blueprint(grupoproduto_bp, url_prefix="/grupoproduto")
app.register_blueprint(pessoacodfabricante_bp, url_prefix="/pessoacodfabricante")
app.register_blueprint(procedencia_bp, url_prefix="/procedencia")
app.register_blueprint(tabelapreco_bp, url_prefix="/tabelapreco")
app.register_blueprint(tipoproduto_bp, url_prefix="/tipoproduto")
app.register_blueprint(unidade_bp, url_prefix="/unidade")
app.register_blueprint(combustivel_bp, url_prefix="/combustivel")
app.register_blueprint(corexterna_bp, url_prefix="/corexterna")
app.register_blueprint(corinterna_bp, url_prefix="/corinterna")
app.register_blueprint(marca_bp, url_prefix="/marca")
app.register_blueprint(modeloveiculo_bp, url_prefix="/modeloveiculo")
app.register_blueprint(opcional_bp, url_prefix="/opcional")
app.register_blueprint(setorservico_bp, url_prefix="/setorservico")
app.register_blueprint(tipoos_bp, url_prefix="/tipoos")
app.register_blueprint(tiposervico_bp, url_prefix="/tiposervico")
app.register_blueprint(tmo_bp, url_prefix="/tmo")
app.register_blueprint(veiculoano_bp, url_prefix="/veiculoano")
app.register_blueprint(agentecobrador_bp, url_prefix="/agentecobrador")
app.register_blueprint(banco_bp, url_prefix="/banco")
app.register_blueprint(contagerencial_bp, url_prefix="/contagerencial")
app.register_blueprint(tipocobranca_bp, url_prefix="/tipocobranca")
app.register_blueprint(tipocreditodebito_bp, url_prefix="/tipocreditodebito")
app.register_blueprint(tipodocumento_bp, url_prefix="/tipodocumento")
app.register_blueprint(tipoficharazao_bp, url_prefix="/tipoficharazao")
app.register_blueprint(tipotitulo_bp, url_prefix="/tipotitulo")
app.register_blueprint(centroresultado_bp, url_prefix="/centroresultado")
app.register_blueprint(historicopadrao_bp, url_prefix="/historicopadrao")
app.register_blueprint(planoconta_bp, url_prefix="/planoconta")
app.register_blueprint(subconta_bp, url_prefix="/subconta")
app.register_blueprint(tipolote_bp, url_prefix="/tipolote")
app.register_blueprint(tiposubconta_bp, url_prefix="/tiposubconta")

# Registrar blueprints de importação - CORRIGIDO
app.register_blueprint(importacao_bp, url_prefix="/importacao")
app.register_blueprint(validador_estrutura_bp, url_prefix="/validador-estrutura")
app.register_blueprint(layout_bp, url_prefix="/importacao/layout")
app.register_blueprint(historico_envio_bp, url_prefix="/importacao/historico-envios")
app.register_blueprint(envio_arquivo_bp, url_prefix="/envio_arquivo")
app.register_blueprint(email_bp, url_prefix="/email")

@app.route("/debug-endpoints")
def debug_endpoints():
    """Desabilitado: exposição da superfície de rotas."""
    return jsonify({"error": "Não encontrado"}), 404


@app.route("/api/diagnostico/upload-limite")
def diagnostico_upload_limite():
    """Limite de upload — apenas administradores autenticados."""
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"error": "Não autorizado"}), 403
    max_bytes = app.config.get("MAX_CONTENT_LENGTH") or 0
    return jsonify({
        "max_content_length_bytes": max_bytes,
        "max_content_length_mb": round(max_bytes / (1024 * 1024), 1),
        "env_max_content_length": os.environ.get("MAX_CONTENT_LENGTH"),
    })


import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)

@app.context_processor
def inject_user():
    """Injeta usuário e projeto da sessão para todos os templates."""
    from utils.projeto_acesso import importacao_completa_liberada, projeto_eh_arquivo_workflow

    ctx = {
        'usuario': None,
        'projeto_selecionado': None,
        'importacao_liberada_projeto': True,
        'projeto_arquivo_workflow': False,
    }
    if "usuario" in session:
        ctx['usuario'] = session['usuario']
    if "projeto_selecionado" in session:
        projeto = session['projeto_selecionado']
        ctx['projeto_selecionado'] = projeto
        ctx['projeto_arquivo_workflow'] = projeto_eh_arquivo_workflow(projeto)
        ctx['importacao_liberada_projeto'] = importacao_completa_liberada(projeto)
    return ctx

@app.route("/")
def index():
    return redirect(url_for("auth.login"))


@app.errorhandler(RequestEntityTooLarge)
def handle_file_too_large(_error):
    """Retorna erro amigável quando upload excede limite."""
    max_bytes = app.config.get("MAX_CONTENT_LENGTH", 0) or 0
    max_mb = round(max_bytes / (1024 * 1024), 1) if max_bytes else 0
    message = (
        f"Arquivo excede o limite de upload ({max_mb} MB). "
        "Divida o arquivo em partes menores."
    )
    logging.getLogger('auth').error(
        "RequestEntityTooLarge: path=%s content_length=%s limite=%s",
        request.path,
        request.content_length,
        max_bytes,
    )

    if request.path.startswith('/importacao'):
        flash(message, 'error')
        return redirect(url_for('importacao.index'))

    if request.path.startswith('/validador-estrutura'):
        flash(message, 'error')
        return redirect(url_for('validador_estrutura.index'))

    if request.path.endswith("/importar") or request.accept_mimetypes.accept_json:
        return jsonify({"success": False, "message": message}), 413

    return message, 413


if __name__ == "__main__":
    _debug = os.environ.get("FLASK_DEBUG", "").strip().lower() in ("1", "true", "yes", "on")
    _no_reload = os.environ.get("FLASK_NO_RELOADER", "").strip().lower() in ("1", "true", "yes", "on")
    app.run(debug=_debug, use_reloader=_debug and not _no_reload)