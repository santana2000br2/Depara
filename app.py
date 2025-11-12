from flask import Flask, redirect, url_for
from config import Config
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# REMOVA ESTA LINHA DUPLICADA:
# from routes.routes import auth_bp

# Importar blueprints
from routes.auth import auth_bp
from routes.dashboard import dashboard_bp
from routes.usuarios import usuarios_bp
from routes.projetos import projetos_bp
from routes.empresas import empresas_bp
from routes.escopos import escopos_bp
from routes.condicao_pagamento import condicao_pagamento_bp
from routes.escolaridade import escolaridade_bp
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

from routes.envio_arquivo import envio_arquivo_bp

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = app.config.get("SECRET_KEY", "chave-secreta-padrao")

# Registrar os blueprints - AUTH PRIMEIRO
app.register_blueprint(auth_bp, url_prefix="/auth")

# Depois os outros blueprints
app.register_blueprint(dashboard_bp, url_prefix="/dashboard")
app.register_blueprint(usuarios_bp, url_prefix="/usuarios")
app.register_blueprint(projetos_bp, url_prefix="/projetos")
app.register_blueprint(empresas_bp, url_prefix="/empresas")
app.register_blueprint(escopos_bp, url_prefix="/escopos")
app.register_blueprint(condicao_pagamento_bp, url_prefix="/condicao_pagamento")
app.register_blueprint(escolaridade_bp, url_prefix="/escolaridade")
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

app.register_blueprint(envio_arquivo_bp, url_prefix="/envio_arquivo")


@app.route("/debug-endpoints")
def debug_endpoints():
    import json

    endpoints = []
    for rule in app.url_map.iter_rules():
        # CORREÇÃO: Verificar se rule.methods não é None antes de converter para lista
        methods = rule.methods
        if methods is None:
            methods_list = []
        else:
            methods_list = list(methods)
        
        endpoints.append(
            {
                "endpoint": rule.endpoint,
                "methods": methods_list,
                "rule": str(rule),
            }
        )
    return json.dumps(endpoints, indent=2)


@app.route("/")
def index():
    return redirect(url_for("auth.login"))


if __name__ == "__main__":
    app.run(debug=True, port=5000)