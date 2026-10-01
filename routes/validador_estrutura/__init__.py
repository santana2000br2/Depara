"""Validador de estrutura — apenas validação de arquivo (sem importação / De/Para)."""

from flask import Blueprint

from routes.importacao import pagina_importacao_arquivo
from routes.importacao import (
    api_detalhes_processamento,
    api_preview_top100_processamento,
    api_status_job,
    api_upload_chunk as _api_upload_chunk,
    api_upload_finalizar as _api_upload_finalizar,
    api_upload_iniciar as _api_upload_iniciar,
    api_importacao_status as _api_importacao_status,
    exportar_erros_processamento,
    exportar_processado_processamento,
)

validador_estrutura_bp = Blueprint('validador_estrutura', __name__)


@validador_estrutura_bp.route('/', methods=['GET', 'POST'])
def index():
    return pagina_importacao_arquivo(somente_validacao=True)


@validador_estrutura_bp.route('/api/upload/iniciar', methods=['POST'])
def api_upload_iniciar():
    return _api_upload_iniciar()


@validador_estrutura_bp.route('/api/upload/chunk', methods=['POST'])
def api_upload_chunk():
    return _api_upload_chunk()


@validador_estrutura_bp.route('/api/upload/finalizar', methods=['POST'])
def api_upload_finalizar():
    return _api_upload_finalizar()


@validador_estrutura_bp.route('/api/importacoes/<int:importacao_id>/status')
def api_importacao_status(importacao_id):
    return _api_importacao_status(importacao_id)


@validador_estrutura_bp.route('/api/job/<job_id>')
def api_job(job_id):
    return api_status_job(job_id)


@validador_estrutura_bp.route('/api/detalhes/<process_id>')
def api_detalhes(process_id):
    return api_detalhes_processamento(process_id)


@validador_estrutura_bp.route('/api/preview/<process_id>')
def api_preview(process_id):
    return api_preview_top100_processamento(process_id)


@validador_estrutura_bp.route('/exportar_erros/<process_id>')
def exportar_erros(process_id):
    return exportar_erros_processamento(
        process_id,
        redirect_endpoint='validador_estrutura.index',
    )


@validador_estrutura_bp.route('/exportar_processado/<process_id>')
def exportar_processado(process_id):
    return exportar_processado_processamento(
        process_id,
        redirect_endpoint='validador_estrutura.index',
    )
