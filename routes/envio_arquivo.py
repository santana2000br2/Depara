from flask import (
    Blueprint,
    render_template,
    session,
    redirect,
    url_for,
    flash,
    request,
    send_file,
    jsonify
)
import pandas as pd
import io
import threading
from utils.layout_configs import load_layout_configs
from utils.data_processing import run_process_file_wrapper, detectar_layout
from datetime import datetime
import uuid
import time
import logging
import traceback

envio_arquivo_bp = Blueprint("envio_arquivo", __name__)

# Carregar configurações dos layouts
layout_columns_map, layouts_rules_map = load_layout_configs()

# Dicionário para armazenar temporariamente os dados de erro
temp_errors_store = {}

# Dicionário para armazenar status de processamento assíncrono
processing_status = {}

# Lock para evitar condições de corrida
status_lock = threading.Lock()


@envio_arquivo_bp.route("/", methods=["GET", "POST"])
def index():
    if "usuario" not in session:
        flash("Você precisa fazer login para acessar esta página.", "warning")
        return redirect(url_for("auth.login"))

    usuario = session["usuario"]
    projeto_selecionado = session.get("projeto_selecionado", {})
    empresa = projeto_selecionado.get("NomeProjeto", "")

    if request.method == "POST":
        # Verificar se um arquivo foi enviado
        if "arquivo" not in request.files:
            flash("Nenhum arquivo selecionado.", "error")
            return render_template(
                "envio_arquivo.html", usuario=usuario, empresa=empresa
            )

        arquivo = request.files["arquivo"]

        # Verificar se o arquivo tem nome
        if arquivo.filename == "":
            flash("Nenhum arquivo selecionado.", "error")
            return render_template(
                "envio_arquivo.html", usuario=usuario, empresa=empresa
            )

        if arquivo:
            try:
                # CORREÇÃO: Processamento sempre síncrono para debug
                return processar_arquivo_pequeno(arquivo, usuario, empresa)

            except Exception as e:
                flash(f"Erro ao processar arquivo: {str(e)}", "error")
                return render_template(
                    "envio_arquivo.html", usuario=usuario, empresa=empresa
                )

    # Limpar dados temporários antigos (mais de 1 hora)
    cleanup_old_temp_data()

    return render_template("envio_arquivo.html", usuario=usuario, empresa=empresa)


def processar_arquivo_pequeno(arquivo, usuario, empresa):
    """Processa arquivos de forma síncrona"""
    start_time = time.time()
    
    try:
        layout, df, df_errors, status, message, elapsed = run_process_file_wrapper(
            arquivo,
            layout_columns_map,
            layouts_rules_map,
            validar_nao_obrigatorios_flag=True,
        )

        # Mensagens flash
        if status == "error":
            flash(f"Erro no processamento: {message}", "error")
        elif status == "warning":
            flash(f"Aviso: {message}", "warning")
        else:
            flash("Arquivo processado com sucesso", "success")

        if layout:
            flash(f"Layout: {layout}", "info")

        flash(f"Tempo: {elapsed:.2f}s", "info")

        # Gerar ID único para os erros (se houver)
        export_id = None
        if not df_errors.empty:
            export_id = str(uuid.uuid4())
            # CORREÇÃO: Armazenar DataFrame serializável
            temp_errors_store[export_id] = {
                "df_errors": df_errors.to_dict('records'),  # Usar 'records' para serialização
                "timestamp": datetime.now(),
                "usuario": usuario.get("usuario", ""),
                "layout": layout,
            }

        # Preparar dados para exibição
        dados_processados = None
        if not df.empty:
            display_df = df.head(50)
            dados_processados = display_df.to_html(
                classes="compact-table", index=False, escape=False
            )

        return render_template(
            "envio_arquivo.html",
            usuario=usuario,
            empresa=empresa,
            dados_processados=dados_processados,
            df_errors=df_errors,
            erros_processados=not df_errors.empty,
            layout=layout,
            total_erros=len(df_errors) if not df_errors.empty else 0,
            export_id=export_id,
        )

    except Exception as e:
        logging.error(f"Erro no processamento síncrono: {str(e)}")
        logging.error(traceback.format_exc())
        flash(f"Erro ao processar arquivo: {str(e)}", "error")
        return render_template("envio_arquivo.html", usuario=usuario, empresa=empresa)


# CORREÇÃO: Removendo as funções de processamento assíncrono que não são mais usadas
# Mantemos apenas as rotas de status para compatibilidade, mas elas retornam erro

@envio_arquivo_bp.route("/status_processamento/<process_id>")
def status_processamento(process_id):
    """Rota mantida para compatibilidade, mas retorna erro pois não usamos mais processamento assíncrono"""
    return jsonify({
        "status": "error",
        "message": "Processamento assíncrono desabilitado. Use o processamento síncrono.",
        "progress": 0
    }), 404


@envio_arquivo_bp.route("/resultado_processamento/<process_id>")
def resultado_processamento(process_id):
    """Rota mantida para compatibilidade, mas redireciona para a página principal"""
    flash("Processamento assíncrono desabilitado. Use o processamento normal.", "info")
    return redirect(url_for("envio_arquivo.index"))


@envio_arquivo_bp.route("/exportar_erros")
def exportar_erros():
    if "usuario" not in session:
        flash("Você precisa fazer login para acessar esta página.", "warning")
        return redirect(url_for("auth.login"))

    # Obter export_id da query string
    export_id = request.args.get("export_id")

    if not export_id or export_id not in temp_errors_store:
        flash("Dados de exportação não encontrados ou expirados.", "error")
        return redirect(url_for("envio_arquivo.index"))

    try:
        # Recuperar dados do erro
        error_data = temp_errors_store[export_id]
        
        # CORREÇÃO: Recriar DataFrame a partir dos registros
        df_errors = pd.DataFrame(error_data["df_errors"])

        # Verificar se há erros para exportar
        if df_errors.empty:
            flash("Nenhum erro encontrado para exportação.", "info")
            return redirect(url_for("envio_arquivo.index"))

        # Criar arquivo Excel em memória
        output = io.BytesIO()

        # Usar pandas para exportar para Excel
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_errors.to_excel(writer, sheet_name='Erros_Validacao', index=False)
        
        output.seek(0)

        # Nome do arquivo com timestamp e layout
        layout = error_data.get("layout", "desconhecido")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"erros_validacao_{layout}_{timestamp}.xlsx"

        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=filename,
        )

    except Exception as e:
        flash(f"Erro ao exportar erros: {str(e)}", "error")
        return redirect(url_for("envio_arquivo.index"))


def cleanup_old_temp_data():
    """Limpa dados temporários com mais de 1 hora"""
    current_time = datetime.now()
    keys_to_remove = []

    for key, data in temp_errors_store.items():
        if (current_time - data["timestamp"]).total_seconds() > 3600:  # 1 hora
            keys_to_remove.append(key)
            
    # Limpar também status antigos
    with status_lock:
        for key in list(processing_status.keys()):
            if "start_time" in processing_status[key]:
                if (current_time - processing_status[key]["start_time"]).total_seconds() > 3600:
                    keys_to_remove.append(key)
                    del processing_status[key]

    for key in keys_to_remove:
        if key in temp_errors_store:
            del temp_errors_store[key]