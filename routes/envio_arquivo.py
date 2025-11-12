from flask import (
    Blueprint,
    render_template,
    session,
    redirect,
    url_for,
    flash,
    request,
    send_file,
)
import pandas as pd
import io
from utils.layout_configs import load_layout_configs
from utils.data_processing import run_process_file_wrapper
from datetime import datetime
import uuid

envio_arquivo_bp = Blueprint("envio_arquivo", __name__)

# Carregar configurações dos layouts
layout_columns_map, layouts_rules_map = load_layout_configs()

# Dicionário para armazenar temporariamente os dados de erro
temp_errors_store = {}


@envio_arquivo_bp.route("/", methods=["GET", "POST"])
def index():
    if "usuario" not in session:
        flash("Você precisa fazer login para acessar esta página.", "warning")
        return redirect(url_for("auth.login"))

    usuario = session["usuario"]
    empresa = usuario.get("empresa", "")

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
                # Processar o arquivo
                layout, df, df_errors, status, message, elapsed = (
                    run_process_file_wrapper(
                        arquivo,
                        layout_columns_map,
                        layouts_rules_map,
                        validar_nao_obrigatorios_flag=True,
                    )
                )

                # Mensagens flash compactas
                if status == "error":
                    flash(f"Erro no processamento: {message}", "error")
                elif status == "warning":
                    flash(f"Aviso: {message}", "warning")
                else:
                    flash("Arquivo processado com sucesso", "success")

                # Informações adicionais compactas
                if layout:
                    flash(f"Layout: {layout}", "info")

                flash(f"Tempo: {elapsed:.2f}s", "info")

                # Gerar ID único para os erros (se houver)
                export_id = None
                if not df_errors.empty:
                    export_id = str(uuid.uuid4())
                    # Armazenar apenas informações essenciais
                    temp_errors_store[export_id] = {
                        "df_errors": df_errors.to_dict(),
                        "timestamp": datetime.now(),
                        "usuario": usuario.get("usuario", ""),
                        "layout": layout,
                    }

                # Preparar dados para exibição
                dados_processados = None
                if not df.empty:
                    # Limitar a exibição para não sobrecarregar a página
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
                flash(f"Erro ao processar arquivo: {str(e)}", "error")
                return render_template(
                    "envio_arquivo.html", usuario=usuario, empresa=empresa
                )

    # Limpar dados temporários antigos (mais de 1 hora)
    cleanup_old_temp_data()

    return render_template("envio_arquivo.html", usuario=usuario, empresa=empresa)


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
        df_errors = pd.DataFrame(error_data["df_errors"])

        # Verificar se há erros para exportar
        if df_errors.empty:
            flash("Nenhum erro encontrado para exportação.", "info")
            return redirect(url_for("envio_arquivo.index"))

        # Criar arquivo Excel em memória
        output = io.BytesIO()

        # Usar openpyxl diretamente para ter mais controle
        from openpyxl import Workbook
        from openpyxl.utils.dataframe import dataframe_to_rows

        wb = Workbook()
        ws = wb.active
        ws.title = "Erros_Validacao"

        # Adicionar cabeçalho
        ws.append(["Linha", "Coluna", "Erro"])

        # Adicionar dados
        for _, row in df_errors.iterrows():
            ws.append([row["Linha"], row["Coluna"], row["Erro"]])

        # Salvar no buffer
        wb.save(output)
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

    for key in keys_to_remove:
        del temp_errors_store[key]
