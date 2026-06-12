from flask import (
    Blueprint, render_template, redirect, url_for, 
    session, request, jsonify, flash
)
from db.connection import conectar_banco
from logger import logger

escopos_bp = Blueprint("escopos", __name__)

# Definindo os tipos de escopo fixos do sistema
TIPOS_ESCOPO = {
    "PESSOA": "Pessoa",
    "PRODUTOS": "Produtos", 
    "VEICULOS": "Veiculos",
    "FINANCEIRO": "Financeiro",
    "CONTABILIDADE": "Contabilidade",
    "FISCAL": "Fiscal",
    "GERAL": "Geral"
}

@escopos_bp.route("/gerenciar_escopos")
def gerenciar_escopos():
    if "usuario" not in session or not session["usuario"].get("adm"):
        flash("Acesso não autorizado", "error")
        return redirect(url_for("dashboard.dashboard"))

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            flash("Erro de conexão com o banco de dados", "error")
            return render_template("escopos.html", escopos=[], projetos=[])

        cursor = conn.cursor()

        # Buscar escopos com informações do projeto
        cursor.execute("""
            SELECT 
                e.EscopoID,
                e.NomeEscopo,
                e.Descricao,
                e.TipoEscopoIDs,
                p.NomeProjeto,
                p.ProjetoID
            FROM Escopo e
            LEFT JOIN Projeto p ON e.ProjetoID = p.ProjetoID
            ORDER BY e.NomeEscopo
        """)
        
        escopos_raw = cursor.fetchall()
        
        # Converter para lista de dicionários
        escopos_list = []
        for escopo in escopos_raw:
            # Processar os tipos de escopo para exibição
            tipos_escopo = []
            if escopo.TipoEscopoIDs:
                tipo_ids = escopo.TipoEscopoIDs.split(',')
                for tipo_id in tipo_ids:
                    if tipo_id in TIPOS_ESCOPO:
                        tipos_escopo.append(TIPOS_ESCOPO[tipo_id])
            
            escopo_dict = {
                'EscopoID': escopo.EscopoID,
                'NomeEscopo': escopo.NomeEscopo,
                'Descricao': escopo.Descricao,
                'TipoEscopoIDs': escopo.TipoEscopoIDs,
                'TiposEscopoDisplay': ", ".join(tipos_escopo) if tipos_escopo else "Nenhum",
                'Projeto': escopo.NomeProjeto,
                'ProjetoID': escopo.ProjetoID
            }
            escopos_list.append(escopo_dict)

        # Buscar projetos para o formulário
        cursor.execute("SELECT ProjetoID, NomeProjeto FROM Projeto ORDER BY NomeProjeto")
        projetos_raw = cursor.fetchall()
        projetos_list = [{'ProjetoID': p.ProjetoID, 'NomeProjeto': p.NomeProjeto} for p in projetos_raw]

        return render_template(
            "escopos.html",
            escopos=escopos_list,
            projetos=projetos_list,
            tipos_escopo=TIPOS_ESCOPO,
            usuario=session["usuario"]
        )

    except Exception as e:
        logger.error(f"Erro ao carregar escopos: {e}")
        flash(f"Erro ao carregar lista de escopos: {str(e)}", "error")
        return render_template("escopos.html", escopos=[], projetos=[])
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@escopos_bp.route("/salvar_escopo", methods=["POST"])
def salvar_escopo():
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"status": "error", "message": "Acesso não autorizado"}), 403

    data = request.get_json()
    escopo_id = data.get("escopo_id")
    nome_escopo = data.get("nome_escopo")
    descricao = data.get("descricao")
    projeto_id = data.get("projeto_id")
    tipos_escopo = data.get("tipos_escopo", [])  # Lista de tipos selecionados

    print(f"DEBUG: Dados recebidos - escopo_id: {escopo_id}, nome_escopo: {nome_escopo}, tipos_escopo: {tipos_escopo}")

    # Validar tipos de escopo
    tipos_validos = []
    for tipo in tipos_escopo:
        if tipo in TIPOS_ESCOPO:
            tipos_validos.append(tipo)
    
    # Converter lista de tipos em string separada por vírgulas
    tipos_escopo_str = ",".join(tipos_validos) if tipos_validos else None

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()

        if escopo_id:  # EDITANDO escopo existente
            escopo_id = int(escopo_id)
            print(f"DEBUG: Editando escopo ID: {escopo_id}")
            
            # Verificar se o escopo existe
            cursor.execute("SELECT EscopoID FROM Escopo WHERE EscopoID = ?", (escopo_id,))
            if not cursor.fetchone():
                return jsonify({"status": "error", "message": "Escopo não encontrado"}), 404

            # Atualizar escopo
            cursor.execute("""
                UPDATE Escopo SET 
                    NomeEscopo = ?, 
                    Descricao = ?,
                    ProjetoID = ?,
                    TipoEscopoIDs = ?
                WHERE EscopoID = ?
            """, (
                nome_escopo,
                descricao,
                projeto_id,
                tipos_escopo_str,
                escopo_id
            ))
            print(f"DEBUG: Escopo {escopo_id} atualizado")

        else:  # NOVO escopo
            print("DEBUG: Criando novo escopo")
            
            # Inserir novo escopo
            cursor.execute("""
                INSERT INTO Escopo (
                    NomeEscopo, Descricao, ProjetoID, TipoEscopoIDs
                ) VALUES (?, ?, ?, ?)
            """, (
                nome_escopo,
                descricao,
                projeto_id,
                tipos_escopo_str
            ))
            print(f"DEBUG: Escopo {nome_escopo} inserido na tabela Escopo")

            # Obter o ID do novo escopo
            cursor.execute("SELECT MAX(EscopoID) FROM Escopo WHERE NomeEscopo = ?", (nome_escopo,))
            result = cursor.fetchone()
            novo_escopo_id = result[0] if result else None
            
            if not novo_escopo_id:
                # Tentar método alternativo
                cursor.execute("SELECT EscopoID FROM Escopo WHERE NomeEscopo = ?", (nome_escopo,))
                result = cursor.fetchone()
                novo_escopo_id = result[0] if result else None

            print(f"DEBUG: Novo escopo ID: {novo_escopo_id}")

            if not novo_escopo_id:
                conn.rollback()
                return jsonify({"status": "error", "message": "Falha ao obter ID do novo escopo"}), 500

        conn.commit()
        logger.info(f"Escopo {'atualizado' if escopo_id else 'criado'} com sucesso: {nome_escopo}")
        return jsonify({"status": "success", "message": "Escopo salvo com sucesso!"}), 200

    except Exception as e:
        logger.error(f"Erro ao salvar escopo: {e}")
        if conn:
            conn.rollback()
        return jsonify({"status": "error", "message": f"Erro ao salvar escopo: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@escopos_bp.route("/excluir_escopo/<int:escopo_id>", methods=["POST"])
def excluir_escopo(escopo_id):
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"status": "error", "message": "Acesso não autorizado"}), 403

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()

        # Verificar se o escopo existe
        cursor.execute("SELECT NomeEscopo FROM Escopo WHERE EscopoID = ?", (escopo_id,))
        escopo = cursor.fetchone()
        if not escopo:
            return jsonify({"status": "error", "message": "Escopo não encontrado"}), 404
        
        # Excluir escopo
        cursor.execute("DELETE FROM Escopo WHERE EscopoID = ?", (escopo_id,))
        deleted = cursor.rowcount

        conn.commit()

        if deleted > 0:
            logger.info(f"Escopo {escopo_id} excluído com sucesso")
            return jsonify({"status": "success", "message": "Escopo excluído com sucesso!"})
        else:
            return jsonify({"status": "error", "message": "Escopo não encontrado."}), 404

    except Exception as e:
        logger.error(f"Erro ao excluir escopo {escopo_id}: {e}")
        if conn:
            conn.rollback()
        return jsonify({"status": "error", "message": f"Erro interno ao excluir escopo: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@escopos_bp.route("/obter_escopo/<int:escopo_id>")
def obter_escopo(escopo_id):
    """Obtém os dados de um escopo específico"""
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"success": False, "message": "Acesso não autorizado"})

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"success": False, "message": "Erro de conexão com o banco de dados"})

        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                EscopoID,
                NomeEscopo,
                Descricao,
                ProjetoID,
                TipoEscopoIDs
            FROM Escopo 
            WHERE EscopoID = ?
        """, (escopo_id,))
        
        escopo = cursor.fetchone()
        
        if not escopo:
            return jsonify({"success": False, "message": "Escopo não encontrado"})
        
        # Converter string de tipos em lista
        tipos_escopo_list = []
        if escopo.TipoEscopoIDs:
            tipos_escopo_list = escopo.TipoEscopoIDs.split(',')
        
        escopo_dict = {
            'EscopoID': escopo.EscopoID,
            'NomeEscopo': escopo.NomeEscopo,
            'Descricao': escopo.Descricao,
            'ProjetoID': escopo.ProjetoID,
            'TiposEscopo': tipos_escopo_list
        }
        
        return jsonify({"success": True, "escopo": escopo_dict})

    except Exception as e:
        logger.error(f"Erro ao obter escopo: {e}")
        return jsonify({"success": False, "message": f"Erro ao obter escopo: {str(e)}"})
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()