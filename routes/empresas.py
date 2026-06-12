from flask import (
    Blueprint, render_template, redirect, url_for, 
    session, request, jsonify, flash
)
from db.connection import conectar_banco
from logger import logger
import re

empresas_bp = Blueprint("empresas", __name__)

def validar_cnpj(cnpj):
    """Valida e formata o CNPJ para garantir que tenha 14 dígitos"""
    if not cnpj:
        return None
    
    # Remove qualquer caractere não numérico
    cnpj_limpo = re.sub(r'\D', '', cnpj)
    
    # Verifica se tem 14 dígitos
    if len(cnpj_limpo) != 14:
        return None
    
    return cnpj_limpo

@empresas_bp.route("/gerenciar_empresas")
def gerenciar_empresas():
    if "usuario" not in session or not session["usuario"].get("adm"):
        flash("Acesso não autorizado", "error")
        return redirect(url_for("dashboard.dashboard"))

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            flash("Erro de conexão com o banco de dados", "error")
            return render_template("empresas.html", empresas=[], projetos=[])

        cursor = conn.cursor()

        # Buscar empresas com informações do projeto
        cursor.execute("""
            SELECT 
                e.EmpresaID,
                e.NomeEmpresa,
                e.CNPJ,
                p.NomeProjeto,
                p.ProjetoID
            FROM Empresa e
            LEFT JOIN Projeto p ON e.ProjetoID = p.ProjetoID
            ORDER BY e.NomeEmpresa
        """)
        
        empresas_raw = cursor.fetchall()
        
        # Converter para lista de dicionários
        empresas_list = []
        for empresa in empresas_raw:
            empresa_dict = {
                'EmpresaID': empresa.EmpresaID,
                'NomeEmpresa': empresa.NomeEmpresa,
                'CNPJ': empresa.CNPJ,
                'Projeto': empresa.NomeProjeto,
                'ProjetoID': empresa.ProjetoID
            }
            empresas_list.append(empresa_dict)

        # Buscar projetos para o formulário
        cursor.execute("SELECT ProjetoID, NomeProjeto FROM Projeto ORDER BY NomeProjeto")
        projetos_raw = cursor.fetchall()
        projetos_list = [{'ProjetoID': p.ProjetoID, 'NomeProjeto': p.NomeProjeto} for p in projetos_raw]

        return render_template(
            "empresas.html",
            empresas=empresas_list,
            projetos=projetos_list,
            usuario=session["usuario"]
        )

    except Exception as e:
        logger.error(f"Erro ao carregar empresas: {e}")
        flash(f"Erro ao carregar lista de empresas: {str(e)}", "error")
        return render_template("empresas.html", empresas=[], projetos=[])
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@empresas_bp.route("/salvar_empresa", methods=["POST"])
def salvar_empresa():
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"status": "error", "message": "Acesso não autorizado"}), 403

    data = request.get_json()
    empresa_id = data.get("empresa_id")
    nome_empresa = data.get("nome_empresa")
    cnpj = data.get("cnpj")
    projeto_id = data.get("projeto_id")

    print(f"DEBUG: Dados recebidos - empresa_id: {empresa_id}, nome_empresa: {nome_empresa}, cnpj: {cnpj}")

    # Validar CNPJ
    cnpj_validado = validar_cnpj(cnpj)
    if cnpj and not cnpj_validado:
        return jsonify({"status": "error", "message": "CNPJ deve conter exatamente 14 dígitos numéricos"}), 400

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()

        if empresa_id:  # EDITANDO empresa existente
            empresa_id = int(empresa_id)
            print(f"DEBUG: Editando empresa ID: {empresa_id}")
            
            # Verificar se a empresa existe
            cursor.execute("SELECT EmpresaID FROM Empresa WHERE EmpresaID = ?", (empresa_id,))
            if not cursor.fetchone():
                return jsonify({"status": "error", "message": "Empresa não encontrada"}), 404

            # Atualizar empresa
            cursor.execute("""
                UPDATE Empresa SET 
                    NomeEmpresa = ?, 
                    CNPJ = ?,
                    ProjetoID = ?
                WHERE EmpresaID = ?
            """, (
                nome_empresa,
                cnpj_validado,
                projeto_id,
                empresa_id
            ))
            print(f"DEBUG: Empresa {empresa_id} atualizada")

        else:  # NOVA empresa
            print("DEBUG: Criando nova empresa")
            
            # Verificar se CNPJ já existe
            if cnpj_validado:
                cursor.execute("SELECT EmpresaID FROM Empresa WHERE CNPJ = ?", (cnpj_validado,))
                if cursor.fetchone():
                    return jsonify({"status": "error", "message": "Já existe uma empresa com este CNPJ"}), 400

            # Inserir nova empresa
            cursor.execute("""
                INSERT INTO Empresa (
                    NomeEmpresa, CNPJ, ProjetoID
                ) VALUES (?, ?, ?)
            """, (
                nome_empresa,
                cnpj_validado,
                projeto_id
            ))
            print(f"DEBUG: Empresa {nome_empresa} inserida na tabela Empresa")

            # Obter o ID da nova empresa
            cursor.execute("SELECT MAX(EmpresaID) FROM Empresa WHERE NomeEmpresa = ?", (nome_empresa,))
            result = cursor.fetchone()
            nova_empresa_id = result[0] if result else None
            
            if not nova_empresa_id:
                # Tentar método alternativo
                cursor.execute("SELECT EmpresaID FROM Empresa WHERE NomeEmpresa = ?", (nome_empresa,))
                result = cursor.fetchone()
                nova_empresa_id = result[0] if result else None

            print(f"DEBUG: Nova empresa ID: {nova_empresa_id}")

            if not nova_empresa_id:
                conn.rollback()
                return jsonify({"status": "error", "message": "Falha ao obter ID da nova empresa"}), 500

        conn.commit()
        logger.info(f"Empresa {'atualizada' if empresa_id else 'criada'} com sucesso: {nome_empresa}")
        return jsonify({"status": "success", "message": "Empresa salva com sucesso!"}), 200

    except Exception as e:
        logger.error(f"Erro ao salvar empresa: {e}")
        if conn:
            conn.rollback()
        return jsonify({"status": "error", "message": f"Erro ao salvar empresa: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@empresas_bp.route("/excluir_empresa/<int:empresa_id>", methods=["POST"])
def excluir_empresa(empresa_id):
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"status": "error", "message": "Acesso não autorizado"}), 403

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()

        # Verificar se a empresa existe
        cursor.execute("SELECT NomeEmpresa FROM Empresa WHERE EmpresaID = ?", (empresa_id,))
        empresa = cursor.fetchone()
        if not empresa:
            return jsonify({"status": "error", "message": "Empresa não encontrada"}), 404
        
        # Excluir empresa
        cursor.execute("DELETE FROM Empresa WHERE EmpresaID = ?", (empresa_id,))
        deleted = cursor.rowcount

        conn.commit()

        if deleted > 0:
            logger.info(f"Empresa {empresa_id} excluída com sucesso")
            return jsonify({"status": "success", "message": "Empresa excluída com sucesso!"})
        else:
            return jsonify({"status": "error", "message": "Empresa não encontrada."}), 404

    except Exception as e:
        logger.error(f"Erro ao excluir empresa {empresa_id}: {e}")
        if conn:
            conn.rollback()
        return jsonify({"status": "error", "message": f"Erro interno ao excluir empresa: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@empresas_bp.route("/obter_empresa/<int:empresa_id>")
def obter_empresa(empresa_id):
    """Obtém os dados de uma empresa específica"""
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
                EmpresaID,
                NomeEmpresa,
                CNPJ,
                ProjetoID
            FROM Empresa 
            WHERE EmpresaID = ?
        """, (empresa_id,))
        
        empresa = cursor.fetchone()
        
        if not empresa:
            return jsonify({"success": False, "message": "Empresa não encontrada"})
        
        empresa_dict = {
            'EmpresaID': empresa.EmpresaID,
            'NomeEmpresa': empresa.NomeEmpresa,
            'CNPJ': empresa.CNPJ,
            'ProjetoID': empresa.ProjetoID
        }
        
        return jsonify({"success": True, "empresa": empresa_dict})

    except Exception as e:
        logger.error(f"Erro ao obter empresa: {e}")
        return jsonify({"success": False, "message": f"Erro ao obter empresa: {str(e)}"})
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()