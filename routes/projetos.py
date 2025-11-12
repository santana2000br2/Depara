from flask import Blueprint, render_template, redirect, url_for, flash, session, request, jsonify
from db.connection import conectar_banco
from logger import logger

projetos_bp = Blueprint("projetos", __name__)

@projetos_bp.route("/gerenciar_projetos")
def gerenciar_projetos():
    if "usuario" not in session or not session["usuario"].get("adm"):
        flash("Acesso não autorizado", "error")
        return redirect(url_for("dashboard.dashboard"))

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            flash("Erro de conexão com o banco de dados", "error")
            return render_template("projetos.html", projetos=[])

        cursor = conn.cursor()

        # Buscar projetos
        cursor.execute("""
            SELECT 
                ProjetoID,
                NomeProjeto,
                DadosGX,
                ServidorWIN,
                UsuarioDB,
                SenhaDB,
                BancoHomo,
                PontoFocal,
                ConsultorLider,
                LiderProjeto
            FROM Projeto 
            ORDER BY NomeProjeto
        """)
        
        projetos_raw = cursor.fetchall()
        
        # Converter para lista de dicionários
        projetos_list = []
        for projeto in projetos_raw:
            projeto_dict = {
                'ProjetoID': projeto.ProjetoID,
                'NomeProjeto': projeto.NomeProjeto,
                'DadosGX': projeto.DadosGX,
                'ServidorWIN': projeto.ServidorWIN,
                'UsuarioDB': projeto.UsuarioDB,
                'SenhaDB': projeto.SenhaDB,
                'BancoHomo': projeto.BancoHomo,
                'PontoFocal': projeto.PontoFocal,
                'ConsultorLider': projeto.ConsultorLider,
                'LiderProjeto': projeto.LiderProjeto
            }
            projetos_list.append(projeto_dict)

        return render_template(
            "projetos.html",
            projetos=projetos_list,
            usuario=session["usuario"]
        )

    except Exception as e:
        logger.error(f"Erro ao carregar projetos: {e}")
        flash(f"Erro ao carregar lista de projetos: {str(e)}", "error")
        return render_template("projetos.html", projetos=[])
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@projetos_bp.route("/salvar_projeto", methods=["POST"])
def salvar_projeto():
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"status": "error", "message": "Acesso não autorizado"}), 403

    data = request.get_json()
    projeto_id = data.get("projeto_id")
    nome_projeto = data.get("nome_projeto")
    dados_gx = data.get("dados_gx")
    servidor_win = data.get("servidor_win")
    usuario_db = data.get("usuario_db")
    senha_db = data.get("senha_db")
    banco_homo = data.get("banco_homo")
    ponto_focal = data.get("ponto_focal")
    consultor_lider = data.get("consultor_lider")
    lider_projeto = data.get("lider_projeto")

    print(f"DEBUG: Dados recebidos - projeto_id: {projeto_id}, nome_projeto: {nome_projeto}")

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()

        if projeto_id:  # EDITANDO projeto existente
            projeto_id = int(projeto_id)
            print(f"DEBUG: Editando projeto ID: {projeto_id}")
            
            # Verificar se o projeto existe
            cursor.execute("SELECT ProjetoID FROM Projeto WHERE ProjetoID = ?", (projeto_id,))
            if not cursor.fetchone():
                return jsonify({"status": "error", "message": "Projeto não encontrado"}), 404

            # Atualizar projeto
            cursor.execute("""
                UPDATE Projeto SET 
                    NomeProjeto = ?, 
                    DadosGX = ?, 
                    ServidorWIN = ?, 
                    UsuarioDB = ?, 
                    SenhaDB = ?, 
                    BancoHomo = ?, 
                    PontoFocal = ?, 
                    ConsultorLider = ?, 
                    LiderProjeto = ?
                WHERE ProjetoID = ?
            """, (
                nome_projeto,
                dados_gx,
                servidor_win,
                usuario_db,
                senha_db,
                banco_homo,
                ponto_focal,
                consultor_lider,
                lider_projeto,
                projeto_id
            ))
            print(f"DEBUG: Projeto {projeto_id} atualizado")

        else:  # NOVO projeto
            print("DEBUG: Criando novo projeto")
            
            # Inserir novo projeto
            cursor.execute("""
                INSERT INTO Projeto (
                    NomeProjeto, DadosGX, ServidorWIN, UsuarioDB, 
                    SenhaDB, BancoHomo, PontoFocal, ConsultorLider, LiderProjeto
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                nome_projeto,
                dados_gx,
                servidor_win,
                usuario_db,
                senha_db,
                banco_homo,
                ponto_focal,
                consultor_lider,
                lider_projeto
            ))
            print(f"DEBUG: Projeto {nome_projeto} inserido na tabela Projeto")

            # Obter o ID do novo projeto
            cursor.execute("SELECT MAX(ProjetoID) FROM Projeto WHERE NomeProjeto = ?", (nome_projeto,))
            result = cursor.fetchone()
            novo_projeto_id = result[0] if result else None
            
            if not novo_projeto_id:
                # Tentar método alternativo
                cursor.execute("SELECT ProjetoID FROM Projeto WHERE NomeProjeto = ?", (nome_projeto,))
                result = cursor.fetchone()
                novo_projeto_id = result[0] if result else None

            print(f"DEBUG: Novo projeto ID: {novo_projeto_id}")

            if not novo_projeto_id:
                conn.rollback()
                return jsonify({"status": "error", "message": "Falha ao obter ID do novo projeto"}), 500

        conn.commit()
        logger.info(f"Projeto {'atualizado' if projeto_id else 'criado'} com sucesso: {nome_projeto}")
        return jsonify({"status": "success", "message": "Projeto salvo com sucesso!"}), 200

    except Exception as e:
        logger.error(f"Erro ao salvar projeto: {e}")
        if conn:
            conn.rollback()
        return jsonify({"status": "error", "message": f"Erro ao salvar projeto: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@projetos_bp.route("/excluir_projeto/<int:projeto_id>", methods=["POST"])
def excluir_projeto(projeto_id):
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"status": "error", "message": "Acesso não autorizado"}), 403

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()

        # Verificar se o projeto existe
        cursor.execute("SELECT NomeProjeto FROM Projeto WHERE ProjetoID = ?", (projeto_id,))
        projeto = cursor.fetchone()
        if not projeto:
            return jsonify({"status": "error", "message": "Projeto não encontrado"}), 404
        
        # Verificar se existem escopos vinculados a este projeto
        cursor.execute("SELECT COUNT(*) FROM Escopo WHERE ProjetoID = ?", (projeto_id,))
        resultado = cursor.fetchone()
        escopos_vinculados = resultado[0] if resultado else 0
        
        if escopos_vinculados > 0:
            return jsonify({
                "status": "error", 
                "message": f"Não é possível excluir o projeto. Existem {escopos_vinculados} escopo(s) vinculado(s) a este projeto."
            }), 400
        
        # Excluir projeto
        cursor.execute("DELETE FROM Projeto WHERE ProjetoID = ?", (projeto_id,))
        deleted = cursor.rowcount

        conn.commit()

        if deleted > 0:
            logger.info(f"Projeto {projeto_id} excluído com sucesso")
            return jsonify({"status": "success", "message": "Projeto excluído com sucesso!"})
        else:
            return jsonify({"status": "error", "message": "Projeto não encontrado."}), 404

    except Exception as e:
        logger.error(f"Erro ao excluir projeto {projeto_id}: {e}")
        if conn:
            conn.rollback()
        return jsonify({"status": "error", "message": f"Erro interno ao excluir projeto: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@projetos_bp.route("/obter_projeto/<int:projeto_id>")
def obter_projeto(projeto_id):
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
                ProjetoID,
                NomeProjeto,
                DadosGX,
                ServidorWIN,
                UsuarioDB,
                SenhaDB,
                BancoHomo,
                PontoFocal,
                ConsultorLider,
                LiderProjeto
            FROM Projeto 
            WHERE ProjetoID = ?
        """, (projeto_id,))
        
        projeto = cursor.fetchone()
        
        if not projeto:
            return jsonify({"success": False, "message": "Projeto não encontrado"})
        
        projeto_dict = {
            'ProjetoID': projeto.ProjetoID,
            'NomeProjeto': projeto.NomeProjeto,
            'DadosGX': projeto.DadosGX,
            'ServidorWIN': projeto.ServidorWIN,
            'UsuarioDB': projeto.UsuarioDB,
            'SenhaDB': projeto.SenhaDB,
            'BancoHomo': projeto.BancoHomo,
            'PontoFocal': projeto.PontoFocal,
            'ConsultorLider': projeto.ConsultorLider,
            'LiderProjeto': projeto.LiderProjeto
        }
        
        return jsonify({"success": True, "projeto": projeto_dict})

    except Exception as e:
        logger.error(f"Erro ao obter projeto: {e}")
        return jsonify({"success": False, "message": f"Erro ao obter projeto: {str(e)}"})
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()