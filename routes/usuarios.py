from flask import (
    Blueprint, render_template, redirect, url_for, 
    session, request, jsonify, flash
)
from db.connection import conectar_banco
from logger import logger
import hashlib

usuarios_bp = Blueprint("usuarios", __name__)

@usuarios_bp.route("/gerenciar_usuarios")
def gerenciar_usuarios():
    if "usuario" not in session or not session["usuario"].get("adm"):
        flash("Acesso não autorizado", "error")
        return redirect(url_for("dashboard.dashboard"))

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            flash("Erro de conexão com o banco de dados", "error")
            return render_template("usuarios.html", usuarios=[], projetos=[])

        cursor = conn.cursor()

        # Buscar usuários
        cursor.execute("""
            SELECT 
                u.UsuarioID,
                u.UsuarioNome,
                u.Ativo,
                u.Adm
            FROM Usuarios u
            ORDER BY u.UsuarioNome
        """)
        
        usuarios_raw = cursor.fetchall()
        
        # Converter para lista de dicionários
        usuarios_list = []
        for user in usuarios_raw:
            usuario_dict = {
                'UsuarioID': user.UsuarioID,
                'UsuarioNome': user.UsuarioNome,
                'Ativo': user.Ativo,
                'Adm': user.Adm,
                'projetos_associados': []
            }
            
            # Buscar projetos associados
            cursor.execute("""
                SELECT 
                    p.ProjetoID,
                    p.NomeProjeto
                FROM UsuarioProjeto up
                INNER JOIN Projeto p ON up.ProjetoID = p.ProjetoID
                WHERE up.UsuarioID = ?
            """, (user.UsuarioID,))
            
            projetos_assoc = cursor.fetchall()
            usuario_dict['projetos_associados'] = [proj.NomeProjeto for proj in projetos_assoc]
            
            usuarios_list.append(usuario_dict)

        # Buscar todos os projetos para o formulário
        cursor.execute("""
            SELECT 
                ProjetoID,
                NomeProjeto,
                DadosGX
            FROM Projeto 
            ORDER BY NomeProjeto
        """)
        projetos_raw = cursor.fetchall()
        todos_projetos = [
            {
                'ProjetoID': proj.ProjetoID, 
                'NomeProjeto': proj.NomeProjeto,
                'DadosGX': proj.DadosGX
            } 
            for proj in projetos_raw
        ]

        return render_template(
            "usuarios.html",
            usuarios=usuarios_list,
            todos_projetos=todos_projetos,
            usuario=session["usuario"]
        )

    except Exception as e:
        logger.error(f"Erro ao carregar usuários: {e}")
        flash(f"Erro ao carregar lista de usuários: {str(e)}", "error")
        return render_template("usuarios.html", usuarios=[], projetos=[])
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@usuarios_bp.route("/salvar_usuario", methods=["POST"])
def salvar_usuario():
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"status": "error", "message": "Acesso não autorizado"}), 403

    data = request.get_json()
    usuario_id = data.get("usuario_id")
    usuario_nome = data.get("usuario_nome")
    ativo = data.get("ativo")
    senha = data.get("senha")
    adm = data.get("adm")
    projetos_selecionados = data.get("projetos", [])

    print(f"DEBUG: Dados recebidos - usuario_id: {usuario_id}, usuario_nome: {usuario_nome}, ativo: {ativo}, adm: {adm}, projetos: {projetos_selecionados}")

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()

        if usuario_id:  # EDITANDO usuário existente
            usuario_id = int(usuario_id)
            print(f"DEBUG: Editando usuário ID: {usuario_id}")
            
            if senha:  # Se senha foi fornecida, atualiza
                senha_hash = hashlib.md5(senha.encode()).hexdigest()
                cursor.execute("""
                    UPDATE Usuarios 
                    SET UsuarioNome=?, Ativo=?, Senha=?, SenhaHash=?, Adm=?
                    WHERE UsuarioID=?
                """, (usuario_nome, ativo, senha, senha_hash, adm, usuario_id))
                print(f"DEBUG: Senha atualizada para usuário {usuario_id}")
            else:  # Se não foi fornecida senha, mantém a senha atual
                cursor.execute("""
                    UPDATE Usuarios 
                    SET UsuarioNome=?, Ativo=?, Adm=?
                    WHERE UsuarioID=?
                """, (usuario_nome, ativo, adm, usuario_id))
                print(f"DEBUG: Dados atualizados sem senha para usuário {usuario_id}")

            # Atualizar projetos associados
            cursor.execute("DELETE FROM UsuarioProjeto WHERE UsuarioID = ?", (usuario_id,))
            print(f"DEBUG: Projetos antigos removidos para usuário {usuario_id}")
            
            for projeto_id in projetos_selecionados:
                projeto_id_int = int(projeto_id)
                cursor.execute(
                    "INSERT INTO UsuarioProjeto (UsuarioID, ProjetoID) VALUES (?, ?)",
                    (usuario_id, projeto_id_int),
                )
                print(f"DEBUG: Projeto {projeto_id_int} associado ao usuário {usuario_id}")

        else:  # NOVO usuário
            print("DEBUG: Criando novo usuário")
            if not senha:
                return jsonify({
                    "status": "error",
                    "message": "Senha é obrigatória para novo usuário",
                }), 400

            # Para novo usuário, criar hash da senha e inserir tanto Senha quanto SenhaHash
            senha_hash = hashlib.md5(senha.encode()).hexdigest()

            # Inserir na tabela Usuarios (incluindo a coluna Senha)
            cursor.execute("""
                INSERT INTO Usuarios (UsuarioNome, Ativo, Senha, SenhaHash, Adm)
                VALUES (?, ?, ?, ?, ?)
            """, (usuario_nome, ativo, senha, senha_hash, adm))
            print(f"DEBUG: Usuário {usuario_nome} inserido na tabela Usuarios")

            # Obter o ID do novo usuário - método mais confiável
            cursor.execute("SELECT MAX(UsuarioID) FROM Usuarios WHERE UsuarioNome = ?", (usuario_nome,))
            result = cursor.fetchone()
            novo_usuario_id = result[0] if result else None
            
            if not novo_usuario_id:
                # Tentar método alternativo
                cursor.execute("SELECT UsuarioID FROM Usuarios WHERE UsuarioNome = ?", (usuario_nome,))
                result = cursor.fetchone()
                novo_usuario_id = result[0] if result else None

            print(f"DEBUG: Novo usuário ID: {novo_usuario_id}")

            if not novo_usuario_id:
                conn.rollback()
                return jsonify({"status": "error", "message": "Falha ao obter ID do novo usuário"}), 500

            # Adicionar projetos associados
            for projeto_id in projetos_selecionados:
                projeto_id_int = int(projeto_id)
                cursor.execute(
                    "INSERT INTO UsuarioProjeto (UsuarioID, ProjetoID) VALUES (?, ?)",
                    (novo_usuario_id, projeto_id_int),
                )
                print(f"DEBUG: Projeto {projeto_id_int} associado ao novo usuário {novo_usuario_id}")

        conn.commit()
        logger.info(f"Usuário {'atualizado' if usuario_id else 'criado'} com sucesso: {usuario_nome}")
        return jsonify({"status": "success", "message": "Usuário salvo com sucesso!"}), 200

    except Exception as e:
        logger.error(f"Erro ao salvar usuário: {e}")
        if conn:
            conn.rollback()
        return jsonify({"status": "error", "message": f"Erro ao salvar usuário: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@usuarios_bp.route("/obter_projetos_usuario/<int:usuario_id>")
def obter_projetos_usuario(usuario_id):
    """Obtém os projetos associados a um usuário"""
    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify([])

        cursor = conn.cursor()
        
        cursor.execute(
            """
            SELECT ProjetoID 
            FROM UsuarioProjeto 
            WHERE UsuarioID = ?
            """,
            (usuario_id,)
        )
        
        projetos = [row.ProjetoID for row in cursor.fetchall()]
        
        return jsonify(projetos)
        
    except Exception as e:
        logger.error(f"Erro ao obter projetos do usuário: {e}")
        return jsonify([])
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

@usuarios_bp.route("/excluir_usuario/<int:usuario_id>", methods=["POST"])
def excluir_usuario(usuario_id):
    if "usuario" not in session:
        return (
            jsonify(
                {"status": "error", "message": "Sessão expirada. Faça login novamente."}
            ),
            401,
        )

    if not session["usuario"].get("adm") == 1:
        return jsonify({"status": "error", "message": "Acesso não autorizado."}), 403

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()

        # Primeiro excluir as associações com projetos
        cursor.execute("DELETE FROM UsuarioProjeto WHERE UsuarioID = ?", (usuario_id,))

        # Depois excluir o usuário
        cursor.execute("DELETE FROM Usuarios WHERE UsuarioID = ?", (usuario_id,))
        deleted = cursor.rowcount

        conn.commit()

        if deleted > 0:
            logger.info(
                f"Usuário {usuario_id} excluído com sucesso por {session['usuario'].get('usuario')}"
            )
            return jsonify(
                {"status": "success", "message": "Usuário excluído com sucesso!"}
            )
        else:
            return (
                jsonify({"status": "error", "message": "Usuário não encontrado."}),
                404,
            )

    except Exception as e:
        logger.error(f"Erro ao excluir usuário {usuario_id}: {e}")
        if conn:
            conn.rollback()
        return (
            jsonify(
                {
                    "status": "error",
                    "message": f"Erro interno ao excluir usuário: {str(e)}",
                }
            ),
            500,
        )
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()