from flask import (
    Blueprint, render_template, redirect, url_for, 
    session, request, jsonify, flash
)
from db.connection import conectar_banco
from logger import logger
from auth.security import hash_senha
from utils.credential_crypto import preparar_login_para_banco, revelar_login_banco
from utils.usuario_login_db import garantir_coluna_login_hash
from utils.password_reset import garantir_coluna_email, email_valido
from utils.login_bloqueio import garantir_colunas_bloqueio_login, desbloquear_conta

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
        garantir_coluna_login_hash(cursor, conn)
        garantir_coluna_email(cursor, conn)
        garantir_colunas_bloqueio_login(cursor, conn)

        # Buscar usuários
        cursor.execute("""
            SELECT 
                u.UsuarioID,
                u.UsuarioNome,
                u.Email,
                u.Ativo,
                u.Adm,
                u.ContaBloqueada
            FROM Usuarios u
            ORDER BY u.UsuarioID
        """)
        
        usuarios_raw = cursor.fetchall()
        
        # Converter para lista de dicionários (nome descriptografado só na memória/UI)
        usuarios_list = []
        for user in usuarios_raw:
            usuario_dict = {
                'UsuarioID': user.UsuarioID,
                'UsuarioNome': revelar_login_banco(user.UsuarioNome),
                'Email': (user.Email or "").strip(),
                'Ativo': user.Ativo,
                'Adm': user.Adm,
                'ContaBloqueada': bool(getattr(user, 'ContaBloqueada', False)),
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

        usuarios_list.sort(key=lambda u: (u['UsuarioNome'] or '').lower())

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
    usuario_nome = (data.get("usuario_nome") or "").strip()
    email = (data.get("email") or "").strip()
    ativo = data.get("ativo")
    senha = data.get("senha")
    adm = data.get("adm")
    desbloquear = bool(data.get("desbloquear_senha"))
    projetos_selecionados = data.get("projetos", [])

    if not usuario_nome:
        return jsonify({"status": "error", "message": "Nome do usuário é obrigatório"}), 400

    if email and not email_valido(email):
        return jsonify({"status": "error", "message": "E-mail inválido"}), 400

    nome_cifrado, nome_hash = preparar_login_para_banco(usuario_nome)

    logger.info(
        "Salvando usuário id=%s ativo=%s adm=%s senha_alterada=%s projetos=%s",
        usuario_id,
        ativo,
        adm,
        bool(senha),
        len(projetos_selecionados or []),
    )

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()
        garantir_coluna_login_hash(cursor, conn)
        garantir_coluna_email(cursor, conn)
        garantir_colunas_bloqueio_login(cursor, conn)

        # Unicidade pelo hash (não pelo texto cifrado)
        if usuario_id:
            cursor.execute(
                """
                SELECT TOP 1 UsuarioID FROM Usuarios
                WHERE UsuarioNomeHash = ? AND UsuarioID <> ?
                """,
                (nome_hash, int(usuario_id)),
            )
        else:
            cursor.execute(
                """
                SELECT TOP 1 UsuarioID FROM Usuarios
                WHERE UsuarioNomeHash = ?
                """,
                (nome_hash,),
            )
        if cursor.fetchone():
            return jsonify({
                "status": "error",
                "message": "Já existe um usuário com este login.",
            }), 400

        if usuario_id:  # EDITANDO usuário existente
            usuario_id = int(usuario_id)

            if senha:
                senha_hash = hash_senha(senha)
                cursor.execute("""
                    UPDATE Usuarios
                    SET UsuarioNome=?, UsuarioNomeHash=?, Email=?, Ativo=?, SenhaHash=?, Adm=?
                    WHERE UsuarioID=?
                """, (nome_cifrado, nome_hash, email or None, ativo, senha_hash, adm, usuario_id))
                # Nova senha limpa o bloqueio
                desbloquear_conta(cursor, None, usuario_id)
            else:
                cursor.execute("""
                    UPDATE Usuarios
                    SET UsuarioNome=?, UsuarioNomeHash=?, Email=?, Ativo=?, Adm=?
                    WHERE UsuarioID=?
                """, (nome_cifrado, nome_hash, email or None, ativo, adm, usuario_id))
                if desbloquear:
                    desbloquear_conta(cursor, None, usuario_id)

            # Atualizar projetos associados
            cursor.execute("DELETE FROM UsuarioProjeto WHERE UsuarioID = ?", (usuario_id,))

            for projeto_id in projetos_selecionados:
                projeto_id_int = int(projeto_id)
                cursor.execute(
                    "INSERT INTO UsuarioProjeto (UsuarioID, ProjetoID) VALUES (?, ?)",
                    (usuario_id, projeto_id_int),
                )

        else:  # NOVO usuário
            if not senha:
                return jsonify({
                    "status": "error",
                    "message": "Senha é obrigatória para novo usuário",
                }), 400

            senha_hash = hash_senha(senha)

            cursor.execute("""
                INSERT INTO Usuarios (UsuarioNome, UsuarioNomeHash, Email, Ativo, SenhaHash, Adm)
                OUTPUT INSERTED.UsuarioID
                VALUES (?, ?, ?, ?, ?, ?)
            """, (nome_cifrado, nome_hash, email or None, ativo, senha_hash, adm))

            result = cursor.fetchone()
            novo_usuario_id = result[0] if result else None

            if not novo_usuario_id:
                conn.rollback()
                return jsonify({"status": "error", "message": "Falha ao obter ID do novo usuário"}), 500

            for projeto_id in projetos_selecionados:
                projeto_id_int = int(projeto_id)
                cursor.execute(
                    "INSERT INTO UsuarioProjeto (UsuarioID, ProjetoID) VALUES (?, ?)",
                    (novo_usuario_id, projeto_id_int),
                )

        conn.commit()
        logger.info(f"Usuário {'atualizado' if usuario_id else 'criado'} com sucesso: id={usuario_id or 'novo'}")
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
    """Obtém os projetos associados a um usuário (somente admin)."""
    if "usuario" not in session or not session["usuario"].get("adm"):
        return jsonify({"error": "Não autorizado"}), 403

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