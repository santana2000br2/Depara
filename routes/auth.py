from flask import (
    Blueprint, render_template, redirect, url_for, 
    session, request, flash, jsonify
)
from db.connection import conectar_banco
from logger import logger
import hashlib

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        print(f"DEBUG: Tentativa de login - Usuário: {username}")

        conn = conectar_banco()
        if not conn:
            flash("Erro de conexão com o banco de dados", "error")
            return render_template("login.html")

        cursor = conn.cursor()
        
        try:
            # Buscar usuário
            cursor.execute("""
                SELECT 
                    UsuarioID, 
                    UsuarioNome, 
                    Ativo, 
                    SenhaHash, 
                    Adm 
                FROM Usuarios 
                WHERE UsuarioNome = ?
            """, (username,))
            
            user = cursor.fetchone()
            
            if user:
                usuario_id, usuario_nome, ativo, senha_hash, adm = user
                
                print(f"DEBUG: Usuário encontrado - ID: {usuario_id}, Nome: {usuario_nome}")
                print(f"DEBUG: Ativo: {ativo}, Admin: {adm}")
                print(f"DEBUG: Hash no BD: {senha_hash}")
                
                if ativo:
                    # Verificar senha
                    senha_hash_input = hashlib.md5(password.encode()).hexdigest()
                    print(f"DEBUG: Hash da senha digitada: {senha_hash_input}")
                    
                    if senha_hash and senha_hash.strip() == senha_hash_input:
                        print("DEBUG: Senha CORRETA!")
                        
                        # Buscar projetos do usuário
                        cursor.execute("""
                            SELECT 
                                p.ProjetoID,
                                p.NomeProjeto,
                                p.DadosGX
                            FROM UsuarioProjeto up
                            INNER JOIN Projeto p ON up.ProjetoID = p.ProjetoID
                            WHERE up.UsuarioID = ?
                        """, (usuario_id,))
                        
                        projetos = cursor.fetchall()
                        print(f"DEBUG: Número de projetos encontrados: {len(projetos)}")
                        
                        for proj in projetos:
                            print(f"DEBUG: Projeto - {proj.NomeProjeto}, Banco - {proj.DadosGX}")
                        
                        session["usuario"] = {
                            "usuario_id": usuario_id,
                            "usuario": usuario_nome,
                            "adm": adm
                        }
                        
                        if len(projetos) == 0:
                            print("DEBUG: NENHUM PROJETO ENCONTRADO")
                            flash("Usuário não possui projetos associados", "error")
                        elif len(projetos) == 1:
                            projeto = projetos[0]
                            session["projeto_selecionado"] = {
                                "ProjetoID": projeto.ProjetoID,
                                "NomeProjeto": projeto.NomeProjeto,
                                "DadosGX": projeto.DadosGX
                            }
                            print("DEBUG: Um projeto encontrado - Redirecionando para dashboard")
                            return redirect(url_for("dashboard.dashboard"))
                        else:
                            session["projetos_disponiveis"] = [
                                {
                                    "ProjetoID": proj.ProjetoID,
                                    "NomeProjeto": proj.NomeProjeto,
                                    "DadosGX": proj.DadosGX
                                }
                                for proj in projetos
                            ]
                            print("DEBUG: Múltiplos projetos - Redirecionando para seleção")
                            return redirect(url_for("auth.selecionar_projeto"))
                    else:
                        print("DEBUG: Senha INCORRETA!")
                        flash("Usuário ou senha incorretos", "error")
                else:
                    print("DEBUG: Usuário INATIVO!")
                    flash("Usuário inativo", "error")
            else:
                print("DEBUG: USUÁRIO NÃO ENCONTRADO!")
                flash("Usuário não encontrado", "error")
                
        except Exception as e:
            print(f"DEBUG: ERRO NO LOGIN: {e}")
            logger.error(f"Erro no login: {e}")
            flash("Erro interno no sistema", "error")
        finally:
            cursor.close()
            conn.close()

    return render_template("login.html")

@auth_bp.route("/selecionar_projeto", methods=["GET", "POST"])
def selecionar_projeto():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))
    
    if "projetos_disponiveis" not in session:
        flash("Nenhum projeto disponível", "error")
        return redirect(url_for("auth.login"))
    
    if request.method == "POST":
        projeto_id = request.form.get("projeto_id")
        
        # Encontrar o projeto selecionado
        projeto_selecionado = next(
            (proj for proj in session["projetos_disponiveis"] if str(proj["ProjetoID"]) == projeto_id),
            None
        )
        
        if projeto_selecionado:
            session["projeto_selecionado"] = projeto_selecionado
            session.pop("projetos_disponiveis", None)
            return redirect(url_for("dashboard.dashboard"))
        else:
            flash("Projeto não encontrado", "error")
    
    return render_template("selecionar_projeto.html", 
                         projetos=session["projetos_disponiveis"])

@auth_bp.route("/trocar_projeto")
def trocar_projeto():
    """Permite ao usuário trocar de projeto"""
    if "usuario" not in session:
        return redirect(url_for("auth.login"))
    
    usuario_id = session["usuario"]["usuario_id"]
    
    conn = conectar_banco()
    if not conn:
        flash("Erro de conexão com o banco", "error")
        return redirect(url_for("dashboard.dashboard"))
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                p.ProjetoID,
                p.NomeProjeto,
                p.DadosGX
            FROM UsuarioProjeto up
            INNER JOIN Projeto p ON up.ProjetoID = p.ProjetoID
            WHERE up.UsuarioID = ?
        """, (usuario_id,))
        
        projetos = cursor.fetchall()
        
        session["projetos_disponiveis"] = [
            {
                "ProjetoID": proj.ProjetoID,
                "NomeProjeto": proj.NomeProjeto,
                "DadosGX": proj.DadosGX
            }
            for proj in projetos
        ]
        
        return render_template("selecionar_projeto.html", 
                             projetos=session["projetos_disponiveis"])
        
    except Exception as e:
        logger.error(f"Erro ao buscar projetos: {e}")
        flash("Erro ao carregar projetos", "error")
        return redirect(url_for("dashboard.dashboard"))
    finally:
        cursor.close()
        conn.close()

@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Logout realizado com sucesso", "success")
    return redirect(url_for("auth.login"))