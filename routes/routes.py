# auth/routes.py
from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from db.connection import conectar_banco
from logger import logger
import hashlib

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        # Conectar ao banco de dados de autenticação
        conn = conectar_banco()
        if not conn:
            flash("Erro de conexão com o banco de dados", "error")
            return render_template("login.html")

        try:
            cursor = conn.cursor()

            # Buscar usuário pela senha em texto (ou pelo hash MD5)
            # Primeiro, tenta pela senha em texto
            cursor.execute(
                "SELECT Codigo, Usuario, CNPJ, Empresa, Ativo, Senha, SenhaHash, adm FROM Usuarios WHERE Usuario = ? AND Senha = ? AND Ativo = 'S'",
                (username, password),
            )
            usuario = cursor.fetchone()

            # Se não encontrou, tenta pelo hash MD5
            if not usuario:
                senha_hash = hashlib.md5(password.encode()).hexdigest()
                cursor.execute(
                    "SELECT Codigo, Usuario, CNPJ, Empresa, Ativo, Senha, SenhaHash, adm FROM Usuarios WHERE Usuario = ? AND SenhaHash = ? AND Ativo = 'S'",
                    (username, senha_hash),
                )
                usuario = cursor.fetchone()

            if usuario:
                # Buscar empresas do usuário
                cursor.execute(
                    """
                    SELECT e.Id, e.Nome, e.DadosGX 
                    FROM Empresa e
                    INNER JOIN UsuarioEmpresa ue ON e.Id = ue.EmpresaId
                    WHERE ue.UsuarioCodigo = ?
                """,
                    (usuario.Codigo,),
                )
                empresas = cursor.fetchall()

                if len(empresas) == 0:
                    flash("Usuário não possui empresas vinculadas.", "error")
                    return render_template("login.html")

                # Se houver apenas uma empresa, define na sessão
                if len(empresas) == 1:
                    session["usuario"] = {
                        "codigo": usuario.Codigo,
                        "usuario": usuario.Usuario,
                        "cnpj": usuario.CNPJ,
                        "empresa": usuario.Empresa,
                        "ativo": usuario.Ativo,
                        "adm": usuario.adm,
                    }
                    session["empresa_selecionada"] = {
                        "id": empresas[0].Id,
                        "nome": empresas[0].Nome,
                        "dados_gx": empresas[0].DadosGX,
                    }
                    return redirect(url_for("dashboard.dashboard"))

                # Se houver mais de uma, redireciona para seleção
                else:
                    session["usuario_temp"] = {
                        "codigo": usuario.Codigo,
                        "usuario": usuario.Usuario,
                        "cnpj": usuario.CNPJ,
                        "empresa": usuario.Empresa,
                        "ativo": usuario.Ativo,
                        "adm": usuario.adm,
                    }
                    session["empresas_disponiveis"] = [
                        {"id": emp.Id, "nome": emp.Nome, "dados_gx": emp.DadosGX}
                        for emp in empresas
                    ]
                    return redirect(url_for("auth.selecionar_empresa"))

            else:
                flash("Usuário ou senha inválidos", "error")
                return render_template("login.html")

        except Exception as e:
            logger.error(f"Erro durante o login: {e}")
            flash("Erro interno do servidor", "error")
            return render_template("login.html")
        finally:
            conn.close()

    return render_template("login.html")


@auth_bp.route("/selecionar_empresa", methods=["GET", "POST"])
def selecionar_empresa():
    if "usuario_temp" not in session:
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        empresa_id = request.form.get("empresa_id")

        # Encontrar a empresa selecionada
        empresa_selecionada = None
        for emp in session["empresas_disponiveis"]:
            if str(emp["id"]) == empresa_id:
                empresa_selecionada = emp
                break

        if empresa_selecionada:
            # Definir sessão completa
            session["usuario"] = session["usuario_temp"]
            session["empresa_selecionada"] = empresa_selecionada

            # Limpar dados temporários
            session.pop("usuario_temp", None)
            session.pop("empresas_disponiveis", None)

            return redirect(url_for("dashboard.dashboard"))
        else:
            flash("Empresa selecionada inválida", "error")

    return render_template(
        "selecionar_empresa.html", empresas=session["empresas_disponiveis"]
    )


@auth_bp.route("/trocar_empresa")
def trocar_empresa():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    # Manter dados do usuário, limpar empresa selecionada
    usuario_temp = session["usuario"]
    session.clear()
    session["usuario_temp"] = usuario_temp

    # Buscar empresas disponíveis novamente
    conn = conectar_banco()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT e.Id, e.Nome, e.DadosGX 
                FROM Empresa e
                INNER JOIN UsuarioEmpresa ue ON e.Id = ue.EmpresaId
                WHERE ue.UsuarioCodigo = ?
            """,
                (usuario_temp["codigo"],),
            )
            empresas = cursor.fetchall()
            session["empresas_disponiveis"] = [
                {"id": emp.Id, "nome": emp.Nome, "dados_gx": emp.DadosGX}
                for emp in empresas
            ]
        except Exception as e:
            logger.error(f"Erro ao buscar empresas: {e}")
            flash("Erro ao carregar lista de empresas", "error")
            return redirect(url_for("dashboard.dashboard"))
        finally:
            conn.close()

    return redirect(url_for("auth.selecionar_empresa"))


@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Logout realizado com sucesso", "success")
    return redirect(url_for("auth.login"))