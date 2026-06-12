from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from db.connection import conectar_banco
from auth.security import verificar_senha, migrar_para_hash
from logger import logger

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    # VERIFICAÇÃO CRÍTICA: Se o usuário já está logado, redireciona para o dashboard
    if "usuario" in session:
        print("Usuário já logado, redirecionando para dashboard")
        return redirect(url_for("dashboard.dashboard"))

    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        # Validação básica dos campos
        if not username or not password:
            flash("Usuário e senha são obrigatórios", "error")
            return render_template("login.html")

        sucesso, resultado = autenticar_usuario(username, password)
        if sucesso:
            session["usuario"] = resultado
            session.permanent = True  # Tornar a sessão permanente
            logger.info(f"Login bem-sucedido para usuário: {username}")
            return redirect(url_for("dashboard.dashboard"))
        else:
            flash(resultado, "error")
            logger.warning(f"Tentativa de login falhou para usuário: {username}")

    return render_template("login.html")


def autenticar_usuario(username, password):
    conexao = conectar_banco()
    if not conexao:
        return False, "Erro de conexão com o banco de dados"

    try:
        cursor = conexao.cursor()
        cursor.execute(
            "SELECT Usuario, Empresa, CNPJ, Senha, SenhaHash, adm, DadosGx "
            "FROM Usuarios WHERE Usuario = ? AND Ativo = 'S'",
            (username,),
        )
        resultado = cursor.fetchone()

        if not resultado:
            logger.warning(f"Usuário não encontrado ou inativo: {username}")
            return False, "Usuário não encontrado ou inativo"

        senha_ok = False

        # Verificar senha hash primeiro (mais seguro)
        if getattr(resultado, "SenhaHash", None):
            senha_ok = verificar_senha(password, resultado.SenhaHash)
            # Se não passou no hash mas tem senha legada, tenta a legada
            if not senha_ok and resultado.Senha:
                senha_ok = password == resultado.Senha
                if senha_ok:
                    # Migrar para hash se a senha legada estiver correta
                    migrar_para_hash(username, password)
        else:
            # Senha legada
            senha_ok = password == resultado.Senha
            if senha_ok:
                # Migrar para hash
                migrar_para_hash(username, password)

        if senha_ok:
            logger.info(f"Autenticação bem-sucedida para: {username}")
            return True, {
                "usuario": resultado.Usuario,
                "empresa": resultado.Empresa,
                "cnpj": resultado.CNPJ,
                "adm": resultado.adm,
                "DadosGx": resultado.DadosGx,
            }
        else:
            logger.warning(f"Senha incorreta para usuário: {username}")
            return False, "Senha incorreta"

    except Exception as e:
        logger.error(f"Erro na autenticação do usuário {username}: {e}")
        return False, "Erro interno do sistema"
    finally:
        if conexao:
            conexao.close()


@auth_bp.route("/logout")
def logout():
    usuario = session.get("usuario", {}).get("usuario", "Desconhecido")
    session.clear()  # Limpa toda a sessão
    logger.info(f"Logout realizado para usuário: {usuario}")
    flash("Logout realizado com sucesso", "success")
    return redirect(url_for("auth.login"))
