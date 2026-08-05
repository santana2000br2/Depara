from flask import (
    Blueprint, render_template, redirect, url_for,
    session, request, flash
)
from config import Config
from db.connection import conectar_banco
from logger import logger
from auth.security import verificar_senha, hash_senha
from utils.credential_crypto import (
    esta_criptografado,
    hash_login_busca,
    preparar_login_para_banco,
    revelar_login_banco,
)
from utils.usuario_login_db import garantir_coluna_login_hash
from utils.recaptcha import recaptcha_habilitado, validar_recaptcha
from utils.captcha_local import gerar_desafio_captcha, validar_captcha_local
from utils.password_reset import (
    garantir_coluna_email,
    garantir_tabela_reset_senha,
    criar_token_reset,
    buscar_token_valido,
    marcar_token_usado,
)
from utils.email_config import obter_smtp_config
from utils.email_service import resolver_config_smtp, enviar_email
from utils.login_bloqueio import (
    MAX_TENTATIVAS_SENHA,
    garantir_colunas_bloqueio_login,
    conta_esta_bloqueada,
    resetar_tentativas_login,
    registrar_falha_senha,
    notificar_admins_bloqueio,
    desbloquear_conta,
)


def _contexto_login():
    """Contexto comum do template de login (Google reCAPTCHA ou captcha local)."""
    google_ativo = recaptcha_habilitado()
    ctx = {
        "recaptcha_ativo": google_ativo,
        "recaptcha_site_key": Config.RECAPTCHA_SITE_KEY if google_ativo else "",
        "captcha_local": None,
    }
    if not google_ativo:
        ctx["captcha_local"] = gerar_desafio_captcha()
    return ctx

auth_bp = Blueprint("auth", __name__)

_CAMPOS_CREDENCIAL = frozenset({
    "senhaproducao",
    "senhahomologacao",
    "senhaProducao",
    "senhaHomologacao",
    "usuarioProducao",
    "usuariohomologacao",
})


def projeto_para_sessao(projeto):
    """Retorna metadados do projeto sem credenciais (senhas/usuários DB não vão no cookie)."""
    if not projeto:
        return None
    return {
        k: v for k, v in projeto.items()
        if k not in _CAMPOS_CREDENCIAL
        and "senha" not in k.lower()
        and k not in ("usuarioProducao", "usuariohomologacao")
    }


def buscar_projeto_completo(projeto_id):
    """Busca metadados de um projeto pelo ID (sem credenciais — uso em sessão)."""
    conn = conectar_banco()
    if not conn:
        logger.error("Não foi possível conectar ao banco para buscar projeto completo")
        return None

    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT
                ProjetoID,
                NomeProjeto,
                DadosGX,
                servidorproducao,
                servidorhomologacao,
                BancoHomo,
                PontoFocal,
                ConsultorLider,
                LiderProjeto,
                migrador,
                bancoProducao,
                TipoWindowsWorkflow,
                TipoWorkflowWorkflow,
                TipoArquivoWorkflow,
                ImportacaoLiberada
            FROM Projeto
            WHERE ProjetoID = ?
        """, (projeto_id,))

        projeto = cursor.fetchone()
        if projeto:
            tipo_projeto = ""
            if getattr(projeto, "TipoArquivoWorkflow", None):
                tipo_projeto = "arquivo_workflow"
            elif getattr(projeto, "TipoWorkflowWorkflow", None):
                tipo_projeto = "workflow_workflow"
            elif getattr(projeto, "TipoWindowsWorkflow", None):
                tipo_projeto = "windows_workflow"
            return {
                "ProjetoID": projeto.ProjetoID,
                "NomeProjeto": projeto.NomeProjeto,
                "DadosGX": projeto.DadosGX,
                "servidorproducao": projeto.servidorproducao,
                "servidorhomologacao": projeto.servidorhomologacao,
                "BancoHomo": projeto.BancoHomo,
                "PontoFocal": projeto.PontoFocal,
                "ConsultorLider": projeto.ConsultorLider,
                "LiderProjeto": projeto.LiderProjeto,
                "migrador": projeto.migrador,
                "bancoProducao": projeto.bancoProducao,
                "TipoProjeto": tipo_projeto,
                "TipoArquivoWorkflow": bool(getattr(projeto, "TipoArquivoWorkflow", False)),
                "ImportacaoLiberada": bool(getattr(projeto, "ImportacaoLiberada", False)),
            }
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar projeto completo: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def _resumo_projeto_lista(proj):
    return {
        "ProjetoID": proj.ProjetoID,
        "NomeProjeto": proj.NomeProjeto,
        "DadosGX": proj.DadosGX,
    }


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")

        if recaptcha_habilitado():
            token = request.form.get("g-recaptcha-response", "")
            if not validar_recaptcha(token, request.remote_addr):
                logger.warning("Login bloqueado: reCAPTCHA inválido")
                flash("Confirme o captcha para continuar.", "error")
                return render_template("login.html", **_contexto_login())
        else:
            if not validar_captcha_local(
                request.form.get("captcha_resposta"),
                request.form.get("captcha_token"),
            ):
                logger.warning("Login bloqueado: captcha local inválido")
                flash("Código do captcha incorreto.", "error")
                return render_template("login.html", **_contexto_login())

        logger.info("Tentativa de login (hash=%s…)", hash_login_busca(username)[:12])

        conn = conectar_banco()
        if not conn:
            flash("Erro de conexão com o banco de dados", "error")
            return render_template("login.html", **_contexto_login())

        cursor = conn.cursor()

        try:
            garantir_coluna_login_hash(cursor, conn)
            garantir_colunas_bloqueio_login(cursor, conn)

            login_hash = hash_login_busca(username)
            cursor.execute("""
                SELECT
                    UsuarioID,
                    UsuarioNome,
                    Ativo,
                    SenhaHash,
                    Adm,
                    TentativasLoginFalha,
                    ContaBloqueada
                FROM Usuarios
                WHERE UsuarioNomeHash = ?
            """, (login_hash,))

            user = cursor.fetchone()
            legado_sem_hash = False

            # Compatibilidade: usuários ainda em texto claro / sem hash
            if not user:
                cursor.execute("""
                    SELECT
                        UsuarioID,
                        UsuarioNome,
                        Ativo,
                        SenhaHash,
                        Adm,
                        TentativasLoginFalha,
                        ContaBloqueada
                    FROM Usuarios
                    WHERE UsuarioNome = ?
                """, (username.strip(),))
                user = cursor.fetchone()
                legado_sem_hash = bool(user)

            if user:
                (
                    usuario_id,
                    usuario_nome_banco,
                    ativo,
                    senha_hash,
                    adm,
                    _tentativas,
                    conta_bloqueada,
                ) = user
                usuario_nome = revelar_login_banco(usuario_nome_banco)

                if conta_esta_bloqueada(conta_bloqueada):
                    logger.warning(
                        "Login bloqueado (senha): usuario_id=%s",
                        usuario_id,
                    )
                    flash(
                        "Senha bloqueada. Procure o administrador do sistema.",
                        "error",
                    )
                elif ativo:
                    if senha_hash:
                        if verificar_senha(password, senha_hash):
                            try:
                                resetar_tentativas_login(cursor, conn, usuario_id)
                            except Exception as reset_exc:
                                logger.warning(
                                    "Não foi possível zerar tentativas de login: %s",
                                    reset_exc,
                                )

                            # Migra login legado para cifra + hash no primeiro login OK
                            if legado_sem_hash or not esta_criptografado(str(usuario_nome_banco or "")):
                                nome_cif, nome_hash = preparar_login_para_banco(usuario_nome)
                                try:
                                    cursor.execute(
                                        """
                                        UPDATE Usuarios
                                        SET UsuarioNome = ?, UsuarioNomeHash = ?
                                        WHERE UsuarioID = ?
                                        """,
                                        (nome_cif, nome_hash, usuario_id),
                                    )
                                    conn.commit()
                                except Exception as mig_exc:
                                    logger.warning(
                                        "Não foi possível migrar login cifrado do usuário %s: %s",
                                        usuario_id,
                                        mig_exc,
                                    )

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

                            session["usuario"] = {
                                "usuario_id": usuario_id,
                                "usuario": usuario_nome,
                                "adm": adm,
                            }

                            if len(projetos) == 0:
                                session.pop("usuario", None)
                                flash(
                                    "Usuário autenticado, mas não possui projetos associados. "
                                    "Peça ao administrador para vincular projetos em Gerenciar Usuários.",
                                    "error",
                                )
                            elif len(projetos) == 1:
                                projeto = projetos[0]
                                projeto_completo = buscar_projeto_completo(projeto.ProjetoID)
                                if projeto_completo:
                                    session["projeto_selecionado"] = projeto_para_sessao(projeto_completo)
                                else:
                                    session["projeto_selecionado"] = _resumo_projeto_lista(projeto)

                                logger.info(
                                    "Login OK: usuario_id=%s projeto_id=%s",
                                    usuario_id,
                                    projeto.ProjetoID,
                                )
                                return redirect(url_for("dashboard.dashboard"))
                            else:
                                session["projetos_disponiveis"] = [
                                    _resumo_projeto_lista(proj) for proj in projetos
                                ]
                                return redirect(url_for("auth.selecionar_projeto"))
                        else:
                            resultado = registrar_falha_senha(cursor, conn, usuario_id)
                            logger.warning(
                                "Login falhou (senha inválida): usuario_id=%s tentativas=%s",
                                usuario_id,
                                resultado.get("tentativas"),
                            )
                            if resultado.get("bloqueou_agora"):
                                notificar_admins_bloqueio(
                                    cursor,
                                    usuario_nome,
                                    usuario_id,
                                    resultado.get("tentativas") or MAX_TENTATIVAS_SENHA,
                                )
                                flash(
                                    "Senha bloqueada. Procure o administrador do sistema.",
                                    "error",
                                )
                    else:
                        logger.error("Usuário sem SenhaHash: id=%s", usuario_id)
                else:
                    logger.warning("Login falhou (usuário inativo): id=%s", usuario_id)
            else:
                logger.warning("Login falhou (credenciais inválidas)")

        except Exception as e:
            logger.error(f"Erro no login: {e}")
            flash("Erro interno no sistema", "error")
        finally:
            cursor.close()
            conn.close()

    return render_template("login.html", **_contexto_login())


@auth_bp.route("/selecionar_projeto", methods=["GET", "POST"])
def selecionar_projeto():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    if "projetos_disponiveis" not in session:
        flash("Nenhum projeto disponível", "error")
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        projeto_id = request.form.get("projeto_id")

        projeto_selecionado = next(
            (proj for proj in session["projetos_disponiveis"] if str(proj["ProjetoID"]) == projeto_id),
            None,
        )

        if projeto_selecionado:
            projeto_completo = buscar_projeto_completo(projeto_selecionado["ProjetoID"])
            if projeto_completo:
                session["projeto_selecionado"] = projeto_para_sessao(projeto_completo)
            else:
                session["projeto_selecionado"] = projeto_para_sessao(projeto_selecionado)

            session.pop("projetos_disponiveis", None)
            logger.info(
                "Projeto selecionado: id=%s nome=%s",
                session["projeto_selecionado"].get("ProjetoID"),
                session["projeto_selecionado"].get("NomeProjeto"),
            )
            return redirect(url_for("dashboard.dashboard"))
        else:
            flash("Projeto não encontrado", "error")

    return render_template(
        "selecionar_projeto.html",
        projetos=session["projetos_disponiveis"],
    )


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
            _resumo_projeto_lista(proj) for proj in projetos
        ]

        return render_template(
            "selecionar_projeto.html",
            projetos=session["projetos_disponiveis"],
        )

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


def _url_absoluta(endpoint, **values):
    """Monta URL absoluta para e-mail (usa BASE_URL se definida)."""
    path = url_for(endpoint, **values)
    if Config.BASE_URL:
        return f"{Config.BASE_URL}{path}"
    return url_for(endpoint, _external=True, **values)


_MSG_RECUPERACAO_GENERICA = (
    "Se o usuário existir e tiver e-mail cadastrado, "
    "enviamos um link para redefinir a senha."
)


@auth_bp.route("/recuperar-senha", methods=["GET", "POST"])
def recuperar_senha():
    if request.method == "GET":
        return render_template("recuperar_senha.html")

    username = (request.form.get("username") or "").strip()
    if not username:
        flash(_MSG_RECUPERACAO_GENERICA, "success")
        return render_template("recuperar_senha.html")

    conn = conectar_banco()
    if not conn:
        flash("Erro de conexão com o banco de dados", "error")
        return render_template("recuperar_senha.html")

    cursor = conn.cursor()
    try:
        garantir_coluna_login_hash(cursor, conn)
        garantir_coluna_email(cursor, conn)
        garantir_tabela_reset_senha(cursor, conn)

        login_hash = hash_login_busca(username)
        cursor.execute(
            """
            SELECT UsuarioID, UsuarioNome, Ativo, Email
            FROM Usuarios
            WHERE UsuarioNomeHash = ?
            """,
            (login_hash,),
        )
        user = cursor.fetchone()
        if not user:
            cursor.execute(
                """
                SELECT UsuarioID, UsuarioNome, Ativo, Email
                FROM Usuarios
                WHERE UsuarioNome = ?
                """,
                (username,),
            )
            user = cursor.fetchone()

        if user and user.Ativo and (user.Email or "").strip():
            email_dest = (user.Email or "").strip()
            token = criar_token_reset(cursor, conn, user.UsuarioID)
            link = _url_absoluta("auth.redefinir_senha", token=token)

            smtp_cfg = resolver_config_smtp(obter_smtp_config(None))
            if not smtp_cfg:
                logger.error("Recuperação de senha: SMTP não configurado")
                flash(
                    "Não foi possível enviar o e-mail. Contate o administrador.",
                    "error",
                )
                return render_template("recuperar_senha.html")

            nome_exibicao = revelar_login_banco(user.UsuarioNome)
            assunto = "Redefinição de senha — DE X PARA"
            texto = (
                f"Olá, {nome_exibicao}.\n\n"
                "Recebemos uma solicitação para redefinir sua senha.\n"
                f"Acesse o link abaixo (válido por 2 horas):\n\n{link}\n\n"
                "Se você não solicitou, ignore este e-mail.\n"
            )
            html = (
                f"<p>Olá, <strong>{nome_exibicao}</strong>.</p>"
                "<p>Recebemos uma solicitação para redefinir sua senha.</p>"
                f'<p><a href="{link}">Clique aqui para alterar a senha</a></p>'
                f"<p>Ou copie o link:<br>{link}</p>"
                "<p>O link é válido por <strong>2 horas</strong>.</p>"
                "<p>Se você não solicitou, ignore este e-mail.</p>"
            )
            try:
                enviar_email(smtp_cfg, [email_dest], assunto, texto, html)
                logger.info(
                    "E-mail de recuperação enviado para usuario_id=%s",
                    user.UsuarioID,
                )
            except Exception as exc:
                logger.error("Falha ao enviar e-mail de recuperação: %s", exc)
                flash(
                    "Não foi possível enviar o e-mail. Contate o administrador.",
                    "error",
                )
                return render_template("recuperar_senha.html")
        else:
            logger.info(
                "Recuperação solicitada sem envio (usuário inexistente/inativo/sem e-mail)"
            )

        flash(_MSG_RECUPERACAO_GENERICA, "success")
        return render_template("recuperar_senha.html")

    except Exception as e:
        logger.error(f"Erro na recuperação de senha: {e}")
        flash("Erro interno no sistema", "error")
        return render_template("recuperar_senha.html")
    finally:
        cursor.close()
        conn.close()


@auth_bp.route("/redefinir-senha/<token>", methods=["GET", "POST"])
def redefinir_senha(token):
    conn = conectar_banco()
    if not conn:
        flash("Erro de conexão com o banco de dados", "error")
        return redirect(url_for("auth.login"))

    cursor = conn.cursor()
    try:
        garantir_tabela_reset_senha(cursor, conn)
        valido = buscar_token_valido(cursor, token)

        if not valido:
            flash("Link inválido ou expirado. Solicite uma nova recuperação.", "error")
            return redirect(url_for("auth.recuperar_senha"))

        token_id, usuario_id = valido

        if request.method == "GET":
            return render_template("redefinir_senha.html", token=token)

        senha = request.form.get("senha") or ""
        senha2 = request.form.get("senha_confirmacao") or ""

        if len(senha) < 6:
            flash("A senha deve ter pelo menos 6 caracteres.", "error")
            return render_template("redefinir_senha.html", token=token)
        if senha != senha2:
            flash("As senhas não coincidem.", "error")
            return render_template("redefinir_senha.html", token=token)

        # Revalida token antes de gravar
        valido = buscar_token_valido(cursor, token)
        if not valido:
            flash("Link inválido ou expirado. Solicite uma nova recuperação.", "error")
            return redirect(url_for("auth.recuperar_senha"))

        token_id, usuario_id = valido
        senha_hash = hash_senha(senha)
        garantir_colunas_bloqueio_login(cursor, conn)
        cursor.execute(
            "UPDATE Usuarios SET SenhaHash = ? WHERE UsuarioID = ?",
            (senha_hash, usuario_id),
        )
        desbloquear_conta(cursor, None, usuario_id)
        marcar_token_usado(cursor, conn, token_id)
        conn.commit()

        logger.info("Senha redefinida com sucesso: usuario_id=%s", usuario_id)
        flash("Senha alterada com sucesso. Faça login com a nova senha.", "success")
        return redirect(url_for("auth.login"))

    except Exception as e:
        logger.error(f"Erro ao redefinir senha: {e}")
        if conn:
            conn.rollback()
        flash("Erro ao alterar a senha. Tente novamente.", "error")
        return render_template("redefinir_senha.html", token=token)
    finally:
        cursor.close()
        conn.close()
