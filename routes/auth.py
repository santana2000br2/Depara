from flask import (
    Blueprint, render_template, redirect, url_for, 
    session, request, flash, jsonify
)
from db.connection import conectar_banco
from logger import logger
from auth.security import verificar_senha

auth_bp = Blueprint("auth", __name__)

def buscar_projeto_completo(projeto_id):
    """Busca todos os campos de um projeto pelo ID"""
    conn = conectar_banco()
    if not conn:
        logger.error("❌ Não foi possível conectar ao banco para buscar projeto completo")
        return None
    
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                ProjetoID,
                NomeProjeto,
                DadosGX,
                servidorproducao,
                usuarioProducao,
                senhaproducao,
                servidorhomologacao,
                usuariohomologacao,
                senhahomologacao,
                BancoHomo,
                PontoFocal,
                ConsultorLider,
                LiderProjeto,
                migrador,
                bancoProducao
            FROM Projeto 
            WHERE ProjetoID = ?
        """, (projeto_id,))
        
        projeto = cursor.fetchone()
        if projeto:
            return {
                'ProjetoID': projeto.ProjetoID,
                'NomeProjeto': projeto.NomeProjeto,
                'DadosGX': projeto.DadosGX,
                'servidorproducao': projeto.servidorproducao,
                'usuarioProducao': projeto.usuarioProducao,
                'senhaproducao': projeto.senhaproducao,
                'servidorhomologacao': projeto.servidorhomologacao,
                'usuariohomologacao': projeto.usuariohomologacao,
                'senhahomologacao': projeto.senhahomologacao,
                'BancoHomo': projeto.BancoHomo,
                'PontoFocal': projeto.PontoFocal,
                'ConsultorLider': projeto.ConsultorLider,
                'LiderProjeto': projeto.LiderProjeto,
                'migrador': projeto.migrador,
                'bancoProducao': projeto.bancoProducao
            }
        return None
    except Exception as e:
        logger.error(f"❌ Erro ao buscar projeto completo: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        print(f"DEBUG: Tentativa de login - Usuário: {username}")
        print(f"DEBUG: Senha digitada: '{password}'")

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
                print(f"DEBUG: Tipo do hash: {type(senha_hash)}")
                
                if ativo:
                    # Verificar senha com bcrypt
                    print(f"DEBUG: Verificando senha com bcrypt...")
                    
                    if senha_hash:
                        resultado_verificacao = verificar_senha(password, senha_hash)
                        print(f"DEBUG: Resultado da verificação: {resultado_verificacao}")
                        
                        if resultado_verificacao:
                            print("DEBUG: Senha CORRETA!")
                            
                            # Buscar projetos do usuário
                            cursor.execute("""
                                SELECT 
                                    p.ProjetoID,
                                    p.NomeProjeto,
                                    p.DadosGX,
                                    p.servidorhomologacao,
                                    p.usuariohomologacao,
                                    p.senhahomologacao
                                FROM UsuarioProjeto up
                                INNER JOIN Projeto p ON up.ProjetoID = p.ProjetoID
                                WHERE up.UsuarioID = ?
                            """, (usuario_id,))
                            
                            projetos = cursor.fetchall()
                            print(f"DEBUG: Número de projetos encontrados: {len(projetos)}")
                            
                            for proj in projetos:
                                print(f"DEBUG: Projeto - {proj.NomeProjeto}, Banco - {proj.DadosGX}")
                                print(f"DEBUG: Servidor Homo - {proj.servidorhomologacao}")
                                print(f"DEBUG: Usuário Homo - {proj.usuariohomologacao}")
                                print(f"DEBUG: Senha Homo - {'*' * len(proj.senhahomologacao) if proj.senhahomologacao else 'vazia'}")
                            
                            session["usuario"] = {
                                "usuario_id": usuario_id,
                                "usuario": usuario_nome,
                                "adm": adm
                            }
                            
                            if len(projetos) == 0:
                                print("DEBUG: NENHUM PROJETO ENCONTRADO")
                                session.pop("usuario", None)
                                flash(
                                    "Usuário autenticado, mas não possui projetos associados. "
                                    "Peça ao administrador para vincular projetos em Gerenciar Usuários.",
                                    "error",
                                )
                            elif len(projetos) == 1:
                                projeto = projetos[0]
                                # Buscar projeto COMPLETO
                                projeto_completo = buscar_projeto_completo(projeto.ProjetoID)
                                if projeto_completo:
                                    session["projeto_selecionado"] = projeto_completo
                                    print("DEBUG: ✅ Projeto completo salvo na sessão")
                                    print(f"DEBUG: Servidor Homologação: {projeto_completo.get('servidorhomologacao')}")
                                    print(f"DEBUG: Usuário Homologação: {projeto_completo.get('usuariohomologacao')}")
                                    print(f"DEBUG: Senha Homologação: {'*' * len(projeto_completo.get('senhahomologacao', '')) if projeto_completo.get('senhahomologacao') else 'vazia'}")
                                else:
                                    # Fallback: salvar pelo menos os campos básicos
                                    session["projeto_selecionado"] = {
                                        "ProjetoID": projeto.ProjetoID,
                                        "NomeProjeto": projeto.NomeProjeto,
                                        "DadosGX": projeto.DadosGX,
                                        "servidorhomologacao": projeto.servidorhomologacao,
                                        "usuariohomologacao": projeto.usuariohomologacao,
                                        "senhahomologacao": projeto.senhahomologacao
                                    }
                                    print("DEBUG: ⚠️ Projeto salvo com campos básicos (fallback)")
                                
                                print("DEBUG: Um projeto encontrado - Redirecionando para dashboard")
                                return redirect(url_for("dashboard.dashboard"))
                            else:
                                # Para múltiplos projetos, salvar informações básicas
                                session["projetos_disponiveis"] = [
                                    {
                                        "ProjetoID": proj.ProjetoID,
                                        "NomeProjeto": proj.NomeProjeto,
                                        "DadosGX": proj.DadosGX,
                                        "servidorhomologacao": proj.servidorhomologacao,
                                        "usuariohomologacao": proj.usuariohomologacao,
                                        "senhahomologacao": proj.senhahomologacao
                                    }
                                    for proj in projetos
                                ]
                                print("DEBUG: Múltiplos projetos - Redirecionando para seleção")
                                return redirect(url_for("auth.selecionar_projeto"))
                        else:
                            print("DEBUG: Senha INCORRETA!")
                            flash("Usuário ou senha incorretos", "error")
                    else:
                        print("DEBUG: Hash de senha não encontrado no BD")
                        flash("Erro de configuração do usuário", "error")
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
            # Buscar projeto COMPLETO do banco
            projeto_completo = buscar_projeto_completo(projeto_selecionado["ProjetoID"])
            if projeto_completo:
                session["projeto_selecionado"] = projeto_completo
                print("✅ Projeto completo carregado na sessão (seleção)")
            else:
                # Fallback: usar os dados que já temos
                session["projeto_selecionado"] = projeto_selecionado
                print("⚠️ Projeto carregado com dados básicos (fallback)")
            
            session.pop("projetos_disponiveis", None)
            
            # Log de diagnóstico
            projeto = session["projeto_selecionado"]
            print(f"🔍 Projeto selecionado: {projeto.get('NomeProjeto')}")
            print(f"🔍 Servidor Homologação: {projeto.get('servidorhomologacao')}")
            print(f"🔍 Usuário Homologação: {projeto.get('usuariohomologacao')}")
            print(f"🔍 Senha Homologação: {'*' * len(projeto.get('senhahomologacao', '')) if projeto.get('senhahomologacao') else 'vazia'}")
            
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
                p.DadosGX,
                p.servidorhomologacao,
                p.usuariohomologacao,
                p.senhahomologacao
            FROM UsuarioProjeto up
            INNER JOIN Projeto p ON up.ProjetoID = p.ProjetoID
            WHERE up.UsuarioID = ?
        """, (usuario_id,))
        
        projetos = cursor.fetchall()
        
        session["projetos_disponiveis"] = [
            {
                "ProjetoID": proj.ProjetoID,
                "NomeProjeto": proj.NomeProjeto,
                "DadosGX": proj.DadosGX,
                "servidorhomologacao": proj.servidorhomologacao,
                "usuariohomologacao": proj.usuariohomologacao,
                "senhahomologacao": proj.senhahomologacao
            }
            for proj in projetos
        ]
        
        print(f"🔍 {len(projetos)} projetos disponíveis para troca")
        
        return render_template("selecionar_projeto.html", 
                             projetos=session["projetos_disponiveis"])
        
    except Exception as e:
        logger.error(f"Erro ao buscar projetos: {e}")
        flash("Erro ao carregar projetos", "error")
        return redirect(url_for("dashboard.dashboard"))
    finally:
        cursor.close()
        conn.close()

@auth_bp.route("/debug_sessao")
def debug_sessao():
    """Rota de debug para verificar a sessão"""
    if "usuario" not in session:
        return jsonify({"error": "Não logado"}), 401
    
    projeto = session.get("projeto_selecionado", {})
    
    # Criar versão segura (esconder senhas)
    projeto_safe = {}
    for key, value in projeto.items():
        if 'senha' in key.lower():
            projeto_safe[key] = '***' if value else 'vazio'
        else:
            projeto_safe[key] = value
    
    # Verificar campos críticos
    campos_criticos = {
        'servidorhomologacao': projeto.get('servidorhomologacao'),
        'usuariohomologacao': projeto.get('usuariohomologacao'),
        'senhahomologacao': projeto.get('senhahomologacao')
    }
    
    status = {
        'tem_credenciais_homologacao': all(campos_criticos.values()),
        'campos_faltando': [k for k, v in campos_criticos.items() if not v],
        'projeto': projeto_safe
    }
    
    return jsonify(status)

@auth_bp.route("/logout")
def logout():
    session.clear()
    flash("Logout realizado com sucesso", "success")
    return redirect(url_for("auth.login"))