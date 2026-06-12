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

        # Buscar projetos COM OS NOMES CORRETOS DAS COLUNAS
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
                migrador,           -- CORRIGIDO: minúsculo
                bancoProducao       -- CORRIGIDO: P maiúsculo
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
                'migrador': projeto.migrador,           # CORRIGIDO: minúsculo
                'bancoProducao': projeto.bancoProducao  # CORRIGIDO: P maiúsculo
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
    logger.info(f"Dados recebidos para salvar projeto: {data}")
    
    projeto_id = data.get("projeto_id")
    nome_projeto = data.get("nome_projeto")
    dados_gx = data.get("dados_gx")
    
    # CAMPOS DE PRODUÇÃO
    servidorproducao = data.get("servidorproducao")
    usuarioProducao = data.get("usuarioProducao")
    senhaproducao = data.get("senhaproducao")
    
    # CAMPOS DE HOMOLOGAÇÃO
    servidorhomologacao = data.get("servidorhomologacao")
    usuariohomologacao = data.get("usuariohomologacao")
    senhahomologacao = data.get("senhahomologacao")
    
    banco_homo = data.get("banco_homo")
    ponto_focal = data.get("ponto_focal")
    consultor_lider = data.get("consultor_lider")
    lider_projeto = data.get("lider_projeto")
    
    # NOVOS CAMPOS - USANDO OS NOMES CORRETOS
    migrador = data.get("migrador")
    bancoProducao = data.get("bancoProducao")

    logger.info(f"DEBUG - Valores dos novos campos: migrador='{migrador}', bancoProducao='{bancoProducao}'")

    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if not conn:
            logger.error("Falha na conexão com o banco")
            return jsonify({"status": "error", "message": "Erro de conexão com o banco"}), 500

        cursor = conn.cursor()

        if projeto_id and projeto_id != 'null' and projeto_id != '':  # EDITANDO projeto existente
            projeto_id = int(projeto_id)
            logger.info(f"Editando projeto ID: {projeto_id}")
            
            # Verificar se o projeto existe
            cursor.execute("SELECT ProjetoID FROM Projeto WHERE ProjetoID = ?", (projeto_id,))
            if not cursor.fetchone():
                return jsonify({"status": "error", "message": "Projeto não encontrado"}), 404

            # Atualizar projeto COM OS NOMES CORRETOS DAS COLUNAS
            cursor.execute("""
                UPDATE Projeto SET 
                    NomeProjeto = ?, 
                    DadosGX = ?, 
                    servidorproducao = ?, 
                    usuarioProducao = ?, 
                    senhaproducao = ?, 
                    servidorhomologacao = ?,
                    usuariohomologacao = ?,
                    senhahomologacao = ?,
                    BancoHomo = ?, 
                    PontoFocal = ?, 
                    ConsultorLider = ?, 
                    LiderProjeto = ?,
                    migrador = ?,
                    bancoProducao = ?
                WHERE ProjetoID = ?
            """, (
                nome_projeto,
                dados_gx,
                servidorproducao,
                usuarioProducao,
                senhaproducao,
                servidorhomologacao,
                usuariohomologacao,
                senhahomologacao,
                banco_homo,
                ponto_focal,
                consultor_lider,
                lider_projeto,
                migrador,
                bancoProducao,
                projeto_id
            ))
            logger.info(f"Projeto {projeto_id} atualizado - linhas afetadas: {cursor.rowcount}")

        else:  # NOVO projeto
            logger.info("Criando novo projeto")
            
            # Inserir novo projeto e obter o ID diretamente
            cursor.execute("""
                INSERT INTO Projeto (
                    NomeProjeto, DadosGX, servidorproducao, usuarioProducao, 
                    senhaproducao, servidorhomologacao, usuariohomologacao,
                    senhahomologacao, BancoHomo, PontoFocal, ConsultorLider, LiderProjeto,
                    migrador, bancoProducao
                ) OUTPUT INSERTED.ProjetoID 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                nome_projeto,
                dados_gx,
                servidorproducao,
                usuarioProducao,
                senhaproducao,
                servidorhomologacao,
                usuariohomologacao,
                senhahomologacao,
                banco_homo,
                ponto_focal,
                consultor_lider,
                lider_projeto,
                migrador,
                bancoProducao
            ))
            
            # Obter o ID do novo projeto diretamente do resultado da inserção
            result = cursor.fetchone()
            novo_projeto_id = result[0] if result else None

            logger.info(f"Novo projeto ID: {novo_projeto_id}")

            if not novo_projeto_id:
                conn.rollback()
                return jsonify({"status": "error", "message": "Falha ao obter ID do novo projeto"}), 500

        conn.commit()
        logger.info(f"Projeto {'atualizado' if projeto_id else 'criado'} com sucesso: {nome_projeto}")
        return jsonify({"status": "success", "message": "Projeto salvo com sucesso!"}), 200

    except Exception as e:
        logger.error(f"Erro ao salvar projeto: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"status": "error", "message": f"Erro ao salvar projeto: {str(e)}"}), 500
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
        
        # Consulta COM OS NOMES CORRETOS DAS COLUNAS
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
                migrador,           -- CORRIGIDO: minúsculo
                bancoProducao       -- CORRIGIDO: P maiúsculo
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
            'migrador': projeto.migrador,           # CORRIGIDO: minúsculo
            'bancoProducao': projeto.bancoProducao  # CORRIGIDO: P maiúsculo
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