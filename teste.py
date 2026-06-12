@condicao_pagamento_bp.route('/diagnostico_completo')
def diagnostico_completo():
    """Diagnóstico completo do problema"""
    try:
        if 'projeto_selecionado' not in session:
            return "Nenhum projeto selecionado"
        
        projeto_selecionado = session['projeto_selecionado']
        banco_usuario = projeto_selecionado.get('DadosGX')
        
        if not banco_usuario:
            return "Banco não configurado"
        
        resultado = f"<h3>Diagnóstico - Banco: {banco_usuario}</h3>"
        
        # Teste 1: Conexão básica
        conexao = conectar_segunda_base(banco_usuario)
        if not conexao:
            return resultado + "Falha na conexão"
        
        cursor = conexao.cursor()
        
        # Teste 2: Contagem inicial
        cursor.execute("SELECT COUNT(*) FROM CondicaoPagamento_DePara")
        count_inicial = cursor.fetchone()[0]
        resultado += f"<p>Registros iniciais: {count_inicial}</p>"
        
        # Teste 3: Verificar se podemos INSERT
        try:
            cursor.execute("""
                INSERT INTO CondicaoPagamento_DePara 
                (cpg_cd_cg, cpg_ds, CondicaoPagamento_Codigo, CondicaoPagamento_Descricao) 
                VALUES (?, ?, ?, ?)
            """, 'DIAG_TEST', 'DIAG DESCRIPTION', 'DIAG_CODE', 'DIAG_DESC')
            
            resultado += "<p>INSERT executado com sucesso</p>"
        except Exception as e:
            resultado += f"<p>Erro no INSERT: {str(e)}</p>"
            cursor.close()
            conexao.close()
            return resultado
        
        # Teste 4: Verificar se o INSERT foi persistido SEM commit
        cursor.execute("SELECT COUNT(*) FROM CondicaoPagamento_DePara")
        count_sem_commit = cursor.fetchone()[0]
        resultado += f"<p>Registros sem commit: {count_sem_commit}</p>"
        
        # Teste 5: Fazer commit e verificar novamente
        conexao.commit()
        cursor.execute("SELECT COUNT(*) FROM CondicaoPagamento_DePara")
        count_com_commit = cursor.fetchone()[0]
        resultado += f"<p>Registros com commit: {count_com_commit}</p>"
        
        # Teste 6: Limpar o registro de teste
        cursor.execute("DELETE FROM CondicaoPagamento_DePara WHERE cpg_cd_cg = 'DIAG_TEST'")
        conexao.commit()
        
        cursor.close()
        conexao.close()
        
        resultado += f"<p>Diferença: {count_com_commit - count_inicial}</p>"
        resultado += f"<p>Commit funcionou: {count_com_commit > count_inicial}</p>"
        
        return resultado
        
    except Exception as e:
        return f"Erro no diagnóstico: {str(e)}"