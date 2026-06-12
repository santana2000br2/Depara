import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask
from db.connection import conectar_banco

# Configurar app Flask
app = Flask(__name__)
app.secret_key = 'teste'

# Dados do projeto (mesmo que está funcionando)
projeto_teste = {
    "ProjetoID": 1,
    "NomeProjeto": "Projeto Teste",
    "DadosGX": "DadosGx_Ramasa2_JanFev",
    "servidorhomologacao": "200.143.171.34,1435",
    "usuariohomologacao": "sa",
    "senhahomologacao": "674DYKpNnQfP8hfSm"
}

print("=" * 80)
print("TESTE COMPLETO DO DASHBOARD")
print("=" * 80)

def testar_escopos_projeto():
    """Testa se o projeto tem escopos configurados"""
    print("\n1. TESTANDO ESCOPOS DO PROJETO")
    print("-" * 40)
    
    conn = None
    cursor = None
    try:
        conn = conectar_banco()
        if conn is None:
            print("❌ Não foi possível conectar ao banco de autenticação")
            return []
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TipoEscopoIDs 
            FROM Escopo 
            WHERE ProjetoID = ?
        """, (projeto_teste["ProjetoID"],))
        
        resultado = cursor.fetchone()
        if resultado and resultado.TipoEscopoIDs:
            escopos = resultado.TipoEscopoIDs.split(',')
            print(f"✅ Escopos encontrados: {escopos}")
            return escopos
        else:
            print("⚠️ Nenhum escopo configurado para este projeto")
            return []
            
    except Exception as e:
        print(f"❌ Erro ao buscar escopos: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def testar_funcao_dados_municipio():
    """Testa a função específica de dados do município"""
    print("\n2. TESTANDO FUNÇÃO dados_municipio")
    print("-" * 40)
    
    with app.app_context():
        with app.test_request_context():
            from flask import session
            from utils.dados_depara import dados_municipio
            
            # Configurar sessão
            session["projeto_selecionado"] = projeto_teste
            
            print(f"Banco: {projeto_teste['DadosGX']}")
            
            # Chamar função
            resultado = dados_municipio(projeto_teste['DadosGX'])
            
            print(f"Resultado:")
            print(f"  qtd: {resultado.get('qtd')} (esperado: 596)")
            print(f"  qtdPendente: {resultado.get('qtdPendente')}")
            print(f"  percentualConclusao: {resultado.get('percentualConclusao')}%")
            
            if resultado.get('qtd') == 596:
                print("✅ Função dados_municipio funcionando corretamente!")
                return True
            else:
                print(f"❌ Esperado 596 registros, mas obteve {resultado.get('qtd')}")
                return False

def testar_obter_dados_por_categoria():
    """Testa a função que coleta dados por categoria"""
    print("\n3. TESTANDO obter_dados_por_categoria")
    print("-" * 40)
    
    with app.app_context():
        with app.test_request_context():
            from flask import session
            import importlib
            
            # Importar módulo dashboard dinamicamente
            import routes.dashboard as dashboard_module
            importlib.reload(dashboard_module)  # Recarregar para pegar alterações
            
            # Configurar sessão
            session["projeto_selecionado"] = projeto_teste
            
            # Simular categorias habilitadas (incluindo municipio)
            categorias_habilitadas = ["municipio", "estado", "pais"]
            
            print(f"Categorias habilitadas: {categorias_habilitadas}")
            print(f"Banco: {projeto_teste['DadosGX']}")
            
            # Chamar função (precisamos acessar a função do módulo)
            dados = dashboard_module.obter_dados_por_categoria(
                projeto_teste['DadosGX'], 
                categorias_habilitadas
            )
            
            print(f"\nDados coletados:")
            for categoria, valores in dados.items():
                if valores.get('qtd', 0) > 0:
                    print(f"  {categoria}: {valores.get('qtd')} registros")
            
            if "municipio" in dados and dados["municipio"].get("qtd") == 596:
                print("✅ obter_dados_por_categoria funcionando!")
                return True
            else:
                print("❌ obter_dados_por_categoria não coletou dados corretamente")
                return False

def testar_calculo_progresso():
    """Testa o cálculo de progresso"""
    print("\n4. TESTANDO CÁLCULO DE PROGRESSO")
    print("-" * 40)
    
    with app.app_context():
        with app.test_request_context():
            from flask import session
            import importlib
            
            # Importar módulo dashboard
            import routes.dashboard as dashboard_module
            importlib.reload(dashboard_module)
            
            # Configurar sessão
            session["projeto_selecionado"] = projeto_teste
            
            # Dados simulados
            dados_tabelas = [
                {"qtd": 596, "qtdPendente": 100},  # municipio
                {"qtd": 27, "qtdPendente": 5},     # estado
                {"qtd": 10, "qtdPendente": 2},     # pais
            ]
            
            progresso = dashboard_module.calcular_progresso_total(dados_tabelas)
            
            print(f"Progresso calculado:")
            print(f"  Total registros: {progresso.get('total_qtd')}")
            print(f"  Total concluído: {progresso.get('total_concluido')}")
            print(f"  Total pendente: {progresso.get('total_pendente')}")
            print(f"  Percentual: {progresso.get('percentual_total')}%")
            
            total_esperado = 596 + 27 + 10
            concluido_esperado = (596-100) + (27-5) + (10-2)
            percentual_esperado = (concluido_esperado / total_esperado * 100) if total_esperado > 0 else 0
            
            print(f"\nValores esperados:")
            print(f"  Total: {total_esperado}")
            print(f"  Concluído: {concluido_esperado}")
            print(f"  Percentual: {round(percentual_esperado, 1)}%")
            
            if abs(progresso.get('percentual_total', 0) - round(percentual_esperado, 1)) < 0.1:
                print("✅ Cálculo de progresso correto!")
                return True
            else:
                print("❌ Cálculo de progresso incorreto")
                return False

if __name__ == "__main__":
    print(f"Projeto ID: {projeto_teste['ProjetoID']}")
    print(f"Nome: {projeto_teste['NomeProjeto']}")
    print(f"Banco DadosGX: {projeto_teste['DadosGX']}")
    
    # Executar testes
    resultados = []
    
    resultados.append(("Escopos do projeto", testar_escopos_projeto()))
    resultados.append(("Função dados_municipio", testar_funcao_dados_municipio()))
    resultados.append(("Coleta por categoria", testar_obter_dados_por_categoria()))
    resultados.append(("Cálculo de progresso", testar_calculo_progresso()))
    
    print("\n" + "=" * 80)
    print("RESUMO FINAL")
    print("=" * 80)
    
    for teste, resultado in resultados:
        status = "✅ PASS" if resultado else "❌ FAIL"
        print(f"{status} - {teste}")
    
    print("\n" + "=" * 80)
    if all(r[1] for r in resultados):
        print("🎉 TODOS OS TESTES PASSARAM!")
        print("O problema pode estar nos escopos do projeto ou na interface.")
    else:
        print("⚠️ ALGUNS TESTES FALHARAM")
        print("Verifique os logs acima para identificar o problema.")
    print("=" * 80)