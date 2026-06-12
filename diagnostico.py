import socket
import sys
sys.path.insert(0, r'E:\Sites\Depara_Novo')

try:
    # Testar se a porta está aberta
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', 8000))
    if result == 0:
        print("✅ Porta 8000 está aberta e aceitando conexões")
    else:
        print("❌ Porta 8000 está fechada ou bloqueada")
    sock.close()
    
    # Testar importação da aplicação
    from app import app
    print("✅ Aplicação Flask importada com sucesso")
    
    print("🚀 Iniciando Waitress...")
    from waitress import serve
    serve(app, host='127.0.0.1', port=8000, threads=4)
    
except Exception as e:
    print(f"❌ ERRO: {e}")
    import traceback
    traceback.print_exc()
    input("Pressione Enter para sair...")
