from test import application

if __name__ == "__main__":
    from wsgiref.simple_server import make_server
    
    # Cria o servidor na porta 8000
    with make_server('', 8000, application) as server:
        print("Servidor WSGI rodando na porta 8000...")
        print("Acesse: http://localhost:8000")
        print("Pressione Ctrl+C para parar")
        server.serve_forever()