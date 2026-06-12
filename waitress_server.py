
from test import application
from waitress import serve

if __name__ == "__main__":
    print('Waitress iniciando em http://0.0.0.0:8000')
    print('Diretório atual: ' + __file__)
    serve(application, host='0.0.0.0', port=8000)
  #  Out-File -FilePath "waitress_server.py" -Encoding utf8 