import sys
import os
sys.path.insert(0, r'E:\Sites\Depara_Novo')

# Configurar variáveis de ambiente para o proxy
os.environ['WSGI_SCRIPT_ALIAS'] = '/DePara'

from app import app

# Configurar a aplicação para trabalhar com proxy reverso
class ProxyFix:
    def __init__(self, app):
        self.app = app
    
    def __call__(self, environ, start_response):
        # Corrigir SCRIPT_NAME e PATH_INFO para o proxy
        script_name = '/DePara'
        if environ.get('HTTP_X_SCRIPT_NAME', ''):
            script_name = environ['HTTP_X_SCRIPT_NAME']
        
        if 'SCRIPT_NAME' not in environ:
            environ['SCRIPT_NAME'] = script_name
        
        if environ.get('PATH_INFO', '').startswith(script_name):
            environ['PATH_INFO'] = environ['PATH_INFO'][len(script_name):]
        
        # Garantir que PATH_INFO não comece com //
        if environ.get('PATH_INFO', '').startswith('//'):
            environ['PATH_INFO'] = environ['PATH_INFO'][1:]
        
        return self.app(environ, start_response)

application = ProxyFix(app)

if __name__ == '__main__':
    from waitress import serve
    serve(application, host='0.0.0.0', port=8000)
