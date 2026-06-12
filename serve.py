import logging
from app import app
from waitress import serve

logging.basicConfig(filename=r"E:\Sites\Depara_Novo\logs\flask.log",
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

serve(app, host='0.0.0.0', port=8000)
