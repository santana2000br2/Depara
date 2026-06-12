# run_waitress.py
from app import app

if __name__ == "__main__":
    from waitress import serve
    # bind apenas ao localhost por segurança
    serve(app, host="127.0.0.1", port=8000)
