import webbrowser
import threading
import time
from app import app


def open_browser():
    time.sleep(2)
    webbrowser.open("http://localhost:5000")


if __name__ == "__main__":
    threading.Timer(1, open_browser).start()
    app.run(debug=True, port=5000, use_reloader=False)
