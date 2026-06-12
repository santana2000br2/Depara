import os
import shutil

# Pasta raiz do projeto
root = r"C:\depara_novo"

# Extensões de arquivos para apagar
extensoes = [".pyc", ".pyo", ".log"]

# Pastas para apagar
pastas = ["__pycache__", "venv"]

for foldername, subfolders, filenames in os.walk(root):
    # Apagar arquivos por extensão
    for f in filenames:
        if any(f.endswith(ext) for ext in extensoes):
            caminho = os.path.join(foldername, f)
            print(f"Apagando arquivo: {caminho}")
            os.remove(caminho)

    # Apagar pastas
    for sub in subfolders:
        if sub in pastas:
            caminho = os.path.join(foldername, sub)
            print(f"Apagando pasta: {caminho}")
            shutil.rmtree(caminho)
