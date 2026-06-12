import os
import re


def corrigir_templates_final():
    templates_path = "templates"

    substituicoes = [
        (r"url_for\('auth\.logout'\)", "url_for('dashboard.logout')"),
        (r"url_for\('logout'\)", "url_for('dashboard.logout')"),
        (
            r"href=\"{{ url_for\('auth\.logout'\) }}\"",
            "href=\"{{ url_for('dashboard.logout') }}\"",
        ),
    ]

    for file_name in os.listdir(templates_path):
        if file_name.endswith(".html"):
            file_path = os.path.join(templates_path, file_name)

            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            for old, new in substituicoes:
                content = re.sub(old, new, content)

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Corrigido: {file_name}")


if __name__ == "__main__":
    corrigir_templates_final()
    print("Correção concluída!")
