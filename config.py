import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Configurações do banco de dados principal (autenticação)
    DB1_SERVER = os.getenv("DB1_SERVER", "localhost")
    DB1_NAME = os.getenv("DB1_NAME", "depara")
    DB1_USER = os.getenv("DB1_USER", "sa")
    DB1_PASSWORD = os.getenv("DB1_PASSWORD", "sua_senha")
    DB1_TIMEOUT = int(os.getenv("DB1_TIMEOUT", "30"))
    
    # Configurações do banco de dados do cliente
    DB2_SERVER = os.getenv("DB2_SERVER", "localhost")
    DB2_USER = os.getenv("DB2_USER", "sa")
    DB2_PASSWORD = os.getenv("DB2_PASSWORD", "sua_senha")
    DB2_TIMEOUT = int(os.getenv("DB2_TIMEOUT", "30"))
    
    # Outras configurações
    SECRET_KEY = os.getenv("SECRET_KEY", "chave-secreta-padrao")
    # Chave Fernet (url-safe base64) para cifrar senhas de projeto/SMTP no banco.
    # Gere com: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    CREDENTIALS_KEY = os.getenv("CREDENTIALS_KEY", "")
    UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "uploads")
    ARQUIVOS_PROJETOS_ROOT = os.getenv(
        "ARQUIVOS_PROJETOS_ROOT",
        r"E:\Arquivos dos Projetos",
    )
    # Mínimo 1 GB — Forn_cli e demais layouts grandes; o teto do IIS precisa coincidir
    MAX_CONTENT_LENGTH = max(
        int(os.getenv("MAX_CONTENT_LENGTH", "1073741824")),
        1073741824,
    )

    # Google reCAPTCHA v2 (login). Sem as duas chaves, o captcha fica desligado.
    RECAPTCHA_SITE_KEY = os.getenv("RECAPTCHA_SITE_KEY", "")
    RECAPTCHA_SECRET_KEY = os.getenv("RECAPTCHA_SECRET_KEY", "")
    
    # Configurações de Log
    LOG_FILE = os.getenv("LOG_FILE", "logs/app.log")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # SMTP padrão (fallback quando o projeto não tiver configuração própria)
    SMTP_HOST = os.getenv("SMTP_HOST", "")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
    SMTP_FROM = os.getenv("SMTP_FROM", "")
    SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() in ("1", "true", "yes", "sim")

    # URL pública do sistema (links de e-mail). Ex.: https://servidor/Depara_Novo
    # Se vazio, usa a URL da requisição atual.
    BASE_URL = (os.getenv("BASE_URL") or "").rstrip("/")

    SECRET_KEY = os.getenv("SECRET_KEY", "chave-secreta-fallback")

    # Cookies de sessão (OWASP): Secure + HttpOnly + SameSite=Lax (first-party).
    # SameSite=None só é necessário em cenários cross-site reais e exige Secure.
    SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "true").lower() in (
        "1", "true", "yes", "on", "sim",
    )
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
    SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "depara_session")
    PREFERRED_URL_SCHEME = os.getenv("PREFERRED_URL_SCHEME", "https")
