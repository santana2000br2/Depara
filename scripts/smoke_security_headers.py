"""Smoke test dos headers de segurança."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app

c = app.test_client()
r = c.get("/auth/login")
print("status", r.status_code)
for h in [
    "Content-Security-Policy",
    "X-Content-Type-Options",
    "X-Frame-Options",
    "Referrer-Policy",
    "Cross-Origin-Opener-Policy",
    "Cross-Origin-Resource-Policy",
    "Permissions-Policy",
    "Strict-Transport-Security",
]:
    val = r.headers.get(h, "")
    print(h, (val[:160] + "...") if len(val) > 160 else val)

html = r.get_data(as_text=True)
print("has_nonce", 'nonce="' in html)
print("has_sri_fa", "sha512-iecdLmaskl7CVkqkXNQ" in html)
print("autocomplete_password", 'autocomplete="new-password"' in html)

ro = c.open("/auth/login", method="OPTIONS")
print("options", ro.status_code)

# HTTPS HSTS via forwarded proto
r2 = c.get("/auth/login", headers={"X-Forwarded-Proto": "https"})
print("hsts_https", r2.headers.get("Strict-Transport-Security"))
