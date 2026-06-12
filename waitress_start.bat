@echo off
cd /d E:\Sites\Depara_Novo
"C:\Program Files\Python313\python.exe" -c "from test import application; from waitress import serve; serve(application, host='0.0.0.0', port=8000)"
