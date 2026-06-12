@echo off
echo 🚀 Configurando De x Para no Windows Server 2022...

echo 📦 Instalando dependências do sistema...
winget install Python.Python.3.10

echo 📥 Instalando ODBC Driver for SQL Server...
%SystemRoot%\system32\msiexec.exe /i https://go.microsoft.com/fwlink/?linkid=2222950 /quiet

echo 🔧 Criando ambiente virtual...
python -m venv venv
call venv\Scripts\activate.bat

echo 📚 Instalando dependências Python...
pip install -r requirements.txt

echo ✅ Configuração concluída!
echo.
echo 📝 Próximos passos:
echo 1. Configure o IIS com o web.config
echo 2. Configure as variáveis de ambiente
echo 3. Teste a aplicação
pause