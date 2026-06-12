@echo off
chcp 65001 > nul
echo Corrigindo endpoints nos templates...

set "templates_path=templates"

for /r "%templates_path%" %%f in (*.html) do (
    echo Processando: %%~nxf
    
    powershell -Command "(
        gc '%%f' -Raw -Encoding UTF8
    ) -replace 'url_for\(\''logout\''\)', 'url_for(''auth.logout'')' |
     -replace 'url_for\(\''gerenciar_usuarios\''\)', 'url_for(''usuarios.gerenciar_usuarios'')' |
     -replace 'url_for\(\''dashboard\''\)', 'url_for(''dashboard.dashboard'')' |
     -replace 'url_for\(\''login\''\)', 'url_for(''auth.login'')' |
     sc '%%f' -Encoding UTF8"
    
    echo OK: %%~nxf
)

echo Correcao concluida!
pause