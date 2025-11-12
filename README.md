# 🚀 Sistema de Controladoria — Flask + SQL Server

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Flask](https://img.shields.io/badge/Flask-Framework-black)
![SQL Server](https://img.shields.io/badge/SQL%20Server-Database-red)
![Status](https://img.shields.io/badge/Status-Production-green)
![Platform](https://img.shields.io/badge/Platform-Windows-lightgrey)

Sistema web completo desenvolvido para gestão de cadastros corporativos, padronização de dados (De/Para), controle de acesso e integração com banco de dados SQL Server.  
Construído com arquitetura modular usando Flask, o sistema contém mais de **70 módulos independentes**, cada um responsável pela administração de uma entidade corporativa.

---

## 📌 Funcionalidades Principais

- 🔐 **Login e controle de acesso**
- 🏢 Seleção de empresa e projeto
- 📊 Dashboard com indicadores e gráficos
- 🗂️ Gestão de dezenas de entidades:
  - Empresas, usuários, equipes, departamentos
  - Natureza de operação, plano de contas, subconta
  - Tipos de documento, serviços, produtos, tabelas de preço
  - Estado, município, país, procedência
  - Escolaridade, profissão, estado civil
  - Marca, modelo, veículo ano
  - Condição de pagamento, cobrança, títulos
  - E muito mais…
- 📁 Padronização automática de dados (De/Para)
- 🧪 Validação e processamento automático
- 📥 Importação/Exportação de dados
- 🧾 Logs detalhados da aplicação e autenticação
- 🖥️ Deploy completo no Windows IIS + Cloudflare Tunnel

---

## 🧩 Arquitetura do Projeto

O sistema segue uma arquitetura modular baseada em **Blueprints**, onde cada módulo é completamente independente:

/auth -> segurança e autenticação
/db -> conexões com SQL Server
/models -> modelos de dados
/routes -> rotas de cada módulo (70+)
/templates -> páginas HTML
/static -> CSS, JS e assets
/utils -> validação, processamento, layouts
/logs -> logs de sistema

## 📁 Estrutura do Projeto
/auth -> segurança e autenticação
/db -> conexões com SQL Server
/models -> modelos de dados
/routes -> rotas de cada módulo (70+)
/templates -> páginas HTML
/static -> CSS, JS e assets
/utils -> validação, processamento, layouts
/logs -> logs de sistema


---

## ⚙️ Tecnologias Utilizadas

### **Back-end**
- Python 3.12  
- Flask  
- Blueprints  
- pyodbc  
- SQL Server  

### **Front-end**
- HTML5  
- CSS3  
- JavaScript  
- Jinja2 Templates  

### **Infraestrutura**
- Windows Server / IIS  
- Cloudflare Tunnel  
- PowerShell Automation Scripts  
- Loggers personalizados  

---

## 🖼️ Screenshots (adicione depois)

Você pode adicionar prints assim:

📦 Instalação
Clone o repositório
git clone https://github.com/santana2000br2/Depara.git
cd Depara

Crie o ambiente virtual
python -m venv venv
venv\Scripts\activate

Instale as dependências
pip install -r requirements.txt

Configure o arquivo .env
DB_SERVER=SEU_SERVIDOR
DB_DATABASE=SEU_BANCO
DB_USER=SEU_USUARIO
DB_PASSWORD=SUA_SENHA

▶️ Como Rodar
Modo Desenvolvimento
python app.py

Modo Produção (via IIS)

Configure o módulo WFastCGI

Utilize o arquivo web.config incluído

Execute via run_flask.py se desejar modo standalone

🛠️ Scripts Úteis

deploy_windows.ps1 → Deploy automático no IIS

backup_windows.ps1 → Backup da base

monitor_windows.ps1 → Monitoramento do serviço

corrigir_templates.bat → Correções automáticas

make.bat → Utilidades gerais

📜 Licença

Este projeto é privado e de uso interno.

Aroldo Santana
Desenvolvedor Full Stack
💼 Expertise em Python, Flask, SQL Server, automação e sistemas corporativos.
📧 [santana2000br@gmail.com]

Aroldo Santana
Desenvolvedor Full Stack
💼 Expertise em Python, Flask, SQL Server, automação e sistemas corporativos.


