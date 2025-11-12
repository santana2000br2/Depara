// dashboard.js - Versão Corrigida com data na sidebar

// Mapeamento de tabelas para rotas
const tableRoutes = {
    'Condição de Pagamento': '/condicao_pagamento',
    'Escolaridade': '/escolaridade',
    'Estado': '/estado',
    'Estado Civil': '/estadocivil',
    'Município': '/municipio',
    'País': '/pais',
    'Profissão': '/profissao',
    'Segmento Mercado': '/segmentomercado',
    'Tipo Logradouro': '/tipologradouro',
    'Departamento': '/departamento',
    'Estoque': '/estoque',
    'Natureza Operação': '/naturezaoperacao',
    'Equipe': '/equipe',
    'Usuario DePara': '/usuario_depara',
    'Clas Montadora': '/clasmontadora',
    'Grupo Lucratividade': '/grupolucratividade',
    'Grupo Produto': '/grupoproduto',
    'Pessoa Cod Fabricante': '/pessoacodfabricante',
    'Procedencia': '/procedencia',
    'Tabela Preco': '/tabelapreco',
    'Tipo Produto': '/tipoproduto',
    'Unidade': '/unidade',
    'Combustivel': '/combustivel',
    'Cor Externa': '/corexterna',
    'Cor Interna': '/corinterna',
    'Marca': '/marca',
    'Modelo Veiculo': '/modeloveiculo',
    'Opcional': '/opcional',
    'Setor Servico': '/setorservico',
    'Tipo OS': '/tipoos',
    'Tipo Servico': '/tiposervico',
    'TMO': '/tmo',
    'Veiculo Ano': '/veiculoano',
    'Agente Cobrador': '/agentecobrador',
    'Banco': '/banco',
    'Conta Gerencial': '/contagerencial',
    'Tipo Cobranca': '/tipocobranca',
    'Tipo Credito Debito': '/tipocreditodebito',
    'Tipo Documento': '/tipodocumento',
    'Tipo Ficha Razao': '/tipoficharazao',
    'Tipo Titulo': '/tipotitulo',
    'Centro Resultado': '/centroresultado',
    'Historico Padrao': '/historicopadrao',
    'Plano Conta': '/planoconta',
    'Sub Conta': '/subconta',
    'Tipo Lote': '/tipolote',
    'Tipo Sub Conta': '/tiposubconta'
};

// Função para criar barra de progresso COM TEXTO PRETO
function createProgressBar(percentual, qtd) {
    if (percentual === undefined || percentual === null) {
        percentual = 0;
    }

    const percentualNum = typeof percentual === 'number' ? percentual : parseFloat(percentual);

    // Se QTD for 0, usa cor neutra (cinza)
    let progressColor;
    if (qtd === 0) {
        progressColor = '#6b7280'; // Cinza neutro
    } else {
        progressColor = percentualNum === 100 ? '#10b981' :
            percentualNum >= 70 ? '#3b82f6' :
                percentualNum >= 40 ? '#f59e0b' : '#ef4444';
    }

    return `
        <div class="progress-container" style="display: flex; align-items: center; gap: 10px;">
            <div class="progress-bar" style="flex: 1; height: 20px; background: #e5e7eb; border-radius: 10px; overflow: hidden;">
                <div class="progress-fill" style="height: 100%; border-radius: 10px; background: ${progressColor}; width: ${percentualNum}%; display: flex; align-items: center; justify-content: center;">
                    <span class="progress-text" style="color: #000000; font-size: 11px; font-weight: bold;">
                        ${percentualNum}%
                    </span>
                </div>
            </div>
        </div>
    `;
}

// Função para criar links nas tabelas
function createTableLinks() {
    console.log("=== CRIANDO LINKS NAS TABELAS ===");
    let banco_usuario = 'DB_PADRAO'; // valor padrão

    if (window.empresa_selecionada && window.empresa_selecionada.DadosGX) {
        banco_usuario = window.empresa_selecionada.DadosGX;
    } else if (window.progresso_total && window.progresso_total.banco) {
        banco_usuario = window.progresso_total.banco;
    }

    console.log("Banco selecionado para links:", banco_usuario);
    document.querySelectorAll('.status-table tbody tr').forEach(row => {
        const firstCell = row.querySelector('td:first-child');
        if (firstCell && !firstCell.querySelector('a')) {
            const tableName = firstCell.textContent.trim();
            const route = tableRoutes[tableName];

            console.log(`Tabela: ${tableName}, Rota: ${route}`);

            if (route) {
                const urlWithParams = `${route}?banco=${encodeURIComponent(banco_usuario)}`;
                firstCell.innerHTML = `<a href="${urlWithParams}" class="table-link">${tableName}</a>`;
            }
        }
    });
}

// Função genérica para preencher tabelas
function fillTable(tbodyId, dataArray, categoryName) {
    const tbody = document.getElementById(tbodyId);
    if (!tbody) {
        console.error(`Elemento #${tbodyId} não encontrado`);
        return;
    }

    tbody.innerHTML = '';

    if (!dataArray || dataArray.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #6b7280;">Nenhum dado disponível</td></tr>';
        return;
    }

    dataArray.forEach(item => {
        const row = document.createElement('tr');

        // Célula do nome da tabela
        const nameCell = document.createElement('td');
        nameCell.textContent = item.tabela || 'N/A';
        nameCell.style.fontWeight = '500';
        nameCell.style.padding = '12px 8px';

        // Célula de status
        const statusCell = document.createElement('td');
        const percentual = item.percentualConclusao || 0;
        const qtd = item.qtd || 0;

        let statusText = 'Pendente';
        let statusClass = 'status-pendente';

        // Se QTD for 0, mostra "Não se aplica" com cor neutra
        if (qtd === 0) {
            statusText = 'Não se aplica';
            statusClass = 'status-inaplicavel';
        } else if (percentual === 100) {
            statusText = 'Concluído';
            statusClass = 'status-concluido';
        } else if (percentual >= 70) {
            statusText = 'Em Andamento';
            statusClass = 'status-andamento';
        } else if (percentual >= 40) {
            statusText = 'Parcial';
            statusClass = 'status-parcial';
        }

        statusCell.textContent = statusText;
        statusCell.className = statusClass;
        statusCell.style.padding = '12px 8px';
        statusCell.style.fontWeight = '600';

        // Célula de quantidade total
        const qtdCell = document.createElement('td');
        qtdCell.textContent = qtd;
        qtdCell.style.textAlign = 'center';
        qtdCell.style.padding = '12px 8px';

        // Célula de quantidade pendente
        const qtdPendenteCell = document.createElement('td');
        qtdPendenteCell.textContent = item.qtdPendente || 0;
        qtdPendenteCell.style.textAlign = 'center';
        qtdPendenteCell.style.padding = '12px 8px';

        // Célula de percentual com barra de progresso
        const percentualCell = document.createElement('td');
        percentualCell.innerHTML = createProgressBar(percentual, qtd);
        percentualCell.style.padding = '12px 8px';

        row.appendChild(nameCell);
        row.appendChild(statusCell);
        row.appendChild(qtdCell);
        row.appendChild(qtdPendenteCell);
        row.appendChild(percentualCell);

        tbody.appendChild(row);
    });

    console.log(`Tabela ${tbodyId} preenchida com`, dataArray.length, "registros");
}

// Preencher tabela PESSOA
function fillPessoaTable() {
    console.log("Preenchendo tabela PESSOA...");
    const data = [
        { tabela: 'Condição de Pagamento', ...(window.cond_pag || {}) },
        { tabela: 'Escolaridade', ...(window.escol || {}) },
        { tabela: 'Estado', ...(window.estado || {}) },
        { tabela: 'Estado Civil', ...(window.estadocivil || {}) },
        { tabela: 'Município', ...(window.municipio || {}) },
        { tabela: 'País', ...(window.pais || {}) },
        { tabela: 'Profissão', ...(window.profissao || {}) },
        { tabela: 'Segmento Mercado', ...(window.segmentomercado || {}) },
        { tabela: 'Tipo Logradouro', ...(window.tipologradouro || {}) }
    ];
    fillTable('tbody-pessoa', data, 'PESSOA');
}

// Preencher tabela GERAL
function fillGeralTable() {
    console.log("Preenchendo tabela GERAL...");
    const data = [
        { tabela: 'Departamento', ...(window.departamento || {}) },
        { tabela: 'Estoque', ...(window.estoque || {}) },
        { tabela: 'Natureza Operação', ...(window.naturezaoperacao || {}) },
        { tabela: 'Equipe', ...(window.equipe || {}) }
    ];
    fillTable('tbody-geral', data, 'GERAL');
}

// Preencher tabela USUÁRIOS
function fillUsuariosTable() {
    console.log("Preenchendo tabela USUÁRIOS...");
    const data = [
        { tabela: 'Usuario DePara', ...(window.usuario_depara || {}) }
    ];
    fillTable('tbody-usuarios', data, 'USUÁRIOS');
}

// Preencher tabela PRODUTO
function fillProdutoTable() {
    console.log("Preenchendo tabela PRODUTO...");
    const data = [
        { tabela: 'Clas Montadora', ...(window.clasmontadora || {}) },
        { tabela: 'Grupo Lucratividade', ...(window.grupolucratividade || {}) },
        { tabela: 'Grupo Produto', ...(window.grupoproduto || {}) },
        { tabela: 'Pessoa Cod Fabricante', ...(window.pessoacodfabricante || {}) },
        { tabela: 'Procedencia', ...(window.procedencia || {}) },
        { tabela: 'Tabela Preco', ...(window.tabelapreco || {}) },
        { tabela: 'Tipo Produto', ...(window.tipoproduto || {}) },
        { tabela: 'Unidade', ...(window.unidade || {}) }
    ];
    fillTable('tbody-produto', data, 'PRODUTO');
}

// Preencher tabela VEÍCULOS
function fillVeiculosTable() {
    console.log("Preenchendo tabela VEÍCULOS...");
    const data = [
        { tabela: 'Combustivel', ...(window.combustivel || {}) },
        { tabela: 'Cor Externa', ...(window.corexterna || {}) },
        { tabela: 'Cor Interna', ...(window.corinterna || {}) },
        { tabela: 'Marca', ...(window.marca || {}) },
        { tabela: 'Modelo Veiculo', ...(window.modeloveiculo || {}) },
        { tabela: 'Opcional', ...(window.opcional || {}) },
        { tabela: 'Setor Servico', ...(window.setorservico || {}) },
        { tabela: 'Tipo OS', ...(window.tipoos || {}) },
        { tabela: 'Tipo Servico', ...(window.tiposervico || {}) },
        { tabela: 'TMO', ...(window.tmo || {}) },
        { tabela: 'Veiculo Ano', ...(window.veiculoano || {}) }
    ];
    fillTable('tbody-veiculos', data, 'VEÍCULOS');
}

// Preencher tabela FINANCEIRO
function fillFinanceiroTable() {
    console.log("Preenchendo tabela FINANCEIRO...");
    const data = [
        { tabela: 'Agente Cobrador', ...(window.agentecobrador || {}) },
        { tabela: 'Banco', ...(window.banco || {}) },
        { tabela: 'Conta Gerencial', ...(window.contagerencial || {}) },
        { tabela: 'Tipo Cobranca', ...(window.tipocobranca || {}) },
        { tabela: 'Tipo Credito Debito', ...(window.tipocreditodebito || {}) },
        { tabela: 'Tipo Documento', ...(window.tipodocumento || {}) },
        { tabela: 'Tipo Ficha Razao', ...(window.tipoficharazao || {}) },
        { tabela: 'Tipo Titulo', ...(window.tipotitulo || {}) }
    ];
    fillTable('tbody-financeiro', data, 'FINANCEIRO');
}

// Preencher tabela CONTABILIDADE
function fillContabilidadeTable() {
    console.log("Preenchendo tabela CONTABILIDADE...");
    const data = [
        { tabela: 'Centro Resultado', ...(window.centroresultado || {}) },
        { tabela: 'Historico Padrao', ...(window.historicopadrao || {}) },
        { tabela: 'Plano Conta', ...(window.planoconta || {}) },
        { tabela: 'Sub Conta', ...(window.subconta || {}) },
        { tabela: 'Tipo Lote', ...(window.tipolote || {}) },
        { tabela: 'Tipo Sub Conta', ...(window.tiposubconta || {}) }
    ];
    fillTable('tbody-contabilidade', data, 'CONTABILIDADE');
}

// Função para atualizar datas - AGORA APENAS NA SIDEBAR
function updateDates() {
    const now = new Date();
    const dateStr = now.toLocaleString('pt-BR');

    // Atualizar apenas a data na sidebar
    const sidebarDateElement = document.getElementById('sidebar-update-date');
    if (sidebarDateElement) {
        sidebarDateElement.textContent = dateStr;
    }
}

// Função para verificar se um quadro deve ser mostrado
function shouldShowCategory(categoryName) {
    if (!window.escopos_habilitados || window.escopos_habilitados.length === 0) {
        return true; // Mostrar tudo se não houver escopos definidos
    }

    const categoryToScope = {
        'Pessoa': 'PESSOA',
        'Geral': 'GERAL',
        'Usuários': 'GERAL',
        'Produto': 'PRODUTOS',
        'Veículos': 'VEICULOS',
        'Financeiro': 'FINANCEIRO',
        'Contabilidade': 'CONTABILIDADE'
    };

    const scope = categoryToScope[categoryName];
    return scope ? window.escopos_habilitados.includes(scope) : false;
}

// Função para scroll suave até o topo
function scrollToTop() {
    window.scrollTo({
        top: 0,
        behavior: 'smooth'
    });
}

// Função principal para inicializar o dashboard
function initializeDashboard() {
    console.log("=== INICIALIZANDO DASHBOARD ===");
    console.log("Escopos habilitados:", window.escopos_habilitados);

    // Verificar se os dados básicos estão disponíveis
    if (!window.cond_pag) {
        console.warn("Dados não carregados completamente. Aguardando...");
        setTimeout(initializeDashboard, 100);
        return;
    }

    console.log("Dados carregados, preenchendo tabelas...");

    // Preencher apenas as tabelas dos quadros habilitados
    if (shouldShowCategory('Pessoa')) {
        fillPessoaTable();
    }
    if (shouldShowCategory('Geral')) {
        fillGeralTable();
    }
    if (shouldShowCategory('Usuários')) {
        fillUsuariosTable();
    }
    if (shouldShowCategory('Produto')) {
        fillProdutoTable();
    }
    if (shouldShowCategory('Veículos')) {
        fillVeiculosTable();
    }
    if (shouldShowCategory('Financeiro')) {
        fillFinanceiroTable();
    }
    if (shouldShowCategory('Contabilidade')) {
        fillContabilidadeTable();
    }

    // Atualizar datas (apenas na sidebar)
    updateDates();

    // Criar links após preencher as tabelas
    setTimeout(createTableLinks, 300);

    console.log("Dashboard inicializado com sucesso!");
}

// Sidebar functionality
function initializeSidebar() {
    const toggleSidebar = document.getElementById('toggleSidebar');
    const toggleSidebarMobile = document.getElementById('toggleSidebarMobile');
    const sidebar = document.getElementById('sidebar');

    if (toggleSidebar && sidebar) {
        toggleSidebar.addEventListener('click', function () {
            sidebar.classList.toggle('collapsed');
            const icon = this.querySelector('i');
            const span = this.querySelector('span');
            if (sidebar.classList.contains('collapsed')) {
                icon.className = 'fas fa-chevron-right';
                span.textContent = 'Expandir Menu';
            } else {
                icon.className = 'fas fa-chevron-left';
                span.textContent = 'Recolher Menu';
            }
        });
    }

    if (toggleSidebarMobile && sidebar) {
        toggleSidebarMobile.addEventListener('click', function () {
            sidebar.classList.toggle('collapsed');
        });
    }
}

// Inicialização quando o DOM estiver pronto
document.addEventListener('DOMContentLoaded', function () {
    console.log("DOM Carregado - Iniciando Dashboard");

    initializeSidebar();

    // Aguardar um pouco para garantir que os dados do window estão disponíveis
    setTimeout(initializeDashboard, 200);
});

// Debug detalhado
window.addEventListener('load', function () {
    console.log("=== DEBUG COMPLETO DASHBOARD ===");
    console.log("cond_pag:", window.cond_pag);
    console.log("escol:", window.escol);
    console.log("estado:", window.estado);
    console.log("progresso_total:", window.progresso_total);
    console.log("escopos_habilitados:", window.escopos_habilitados);
    console.log("progresso_categorias:", window.progresso_categorias);

    // Verificar todas as variáveis disponíveis
    const availableVars = Object.keys(window).filter(key =>
        typeof window[key] === 'object' &&
        window[key] !== null &&
        !Array.isArray(window[key]) &&
        key !== 'tableRoutes'
    );
    console.log("Variáveis disponíveis:", availableVars);
});