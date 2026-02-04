/**
 * Frontend Vue.js para Dashboard de Operadoras de Saúde
 * 
 * TRADE-OFFS IMPLEMENTADOS:
 * 
 * 1. Gerenciamento de Estado: Props/Events simples (Opção A)
 *    Justificativa: App pequeno, sem hierarquia complexa
 *    - Sem necessidade de Vuex/Pinia para este escopo
 *    - Menos boilerplate, mais direto
 *    - Fácil de entender e manter
 * 
 * 2. Estratégia de Busca: Servidor (Opção A)
 *    Justificativa: Backend já implementa filtro eficiente
 *    - Menos dados trafegados
 *    - Escala melhor para grandes volumes
 *    - Debounce para evitar requests excessivos
 * 
 * 3. Performance da Tabela: Paginação simples
 *    - 10 itens por página (ajustável)
 *    - Suficiente para volume atual
 *    - Virtual scrolling seria overkill
 * 
 * 4. Tratamento de Erros: Mensagens específicas
 *    - Mostra erro exato da API quando possível
 *    - Estados de loading claros
 *    - Opção de retry
 */

const { createApp } = Vue;

createApp({
    data() {
        return {
            // URL da API
            apiUrl: 'http://localhost:8000/api',
            
            // Estado
            loading: false,
            loadingModal: false,
            erro: null,
            
            // Dados
            operadoras: [],
            estatisticas: null,
            operadoraSelecionada: null,
            despesasOperadora: [],
            
            // Paginação
            paginacao: {
                page: 1,
                limit: 10,
                total_items: 0,
                total_pages: 0
            },
            
            // Busca
            termoBusca: '',
            timerBusca: null,
            
            // Modal
            mostrarModal: false,
            
            // Gráfico
            graficoUF: null
        };
    },
    
    mounted() {
        this.carregarDados();
        this.carregarEstatisticas();
    },
    
    watch: {
        /**
         * Re-renderiza gráfico quando estatísticas mudam
         */
        estatisticas: {
            handler(novoValor, valorAntigo) {
                console.log('=== WATCH ESTATÍSTICAS ATIVADO ===');
                console.log('Novo valor:', novoValor);
                console.log('Valor anterior:', valorAntigo);
                if (novoValor && novoValor.distribuicao_uf && novoValor.distribuicao_uf.length > 0) {
                    console.log('Renderizando gráfico com', novoValor.distribuicao_uf.length, 'UFs');
                    this.$nextTick(() => {
                        this.renderizarGrafico();
                    });
                } else {
                    console.warn('Sem dados para gráfico no watch');
                }
            },
            deep: true
        }
    },
    
    methods: {
        /**
         * Carrega lista de operadoras com paginação
         */
        async carregarDados() {
            this.loading = true;
            this.erro = null;
            
            try {
                const params = new URLSearchParams({
                    page: this.paginacao.page,
                    limit: this.paginacao.limit
                });
                
                if (this.termoBusca) {
                    // Limpar CNPJ (remover pontos, barras, hífens) para busca funcionar
                    const termoLimpo = this.termoBusca.replace(/[.\/-]/g, '');
                    console.log('Busca - Original:', this.termoBusca, '→ Limpo:', termoLimpo);
                    params.append('busca', termoLimpo);
                }
                
                const response = await fetch(`${this.apiUrl}/operadoras?${params}`);
                
                if (!response.ok) {
                    throw new Error(`Erro HTTP: ${response.status}`);
                }
                
                const data = await response.json();
                
                this.operadoras = data.data;
                this.paginacao = {
                    page: data.page,
                    limit: data.limit,
                    total_items: data.total_items,
                    total_pages: data.total_pages
                };
                
            } catch (error) {
                console.error('Erro ao carregar operadoras:', error);
                this.erro = `Não foi possível carregar as operadoras. Verifique se a API está rodando em ${this.apiUrl}`;
            } finally {
                this.loading = false;
            }
        },
        
        /**
         * Carrega estatísticas gerais (com filtro opcional)
         */
        async carregarEstatisticas() {
            try {
                const params = new URLSearchParams();
                
                if (this.termoBusca) {
                    const termoLimpo = this.termoBusca.replace(/[.\/-]/g, '');
                    params.append('busca', termoLimpo);
                    console.log('Estatísticas com filtro:', termoLimpo);
                }
                
                const url = `${this.apiUrl}/estatisticas${params.toString() ? '?' + params : ''}`;
                console.log('URL estatísticas:', url);
                const response = await fetch(url);
                
                if (!response.ok) {
                    throw new Error(`Erro HTTP: ${response.status}`);
                }
                
                const novasEstatisticas = await response.json();
                console.log('Estatísticas recebidas:', novasEstatisticas);
                console.log('Distribuição UF:', novasEstatisticas.distribuicao_uf);
                this.estatisticas = novasEstatisticas;
                
            } catch (error) {
                console.error('Erro ao carregar estatísticas:', error);
                // Não mostrar erro para estatísticas (não crítico)
            }
        },
        
        /**
         * Busca com debounce (evita requests excessivos)
         */
        buscar() {
            clearTimeout(this.timerBusca);
            
            this.timerBusca = setTimeout(() => {
                console.log('=== BUSCA INICIADA ===');
                console.log('Termo de busca:', this.termoBusca);
                this.paginacao.page = 1; // Resetar para primeira página
                this.carregarDados();
                // Recarregar estatísticas para atualizar gráfico com dados filtrados
                console.log('Carregando estatísticas com filtro...');
                this.carregarEstatisticas();
            }, 500); // 500ms de debounce
        },
        
        /**
         * Navega para uma página específica
         */
        irParaPagina(pagina) {
            if (pagina < 1 || pagina > this.paginacao.total_pages) {
                return;
            }
            
            this.paginacao.page = pagina;
            this.carregarDados();
            
            // Scroll para o topo
            window.scrollTo({ top: 0, behavior: 'smooth' });
        },
        
        /**
         * Abre modal com detalhes da operadora
         */
        async verDetalhes(cnpj) {
            this.mostrarModal = true;
            this.loadingModal = true;
            this.operadoraSelecionada = null;
            this.despesasOperadora = [];
            
            try {
                // Carregar detalhes da operadora
                const responseDetalhes = await fetch(`${this.apiUrl}/operadoras/${cnpj}`);
                if (!responseDetalhes.ok) {
                    throw new Error('Operadora não encontrada');
                }
                this.operadoraSelecionada = await responseDetalhes.json();
                
                // Carregar histórico de despesas
                const responseDespesas = await fetch(`${this.apiUrl}/operadoras/${cnpj}/despesas`);
                if (responseDespesas.ok) {
                    const despesas = await responseDespesas.json();
                    // API retorna array diretamente
                    this.despesasOperadora = Array.isArray(despesas) ? despesas : [];
                    console.log(`Despesas carregadas para ${cnpj}:`, this.despesasOperadora.length, 'registros');
                } else {
                    // Operadora sem histórico de despesas
                    console.warn(`Sem despesas para ${cnpj}, status:`, responseDespesas.status);
                    this.despesasOperadora = [];
                }
                
            } catch (error) {
                console.error('Erro ao carregar detalhes:', error);
                alert('Erro ao carregar detalhes da operadora');
                this.fecharModal();
            } finally {
                this.loadingModal = false;
            }
        },
        
        /**
         * Fecha modal de detalhes
         */
        fecharModal() {
            this.mostrarModal = false;
            this.operadoraSelecionada = null;
            this.despesasOperadora = [];
        },
        
        /**
         * Renderiza gráfico de distribuição por UF
         */
        renderizarGrafico() {
            if (!this.estatisticas || !this.estatisticas.distribuicao_uf || this.estatisticas.distribuicao_uf.length === 0) {
                console.warn('Sem dados para gráfico:', this.estatisticas);
                // Se não há dados, destruir gráfico existente
                if (this.graficoUF) {
                    this.graficoUF.destroy();
                    this.graficoUF = null;
                }
                return;
            }
            
            console.log('Renderizando gráfico com', this.estatisticas.distribuicao_uf.length, 'UFs');
            
            const canvas = document.getElementById('chartUF');
            if (!canvas) {
                console.error('Canvas chartUF não encontrado');
                return;
            }
            
            const ctx = canvas.getContext('2d');
            
            // Destruir gráfico anterior se existir
            if (this.graficoUF) {
                console.log('Destruindo gráfico anterior');
                this.graficoUF.destroy();
                this.graficoUF = null;
            }
            
            // Limpar canvas antes de redesenhar
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            
            // Pegar top 10 UFs
            const dados = this.estatisticas.distribuicao_uf.slice(0, 10);
            
            const labels = dados.map(item => item.uf);
            const valores = dados.map(item => item.total_despesas);
            
            // Cores vibrantes
            const cores = [
                '#667eea', '#764ba2', '#f093fb', '#4facfe',
                '#43e97b', '#fa709a', '#fee140', '#30cfd0',
                '#a8edea', '#fed6e3'
            ];
            
            this.graficoUF = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [{
                        label: 'Despesas Totais (R$)',
                        data: valores,
                        backgroundColor: cores,
                        borderColor: cores.map(c => c.replace('0.6', '1')),
                        borderWidth: 2,
                        borderRadius: 8,
                        hoverBackgroundColor: cores.map(c => c + 'dd')
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {
                        legend: {
                            display: false
                        },
                        tooltip: {
                            callbacks: {
                                label: (context) => {
                                    return this.formatarMoeda(context.parsed.y);
                                }
                            }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            ticks: {
                                callback: (value) => {
                                    if (value >= 1000000) {
                                        return 'R$ ' + (value / 1000000).toFixed(1) + 'M';
                                    }
                                    return 'R$ ' + (value / 1000).toFixed(0) + 'K';
                                }
                            }
                        }
                    }
                }
            });
        },
        
        /**
         * Formata valor monetário
         */
        formatarMoeda(valor) {
            if (valor === null || valor === undefined) return 'R$ 0,00';
            
            return new Intl.NumberFormat('pt-BR', {
                style: 'currency',
                currency: 'BRL',
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            }).format(valor);
        },
        
        /**
         * Formata CNPJ
         */
        formatarCNPJ(cnpj) {
            if (!cnpj) return '';
            
            const limpo = cnpj.replace(/\D/g, '');
            
            if (limpo.length !== 14) return cnpj;
            
            return limpo.replace(
                /^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$/,
                '$1.$2.$3/$4-$5'
            );
        }
    }
}).mount('#app');
