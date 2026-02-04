# Teste de Estágio

##  Pré-requisitos

Antes de começar, certifique-se de ter instalado:

- **Python 3.10 ou superior** - [Download](https://www.python.org/downloads/)
- **MySQL 8.0 ou superior** - [Download](https://dev.mysql.com/downloads/mysql/)
- **Git** (opcional) - [Download](https://git-scm.com/downloads)

---

##  Instalação (**Somente** Windows)


1. **Clone ou baixe o projeto**
   ```bash
   git clone <url-do-repositorio>
   cd teste_estagio
   ```

2. **Execute o script de setup**
   ```cmd
   setup.bat
   ```
   
   Este script irá:
   - Verificar se Python, MYSQL e pip estão instalados

3. **Instale as dependências**  
   Instale as dependências uma por uma:

   ```
      pip install -r parte1\requirements.txt
      pip install -r parte2\requirements.txt
      pip install -r parte3\requirements.txt
      pip install -r parte4\backend\requirements.txt
   ```
4. **Configure o MySQL**
   - Usuário: `root`
   - Senha: `1234`
   - Porta: `3306`
   
   **Se usar credenciais diferentes**, edite diretamente:
   - [parte3/load_data.py](parte3/load_data.py#L429)
   - [parte3/executar_queries.py](parte3/executar_queries.py#L11)
   - [parte4/backend/main.py](parte4/backend/main.py#L118)



## Execução Passo a Passo

### **Parte 1: Integração ANS**

Baixa dados dos últimos 3 trimestres da API ANS.

```cmd
cd parte1
python integracao_ans.py
```

**Output**: `resultados/consolidado_despesas.zip`

---

### **Parte 2: Transformação e Validação**

Valida CNPJs, enriquece dados e agrega informações.

```cmd
cd ..\parte2
python transformacao_validacao.py
```

**Outputs**:
- `resultados/dados_validados.csv`
- `resultados/dados_agregados.csv`

---

### **Parte 3: Banco de Dados**

Cria banco MySQL e carrega os dados.

```cmd
cd ..\parte3
python load_data.py
```

**Executa queries analíticas:**
```cmd
python executar_queries.py
```

---

### **Parte 4: API REST + Frontend**

#### **Backend (Terminal 1)**

```cmd
cd ..\parte4\backend
run_backend.bat
```

Ou manualmente:
```cmd
cd C:\caminho\para\teste_estagio
.venv\Scripts\activate
cd parte4\backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Endpoints disponíveis:**
- API: http://localhost:8000/api/operadoras
- Documentação Swagger: http://localhost:8000/docs
- Documentação ReDoc: http://localhost:8000/redoc

#### **Frontend (Terminal 2)**

```cmd
cd parte4\frontend
python -m http.server 5173
```

**Acesse**: http://localhost:5173



## Trade-offs Técnicos Implementados

### **Parte 1: Integração ANS**

#### **1. Processamento de Arquivos: Incremental vs. Em Memória**
**Decisão:** Processar arquivos incrementalmente (um de cada vez)

**Justificativa:**
- Arquivos da ANS podem ter centenas de MB
- Processamento incremental permite lidar com arquivos de qualquer tamanho
- Evita problemas de falta de memória em máquinas com menos RAM


#### **2. Validação de CNPJ: Completa vs. Básica**
**Decisão:** Validação completa com algoritmo de dígitos verificadores

**Justificativa:**
- Detecta erros de digitação e CNPJs inválidos
- Garante qualidade dos dados antes de inserir no banco
- É mais lento


#### **3. Tratamento de Inconsistências: Exclusão vs. Marcação**
**Decisão:** Manter registros inconsistentes mas marcar com flags

**Justificativa:**

- Permite análise depois

---

### **Parte 2: Processamento de Dados**


#### **4. Cálculo de Média: MEAN vs. Total/Trimestres**
**Decisão:** `Total / Número de Trimestres` em vez de `MEAN()`

**Justificativa:**
- Dados acumulados não podem usar MEAN() diretamente
- Total / Trimestres fornece média real do período
- Exemplo: 3T (acumulado=300) → Média=300/3=100 por trimestre

#### **5. Desvio Padrão: 0 ou Omitir**
**Decisão:** Desvio padrão = 0 para dados acumulados

**Justificativa:**
- Não faz sentido calcular desvio padrão de valores cumulativos
- Seria necessário "desacumular" os valores primeiro
- Para esta análise exploratória, média é suficiente

---

### **Parte 3: Banco de Dados e Queries**

#### **6. Normalização: Desnormalizado vs. Múltiplas Tabelas**
**Decisão:** Estrutura desnormalizada com 3 tabelas (operadoras, despesas_consolidadas, despesas_agregadas)

**Justificativa:**
- Performance superior em queries analíticas (menos JOINs)
- Simplicidade de manutenção

#### **7. Query 1: LAG() vs. Self-Join para Crescimento**
**Decisão:** Window function `LAG()` para comparar trimestres consecutivos

**Justificativa:**
- Sintaxe mais clara e declarativa
- Performance superior a self-joins múltiplos
- Permite calcular crescimento real (período vs. período) em dados acumulados


#### **8. Tipo de JOIN: LEFT vs. INNER**
**Decisão:** `LEFT JOIN` para incluir todas operadoras

**Justificativa:**
- Mantém operadoras mesmo sem despesas no período
- Evita viés de seleção (excluir operadoras novas/inativas)
- Porém requer tratamento de NULLs nas queries


---

### **Parte 4: API e Frontend**

#### **9. Framework Backend: FastAPI vs. Flask**
**Decisão:** FastAPI

**Justificativa:**
- **Documentação automática** (Swagger/OpenAPI) - crucial para demonstrar a API
- Validação de dados com Pydantic (type-safe)
- Suporte nativo a async/await
- Performance superior (ASGI vs WSGI)

#### **10. Busca: Server-side vs. Client-side**
**Decisão:** Busca no servidor (backend processa filtro)

**Justificativa:**
- Menos dados trafegados pela rede
- Escala melhor para grandes volumes
- Backend já tem acesso direto ao MySQL
- Demora menos se for fazer uma procura de somente um item


#### **11. Gerenciamento de Estado: Props/Events vs. Vuex**
**Decisão:** Props/Events simples (sem biblioteca de estado)

**Justificativa:**
- Mais direto
- Fácil de entender e manter

#### **12. Performance de Tabela: Paginação vs. Virtual Scrolling**
**Decisão:** Paginação simples (10 itens/página)

**Justificativa:**
- Suficiente para volume atual (~centenas de operadoras)
- Implementação trivial


#### **13. Reatividade de Gráficos: Watch vs. Manual**
**Decisão:** Vue.js `watch` para re-render automático

**Justificativa:**
- Gráfico atualiza automaticamente ao buscar/paginar
- Aproveita reatividade do Vue.js
- Menos código imperativo

#### **14. Trade-off: Estrutura de resposta da API**
**Decisão:** Dados + Metadados (Opção B)

**Justificativa:**
- Frontend precisa de total_items para calcular páginas
- Facilita implementação de paginação no cliente
- Melhora UX (mostra "Página 1 de 10")

#### **15. Trade-off: Fonte de dados (CSV vs Banco de Dados)**
**Decisão:** MySQL (Parte 3)
    
**Justificativa:**
- Dados já carregados e normalizados na Parte 3
- Queries otimizadas com índices


#### **16. Trade-off: Estratégia de Paginação**
**Decisão:** Offset-based (Opção A)

**Justificativa:**
- Simples de implementar

#### **17. Trade-off: Cache vs Queries Diretas**
**Decisão:** Calcular sempre na hora (Opção A) para demonstração

**Justificativa:**
- Dados sempre atualizados
- Simples de implementar