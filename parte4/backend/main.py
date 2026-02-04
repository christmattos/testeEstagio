"""
API REST para consulta de dados de operadoras de saúde
Desenvolvido com FastAPI + Python 3.13
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
from decimal import Decimal

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar FastAPI
app = FastAPI(
    title="API de Operadoras de Saúde",
    description="API REST para consulta de dados das operadoras de planos de saúde (ANS)",
    version="1.0.0",
    docs_url="/docs",  # Swagger UI
    redoc_url="/redoc"  # ReDoc
)

# CORS - Permitir requisições do frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# MODELOS PYDANTIC (Validação e Documentação)
# ============================================================================

class Operadora(BaseModel):
    """Modelo de dados de uma operadora"""
    cnpj: str = Field(..., description="CNPJ da operadora (14 dígitos)")
    razao_social: str = Field(..., description="Razão social da operadora")
    registro_ans: Optional[str] = Field(None, description="Registro ANS")
    modalidade: Optional[str] = Field(None, description="Modalidade (Medicina de Grupo, Cooperativa, etc)")
    uf: Optional[str] = Field(None, description="UF da sede")
    
    class Config:
        schema_extra = {
            "example": {
                "cnpj": "00000000000191",
                "razao_social": "BRADESCO SAUDE S.A.",
                "registro_ans": "300011",
                "modalidade": "Cooperativa Médica",
                "uf": "SP"
            }
        }

class Despesa(BaseModel):
    """Modelo de despesa de uma operadora em um trimestre"""
    cnpj: str
    razao_social: str
    ano: int = Field(..., ge=2020, le=2099, description="Ano")
    trimestre: int = Field(..., ge=1, le=4, description="Trimestre (1-4)")
    valor_despesas: float = Field(..., description="Valor das despesas")
    periodo: str = Field(..., description="Período formatado (ex: 2024-Q1)")
    
    class Config:
        schema_extra = {
            "example": {
                "cnpj": "00000000000191",
                "razao_social": "BRADESCO SAUDE S.A.",
                "ano": 2024,
                "trimestre": 1,
                "valor_despesas": 2300000.00,
                "periodo": "2024-Q1"
            }
        }

class OperadoraDetalhada(Operadora):
    """Modelo de operadora com informações agregadas"""
    total_despesas: Optional[float] = Field(None, description="Total de despesas")
    media_despesas: Optional[float] = Field(None, description="Média de despesas")
    qtd_trimestres: Optional[int] = Field(None, description="Quantidade de trimestres com dados")

class Estatisticas(BaseModel):
    """Modelo de estatísticas gerais"""
    total_operadoras: int
    total_despesas: float
    media_despesas: float
    top_5_operadoras: List[dict]
    distribuicao_uf: List[dict]
    data_atualizacao: str

class PaginatedResponse(BaseModel):
    data: List[dict]
    total_items: int
    page: int
    limit: int
    total_pages: int

# ============================================================================
# CONEXÃO COM BANCO DE DADOS MYSQL
# ============================================================================

class DatabaseConnection:
    """
    Trade-off: Fonte de dados (CSV vs Banco de Dados)
    """
    
    def __init__(self):
        self.config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '1234',
            'database': 'ans_despesas',
            'charset': 'utf8mb4'
        }
        self.test_connection()
    
    def test_connection(self):
        """Testa conexão com o banco de dados"""
        try:
            conn = mysql.connector.connect(**self.config)
            cursor = conn.cursor()
            
            # Verificar se as tabelas existem
            cursor.execute("SHOW TABLES")
            tabelas = [t[0] for t in cursor.fetchall()]
            
            logger.info(f"Conectado ao MySQL - Banco: ans_despesas")
            logger.info(f"Tabelas encontradas: {', '.join(tabelas)}")
            
            # Contar registros
            for tabela in ['operadoras', 'despesas_consolidadas', 'despesas_agregadas']:
                if tabela in tabelas:
                    cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
                    count = cursor.fetchone()[0]
                    logger.info(f"   - {tabela}: {count} registros")
            
            cursor.close()
            conn.close()
            
        except Error as e:
            logger.error(f"❌ Erro ao conectar ao MySQL: {e}")
            logger.error("   Verifique se o MySQL está rodando e se o banco 'ans_despesas' foi criado (Parte 3)")
            raise
    
    def get_connection(self):
        """Retorna uma nova conexão com o banco"""
        return mysql.connector.connect(**self.config)

# Instância global da conexão
db = DatabaseConnection()

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def limpar_cnpj(cnpj: str) -> str:
    """Remove formatação do CNPJ"""
    return str(cnpj).replace('.', '').replace('/', '').replace('-', '').strip()

def converter_decimal(valor) -> float:
    """Converte Decimal para float"""
    if isinstance(valor, Decimal):
        return float(valor)
    return valor if valor is not None else 0.0

# ============================================================================
# ROTAS DA API
# ============================================================================

@app.get("/", tags=["Root"])
async def root():
    """Rota raiz - informações da API"""
    return {
        "api": "API de Operadoras de Saúde",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": [
            "GET /api/operadoras",
            "GET /api/operadoras/{cnpj}",
            "GET /api/operadoras/{cnpj}/despesas",
            "GET /api/estatisticas"
        ]
    }

@app.get("/api/operadoras", response_model=PaginatedResponse, tags=["Operadoras"])
async def listar_operadoras(
    page: int = Query(1, ge=1, description="Número da página"),
    limit: int = Query(10, ge=1, le=100, description="Itens por página"),
    busca: Optional[str] = Query(None, description="Buscar por razão social ou CNPJ")
):
    """
    Lista todas as operadoras com paginação
    """
    try:
        conn = db.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Query base
        where_clause = ""
        params = []
        
        if busca:
            where_clause = "WHERE razao_social LIKE %s OR cnpj LIKE %s"
            busca_param = f"%{busca}%"
            params = [busca_param, busca_param]
        
        # Contar total de operadoras
        count_query = f"SELECT COUNT(*) as total FROM operadoras {where_clause}"
        cursor.execute(count_query, params)
        total_items = cursor.fetchone()['total']
        total_pages = (total_items + limit - 1) // limit
        
        # Buscar operadoras com paginação
        offset = (page - 1) * limit
        query = f"""
            SELECT cnpj, razao_social, modalidade, uf
            FROM operadoras
            {where_clause}
            ORDER BY razao_social
            LIMIT %s OFFSET %s
        """
        cursor.execute(query, params + [limit, offset])
        operadoras = cursor.fetchall()
        
        # Formatar resultado
        resultado = []
        for op in operadoras:
            resultado.append({
                'cnpj': op['cnpj'],
                'razao_social': op['razao_social'],
                'registro_ans': None,
                'modalidade': op['modalidade'],
                'uf': op['uf']
            })
        
        cursor.close()
        conn.close()
        
        return PaginatedResponse(
            data=resultado,
            total_items=total_items,
            page=page,
            limit=limit,
            total_pages=total_pages
        )
        
    except Error as e:
        logger.error(f"Erro ao listar operadoras: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/operadoras/{cnpj}", response_model=OperadoraDetalhada, tags=["Operadoras"])
async def obter_operadora(cnpj: str):
    """
    Retorna detalhes de uma operadora específica
    
    Inclui informações cadastrais e estatísticas agregadas
    """
    try:
        cnpj_limpo = limpar_cnpj(cnpj)
        
        conn = db.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Buscar dados da operadora
        query = """
            SELECT o.cnpj, o.razao_social, o.modalidade, o.uf,
                   a.total_despesas, a.media_despesas, a.total_trimestres
            FROM operadoras o
            LEFT JOIN despesas_agregadas a ON o.cnpj = a.cnpj
            WHERE o.cnpj = %s
            LIMIT 1
        """
        cursor.execute(query, (cnpj_limpo,))
        resultado = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not resultado:
            raise HTTPException(
                status_code=404,
                detail=f"Operadora com CNPJ {cnpj} não encontrada"
            )
        
        # Usar total_trimestres da agregação ou contar manualmente
        qtd_trimestres = resultado.get('total_trimestres')
        if not qtd_trimestres:
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM despesas_consolidadas WHERE cnpj = %s", (cnpj_limpo,))
            qtd_trimestres = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        
        return OperadoraDetalhada(
            cnpj=resultado['cnpj'],
            razao_social=resultado['razao_social'],
            registro_ans=None,
            modalidade=resultado['modalidade'],
            uf=resultado['uf'],
            total_despesas=converter_decimal(resultado['total_despesas']),
            media_despesas=converter_decimal(resultado['media_despesas']),
            qtd_trimestres=qtd_trimestres or 0
        )
        
    except HTTPException:
        raise
    except Error as e:
        logger.error(f"Erro ao obter operadora {cnpj}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/operadoras/{cnpj}/despesas", response_model=List[Despesa], tags=["Despesas"])
async def obter_despesas_operadora(cnpj: str):
    """
    Retorna histórico de despesas de uma operadora
    
    Ordenado por ano e trimestre (mais recente primeiro)
    """
    try:
        cnpj_limpo = limpar_cnpj(cnpj)
        
        conn = db.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = """
            SELECT d.cnpj, o.razao_social, d.ano, d.trimestre, d.valor_despesas
            FROM despesas_consolidadas d
            INNER JOIN operadoras o ON d.cnpj = o.cnpj
            WHERE d.cnpj = %s
            ORDER BY d.ano DESC, d.trimestre DESC
        """
        cursor.execute(query, (cnpj_limpo,))
        despesas = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not despesas:
            raise HTTPException(
                status_code=404,
                detail=f"Nenhuma despesa encontrada para CNPJ {cnpj}"
            )
        
        # Converter para lista de dicionários
        resultado = []
        for desp in despesas:
            resultado.append({
                'cnpj': desp['cnpj'],
                'razao_social': desp['razao_social'],
                'ano': int(desp['ano']),
                'trimestre': int(desp['trimestre']),
                'valor_despesas': converter_decimal(desp['valor_despesas']),
                'periodo': f"{int(desp['ano'])}-Q{int(desp['trimestre'])}"
            })
        
        return resultado
        
    except HTTPException:
        raise
    except Error as e:
        logger.error(f"Erro ao obter despesas da operadora {cnpj}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/estatisticas", response_model=Estatisticas, tags=["Estatísticas"])
async def obter_estatisticas(busca: str = None):
    """
    Retorna estatísticas gerais agregadas
    """
    try:
        conn = db.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Construir filtro WHERE se houver busca
        where_clause = ""
        params = []
        
        if busca:
            where_clause = "WHERE o.razao_social LIKE %s OR o.cnpj LIKE %s"
            busca_param = f"%{busca}%"
            params = [busca_param, busca_param]
        
        # Total de operadoras
        count_query = f"SELECT COUNT(*) as total FROM operadoras o {where_clause}"
        cursor.execute(count_query, params)
        total_operadoras = cursor.fetchone()['total']
        
        # Total e média de despesas
        # Usa apenas despesas_agregadas pois já tem valores corretos e todas operadoras
        if busca:
            # Se houver busca, precisa do JOIN com operadoras
            despesas_query = f"""
                SELECT 
                    SUM(a.total_despesas) as total,
                    AVG(a.total_despesas) as media
                FROM despesas_agregadas a
                INNER JOIN operadoras o ON a.cnpj = o.cnpj
                {where_clause}
            """
            cursor.execute(despesas_query, params)
        else:
            # Sem busca, usa todos os dados de despesas_agregadas
            cursor.execute("""
                SELECT 
                    SUM(total_despesas) as total,
                    AVG(total_despesas) as media
                FROM despesas_agregadas
            """)
        
        despesas_stats = cursor.fetchone()
        total_despesas = converter_decimal(despesas_stats['total'])
        media_despesas = converter_decimal(despesas_stats['media'])
        
        # Top 5 operadoras por despesas totais
        if busca:
            top5_query = """
                SELECT 
                    COALESCE(o.cnpj, a.cnpj) as cnpj, 
                    COALESCE(o.razao_social, a.razao_social) as razao_social, 
                    COALESCE(o.uf, a.uf) as uf, 
                    a.total_despesas as total
                FROM despesas_agregadas a
                LEFT JOIN operadoras o ON a.cnpj = o.cnpj
                WHERE (o.razao_social LIKE %s OR o.cnpj LIKE %s OR a.razao_social LIKE %s OR a.cnpj LIKE %s)
                ORDER BY total DESC
                LIMIT 5
            """
            top5_params = params + params  # 4 parâmetros (2x busca_param)
            cursor.execute(top5_query, top5_params)
        else:
            top5_query = """
                SELECT 
                    COALESCE(o.cnpj, a.cnpj) as cnpj, 
                    COALESCE(o.razao_social, a.razao_social) as razao_social, 
                    COALESCE(o.uf, a.uf) as uf, 
                    a.total_despesas as total
                FROM despesas_agregadas a
                LEFT JOIN operadoras o ON a.cnpj = o.cnpj
                ORDER BY total DESC
                LIMIT 5
            """
            cursor.execute(top5_query)
        
        top_5 = cursor.fetchall()
        
        top_5_operadoras = []
        for op in top_5:
            top_5_operadoras.append({
                'cnpj': op['cnpj'],
                'razao_social': op['razao_social'],
                'total_despesas': converter_decimal(op['total']),
                'uf': op['uf']
            })
        
        # Distribuição por UF (com filtro)
        if busca:
            uf_query = f"""
                SELECT 
                    COALESCE(o.uf, a.uf) as uf, 
                    SUM(a.total_despesas) as total, 
                    COUNT(*) as qtd
                FROM despesas_agregadas a
                LEFT JOIN operadoras o ON a.cnpj = o.cnpj
                WHERE (o.razao_social LIKE %s OR o.cnpj LIKE %s OR a.razao_social LIKE %s OR a.cnpj LIKE %s)
                  AND COALESCE(o.uf, a.uf) IS NOT NULL
                GROUP BY COALESCE(o.uf, a.uf)
                ORDER BY total DESC
            """
            # Quadruplicar parâmetros de busca para as 4 condições LIKE
            uf_params = params + params
            cursor.execute(uf_query, uf_params)
        else:
            uf_query = """
                SELECT 
                    COALESCE(o.uf, a.uf) as uf, 
                    SUM(a.total_despesas) as total, 
                    COUNT(*) as qtd
                FROM despesas_agregadas a
                LEFT JOIN operadoras o ON a.cnpj = o.cnpj
                WHERE COALESCE(o.uf, a.uf) IS NOT NULL
                GROUP BY COALESCE(o.uf, a.uf)
                ORDER BY total DESC
            """
            cursor.execute(uf_query)
        
        uf_stats = cursor.fetchall()
        
        distribuicao_uf = []
        for uf in uf_stats:
            distribuicao_uf.append({
                'uf': uf['uf'],
                'total_despesas': converter_decimal(uf['total']),
                'qtd_registros': int(uf['qtd'])
            })
        
        cursor.close()
        conn.close()
        
        return Estatisticas(
            total_operadoras=total_operadoras,
            total_despesas=total_despesas,
            media_despesas=media_despesas,
            top_5_operadoras=top_5_operadoras,
            distribuicao_uf=distribuicao_uf,
            data_atualizacao=datetime.now().isoformat()
        )
        
    except Error as e:
        logger.error(f"Erro ao calcular estatísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# HEALTH CHECK
# ============================================================================

@app.get("/health", tags=["Health"])
async def health_check():
    """Verifica se a API está funcionando"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Contar registros em cada tabela
        stats = {}
        for tabela in ['operadoras', 'despesas_consolidadas', 'despesas_agregadas']:
            cursor.execute(f"SELECT COUNT(*) as total FROM {tabela}")
            stats[tabela] = cursor.fetchone()['total']
        
        cursor.close()
        conn.close()
        
        return {
            "status": "healthy",
            "database": "MySQL - ans_despesas",
            "timestamp": datetime.now().isoformat(),
            "data_loaded": stats
        }
    except Error as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ============================================================================
# EXECUÇÃO
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 80)
    print("API de Operadoras de Saúde")
    print("=" * 80)
    print("\nDocumentação interativa:")
    print("   Swagger UI: http://localhost:8000/docs")
    print("   ReDoc:      http://localhost:8000/redoc")
    print("\nEndpoints disponíveis:")
    print("   GET  /api/operadoras")
    print("   GET  /api/operadoras/{cnpj}")
    print("   GET  /api/operadoras/{cnpj}/despesas")
    print("   GET  /api/estatisticas")
    print("\nServidor iniciando...\n")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Auto-reload durante desenvolvimento
        log_level="info"
    )
