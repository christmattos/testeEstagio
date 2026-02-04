"""
Script Python para popular automaticamente o banco de dados MySQL
com os dados gerados nas Partes 1 e 2 do teste
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd
from pathlib import Path
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Carrega dados CSV no banco de dados MySQL"""
    
    def __init__(self, host='localhost', port=3306, database='ans_despesas', 
                 user='root', password=''):
        """
        Inicializa conexão com banco de dados
        
        Args:
            host: Host do MySQL (padrão: localhost)
            port: Porta do MySQL (padrão: 3306)
            database: Nome do database (padrão: ans_despesas)
            user: Usuário do MySQL (padrão: root)
            password: Senha do MySQL
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None
    
    def conectar(self):
        """Estabelece conexão com o banco de dados"""
        try:
            # Primeiro conectar sem especificar database
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                consume_results=True
            )
            self.cursor = self.connection.cursor(buffered=True)
            
            # Reconectar especificando o database
            self.connection.database = self.database
            
            logger.info(f"Conectado ao MySQL: {self.host}:{self.port}/{self.database}")
            return True
            
        except Error as e:
            logger.error(f"Erro ao conectar ao MySQL: {e}")
            return False
    
    def executar_script_sql(self, arquivo_sql):
        """
        Executa um arquivo SQL
        
        Args:
            arquivo_sql: Caminho para o arquivo .sql
        """
        try:
            logger.info(f"Executando script: {arquivo_sql.name}")
            
            with open(arquivo_sql, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            # Dividir em statements individuais
            statements = []
            current_statement = []
            
            for line in sql_script.split('\n'):
                # Ignorar comentários
                if line.strip().startswith('--'):
                    continue
                if line.strip().startswith('/*') or line.strip().endswith('*/'):
                    continue
                
                current_statement.append(line)
                
                # Executar quando encontrar ponto-e-vírgula
                if ';' in line and not line.strip().startswith('--'):
                    statement = '\n'.join(current_statement)
                    
                    # Remover comentários inline e whitespace
                    statement = statement.strip()
                    
                    if statement and not statement.startswith('/*'):
                        try:
                            self.cursor.execute(statement)
                        except Error as e:
                            # Ignorar erros de DROP TABLE IF EXISTS
                            if 'DROP' not in statement.upper():
                                logger.warning(f"Aviso ao executar statement: {e}")
                    
                    current_statement = []
            
            self.connection.commit()
            logger.info(f"Script executado com sucesso\n")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao executar script {arquivo_sql}: {e}")
            return False
    
    def carregar_operadoras(self, arquivo_cadastral):
        """
        Carrega dados cadastrais das operadoras
        
        Args:
            arquivo_cadastral: Path para Relatorio_cadop.csv
        """
        try:
            logger.info("=== CARREGANDO OPERADORAS ===")
            
            if not arquivo_cadastral.exists():
                logger.warning(f"Arquivo não encontrado: {arquivo_cadastral}")
                return False
            
            # Ler CSV
            df = pd.read_csv(arquivo_cadastral, encoding='utf-8-sig', sep=';')
            logger.info(f"Lidos {len(df)} registros do arquivo cadastral")
            
            # Normalizar nomes de colunas
            df.columns = df.columns.str.strip()
            
            # Identificar colunas (podem variar)
            col_cnpj = 'CNPJ' if 'CNPJ' in df.columns else df.columns[1]
            col_registro = 'REGISTRO_OPERADORA' if 'REGISTRO_OPERADORA' in df.columns else df.columns[0]
            col_razao_social = 'Razao_Social' if 'Razao_Social' in df.columns else (
                'RAZAO_SOCIAL' if 'RAZAO_SOCIAL' in df.columns else df.columns[2]
            )
            col_modalidade = 'Modalidade' if 'Modalidade' in df.columns else df.columns[4]
            col_uf = 'UF' if 'UF' in df.columns else df.columns[10]
            
            logger.info(f"Colunas identificadas: CNPJ={col_cnpj}, Registro={col_registro}, "
                       f"RazaoSocial={col_razao_social}, Modalidade={col_modalidade}, UF={col_uf}")
            
            # Preparar dados
            operadoras_inseridas = 0
            operadoras_erro = 0
            
            for idx, row in df.iterrows():
                try:
                    cnpj = str(row[col_cnpj]).replace('.', '').replace('/', '').replace('-', '').strip()
                    
                    # Validar CNPJ
                    if not cnpj or len(cnpj) != 14 or not cnpj.isdigit():
                        continue
                    
                    razao_social = str(row[col_razao_social]).strip() if pd.notna(row[col_razao_social]) else 'NÃO INFORMADO'
                    registro_ans = str(row[col_registro]).strip() if pd.notna(row[col_registro]) else None
                    modalidade = str(row[col_modalidade]).strip() if pd.notna(row[col_modalidade]) else None
                    uf = str(row[col_uf]).strip().upper() if pd.notna(row[col_uf]) else None
                    
                    # Validar UF
                    ufs_validas = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA',
                                  'MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN',
                                  'RS','RO','RR','SC','SP','SE','TO']
                    if uf not in ufs_validas:
                        uf = None
                    
                    # Inserir
                    sql = """
                        INSERT INTO operadoras (cnpj, razao_social, registro_ans, modalidade, uf)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            razao_social = VALUES(razao_social),
                            registro_ans = COALESCE(VALUES(registro_ans), registro_ans),
                            modalidade = COALESCE(VALUES(modalidade), modalidade),
                            uf = COALESCE(VALUES(uf), uf)
                    """
                    
                    self.cursor.execute(sql, (
                        cnpj,
                        razao_social,
                        registro_ans,
                        modalidade,
                        uf
                    ))
                    
                    operadoras_inseridas += 1
                    
                except Exception as e:
                    operadoras_erro += 1
                    if operadoras_erro <= 5:  # Mostrar apenas os primeiros erros
                        logger.warning(f"Erro ao inserir linha {idx}: {e}")
            
            self.connection.commit()
            
            logger.info(f"Operadoras inseridas: {operadoras_inseridas}")
            logger.info(f"Erros/ignorados: {operadoras_erro}\n")
            
            return True
            
        except Exception as e:
            logger.error(f" Erro ao carregar operadoras: {e}")
            return False
    
    def carregar_despesas_consolidadas(self, arquivo_consolidado):
        """
        Carrega dados consolidados de despesas
        
        Args:
            arquivo_consolidado: Path para consolidado_despesas.csv
        """
        try:
            logger.info("=== CARREGANDO DESPESAS CONSOLIDADAS ===")
            
            if not arquivo_consolidado.exists():
                logger.error(f"Arquivo não encontrado: {arquivo_consolidado}")
                return False
            
            # Ler CSV
            df = pd.read_csv(arquivo_consolidado, encoding='utf-8-sig')
            logger.info(f"Lidos {len(df)} registros de despesas consolidadas")
            
            # Validar colunas
            colunas_esperadas = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']
            if not all(col in df.columns for col in colunas_esperadas):
                logger.error(f"Colunas esperadas: {colunas_esperadas}")
                logger.error(f"Colunas encontradas: {df.columns.tolist()}")
                return False
            
            despesas_inseridas = 0
            despesas_erro = 0
            
            for idx, row in df.iterrows():
                try:
                    cnpj = str(row['CNPJ']).replace('.', '').replace('/', '').replace('-', '').strip()
                    razao_social = str(row['RazaoSocial']).strip() if pd.notna(row['RazaoSocial']) else 'NÃO INFORMADO'
                    
                    # Extrair ano e trimestre
                    trimestre_str = str(row['Trimestre']).strip()
                    # Converter "4T" -> 4, "3T" -> 3, etc
                    if 'T' in trimestre_str:
                        trimestre = int(trimestre_str.replace('T', ''))
                    else:
                        trimestre = int(trimestre_str)
                    
                    ano = int(row['Ano'])
                    
                    valor = float(row['ValorDespesas'])
                    
                    # Validar CNPJ
                    if not cnpj or len(cnpj) != 14 or not cnpj.isdigit():
                        continue
                    
                    # Garantir que operadora existe
                    self.cursor.execute(
                        "INSERT IGNORE INTO operadoras (cnpj, razao_social) VALUES (%s, %s)",
                        (cnpj, razao_social)
                    )
                    
                    # Validações básicas (dados inconsistentes não são inseridos)
                    
                    # Inserir despesa
                    sql = """
                        INSERT INTO despesas_consolidadas 
                            (cnpj, razao_social, ano, trimestre, valor_despesas)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            valor_despesas = GREATEST(valor_despesas, VALUES(valor_despesas))
                    """
                    
                    self.cursor.execute(sql, (
                        cnpj, razao_social, ano, trimestre, valor
                    ))
                    
                    despesas_inseridas += 1
                    
                except Exception as e:
                    despesas_erro += 1
                    if despesas_erro <= 5:
                        logger.warning(f"Erro ao inserir linha {idx}: {e}")
            
            self.connection.commit()
            
            logger.info(f"Despesas inseridas: {despesas_inseridas}")
            logger.info(f"Erros/ignorados: {despesas_erro}\n")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar despesas: {e}")
            return False
    
    def carregar_despesas_agregadas(self, arquivo_agregado):
        """
        Carrega dados agregados
        
        Args:
            arquivo_agregado: Path para despesas_agregadas.csv
        """
        try:
            logger.info("=== CARREGANDO DESPESAS AGREGADAS ===")
            
            if not arquivo_agregado.exists():
                logger.warning(f"Arquivo não encontrado: {arquivo_agregado}")
                logger.info("Pulando carga de dados agregados (arquivo opcional)\n")
                return True
            
            # Ler CSV
            df = pd.read_csv(arquivo_agregado, encoding='utf-8-sig')
            logger.info(f"Lidos {len(df)} registros agregados")
            
            agregadas_inseridas = 0
            agregadas_erro = 0
            
            for idx, row in df.iterrows():
                try:
                    razao_social = str(row['RazaoSocial']).strip()
                    uf = str(row['UF']).strip().upper() if pd.notna(row['UF']) else None
                    
                    # Tentar buscar CNPJ pela razão social E UF
                    cnpj = None
                    self.cursor.execute(
                        "SELECT cnpj FROM operadoras WHERE razao_social = %s AND uf = %s LIMIT 1",
                        (razao_social, uf)
                    )
                    result = self.cursor.fetchone()
                    if result:
                        cnpj = result[0]
                    
                    # Inserir dados agregados
                    sql = """
                        INSERT INTO despesas_agregadas 
                            (cnpj, razao_social, uf, total_despesas, media_despesas, desvio_padrao, total_trimestres)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            total_despesas = VALUES(total_despesas),
                            media_despesas = VALUES(media_despesas),
                            desvio_padrao = VALUES(desvio_padrao),
                            total_trimestres = VALUES(total_trimestres)
                    """
                    
                    self.cursor.execute(sql, (
                        cnpj,
                        razao_social,
                        uf,
                        float(row['Total_Despesas']),
                        float(row['Media_Despesas']),
                        float(row['Desvio_Padrao']) if pd.notna(row['Desvio_Padrao']) else None,
                        int(row['Qtd_Trimestres'])
                    ))
                    
                    agregadas_inseridas += 1
                    
                except Exception as e:
                    agregadas_erro += 1
                    if agregadas_erro <= 5:
                        logger.warning(f"Erro ao inserir linha {idx}: {e}")
            
            self.connection.commit()
            
            logger.info(f"Agregadas inseridas: {agregadas_inseridas}")
            logger.info(f"Erros/ignorados: {agregadas_erro}\n")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao carregar agregadas: {e}")
            return False
    
    def exibir_estatisticas(self):
        """Exibe estatísticas do banco de dados"""
        try:
            logger.info("=== ESTATÍSTICAS DO BANCO DE DADOS ===\n")
            
            # Total de registros por tabela
            tabelas = ['operadoras', 'despesas_consolidadas', 'despesas_agregadas']
            
            for tabela in tabelas:
                self.cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
                total = self.cursor.fetchone()[0]
                logger.info(f"{tabela:25} {total:>6} registros")
            
            logger.info("\n")
            
            # Distribuição por UF
            logger.info("Distribuição por UF (Top 5):")
            self.cursor.execute("""
                SELECT o.uf, COUNT(*) AS total
                FROM despesas_consolidadas d
                INNER JOIN operadoras o ON d.cnpj = o.cnpj
                WHERE o.uf IS NOT NULL
                GROUP BY o.uf
                ORDER BY total DESC
                LIMIT 5
            """)
            
            for uf, total in self.cursor.fetchall():
                logger.info(f"  {uf}: {total} registros")
            
            logger.info("\n")
            
            logger.info("")
            
        except Exception as e:
            logger.error(f"Erro ao exibir estatísticas: {e}")
    
    def fechar(self):
        """Fecha conexão com o banco"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Conexão fechada")


def main():
    """Função principal"""
    logger.info("=== CARREGAMENTO DE DADOS NO MYSQL ===\n")
    
    # Configuração do banco
    DB_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'database': 'ans_despesas',
        'user': 'root',
        'password': '1234'
    }
    
    # Arquivos de dados
    base_dir = Path(__file__).parent.parent
    arquivo_cadastral = base_dir / 'parte2' / 'dados' / 'Relatorio_cadop.csv'
    arquivo_consolidado = base_dir / 'parte1' / 'resultados' / 'consolidado_despesas.csv'
    arquivo_agregado = base_dir / 'parte2' / 'resultados' / 'despesas_agregadas.csv'
    
    # Script SQL
    script_schema = Path(__file__).parent / 'schema.sql'
    
    # Verificar arquivos
    logger.info("Verificando arquivos...")
    if not script_schema.exists():
        logger.error(f"Schema não encontrado: {script_schema}")
        return
    
    if not arquivo_consolidado.exists():
        logger.error(f"Arquivo consolidado não encontrado: {arquivo_consolidado}")
        logger.info("Execute primeiro a Parte 1 (demo_parte1.py)")
        return
    
    logger.info(f"Schema: {script_schema}")
    logger.info(f"Consolidado: {arquivo_consolidado}")
    logger.info(f"{'Cadastral: ' + str(arquivo_cadastral) if arquivo_cadastral.exists() else 'Cadastral: Arquivo não encontrado'}")
    logger.info(f"{'Agregado: ' + str(arquivo_agregado) if arquivo_agregado.exists() else 'Agregado: Arquivo não encontrado'}\n")
    
    # Inicializar loader
    loader = DatabaseLoader(**DB_CONFIG)
    
    try:
        # Conectar
        if not loader.conectar():
            return
        
        # Executar schema
        logger.info("=== CRIANDO ESTRUTURA DO BANCO ===")
        if not loader.executar_script_sql(script_schema):
            logger.error("Falha ao criar estrutura")
            return
        
        # Carregar dados
        if arquivo_cadastral.exists():
            loader.carregar_operadoras(arquivo_cadastral)
        
        loader.carregar_despesas_consolidadas(arquivo_consolidado)
        
        if arquivo_agregado.exists():
            loader.carregar_despesas_agregadas(arquivo_agregado)
        
        # Estatísticas
        loader.exibir_estatisticas()
        
        logger.info("CARGA CONCLUÍDA COM SUCESSO!")
        logger.info(f"\nBanco de dados: {DB_CONFIG['database']}")
        logger.info(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        logger.info("\nPróximos passos:")
        logger.info("1. Conecte-se ao MySQL com seu cliente favorito")
        logger.info(f"2. USE {DB_CONFIG['database']};")
        logger.info("3. Execute as queries em queries_analiticas.sql")
        
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        loader.fechar()


if __name__ == "__main__":
    main()
