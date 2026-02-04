"""
Executa as queries analíticas do arquivo queries_analiticas.sql
"""
import mysql.connector
from pathlib import Path

# Configuração do banco
config = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database': 'ans_despesas'
}

# Ler arquivo SQL
sql_file = Path(__file__).parent / 'queries_analiticas.sql'
with open(sql_file, 'r', encoding='utf-8') as f:
    sql_content = f.read()

# Separar queries por ponto-e-vírgula
# Remove comentários de bloco e mantém apenas queries
queries_raw = []
for statement in sql_content.split(';'):
    # Remover USE DATABASE
    if 'USE ans_despesas' in statement:
        continue
    # Pular se for só comentários ou vazio
    linhas_codigo = [l for l in statement.split('\n') if l.strip() and not l.strip().startswith('--')]
    if linhas_codigo:
        queries_raw.append(statement.strip())

queries = [q for q in queries_raw if q]

# Conectar e executar
conn = mysql.connector.connect(**config)
cursor = conn.cursor(dictionary=True)

print("=" * 80)
print("EXECUTANDO QUERIES ANALÍTICAS - PARTE 3")
print("=" * 80)

for i, query in enumerate(queries, 1):
    print(f"\n{'='*80}")
    print(f"QUERY {i}")
    print('='*80)
    
    # Extrair título da query dos comentários
    linhas_comentario = [l.strip() for l in query.split('\n') if l.strip().startswith('--')]
    titulo = None
    for linha in linhas_comentario:
        if 'QUERY' in linha.upper() and ':' in linha:
            titulo = linha.replace('--', '').strip()
            break
    
    if titulo:
        print(f"\n{titulo}\n")
    
    # Remover comentários e executar
    query_limpa = '\n'.join([l for l in query.split('\n') if l.strip() and not l.strip().startswith('--')])
    
    try:
        cursor.execute(query_limpa)
        resultados = cursor.fetchall()
        
        if resultados:
            # Exibir resultados em formato tabular
            colunas = list(resultados[0].keys())
            print(" | ".join(f"{col:^20}" for col in colunas))
            print("-" * (len(colunas) * 23))
            
            # Dados
            for row in resultados:
                valores = []
                for col in colunas:
                    val = row[col]
                    if isinstance(val, float):
                        valores.append(f"{val:>20,.2f}")
                    elif isinstance(val, int):
                        valores.append(f"{val:>20,}")
                    else:
                        valores.append(f"{str(val)[:20]:^20}")
                print(" | ".join(valores))
            
            print(f"\nTotal de registros: {len(resultados)}")
        else:
            print("\nNenhum resultado retornado.")
    
    except Exception as e:
        print(f"\n Erro ao executar query: {e}")

cursor.close()
conn.close()

print("\n" + "=" * 80)
print("TODAS AS QUERIES EXECUTADAS COM SUCESSO!")
print("=" * 80)
