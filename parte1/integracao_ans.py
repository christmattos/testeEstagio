"""
INTEGRAÇÃO COM API PÚBLICA - ANS
Parte 1 do Teste
"""

import os
import re
import zipfile
import csv
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import logging
from typing import List, Dict, Tuple
import chardet

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ANSDataIntegrator:
    """Classe para integração com dados da ANS"""
    
    def __init__(self, base_url: str = "https://dadosabertos.ans.gov.br/FTP/PDA/"):
        self.base_url = base_url
        self.demonstracoes_url = base_url + "demonstracoes_contabeis/"
        self.operadoras_url = base_url + "operadoras_de_plano_de_saude_ativas/"
        self.dados_dir = Path("dados")
        self.resultados_dir = Path("resultados")
        
        # Criar diretórios se não existirem
        self.dados_dir.mkdir(exist_ok=True)
        self.resultados_dir.mkdir(exist_ok=True)
        
        # Cache de operadoras (REG_ANS -> dados)
        self.operadoras_cache = None
    
    def identificar_trimestres_disponiveis(self) -> List[Tuple[str, str]]:
        """
        Identifica os trimestres disponíveis na API da ANS
        Retorna: Lista de tuplas (ano, trimestre, url_arquivo_zip)
        """
        logger.info("Identificando trimestres disponíveis...")
        trimestres = []
        
        try:
            response = requests.get(self.demonstracoes_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Procurar por links que representam anos (2020, 2021, etc.)
            for link in soup.find_all('a'):
                href = link.get('href', '')
                # Procurar padrão de ano (4 dígitos seguido de /)
                match_ano = re.match(r'^(\d{4})/$', href)
                if match_ano:
                    ano = match_ano.group(1)
                    logger.info(f"Verificando ano: {ano}")
                    
                    # Buscar arquivos ZIP dentro de cada ano
                    ano_url = self.demonstracoes_url + href
                    ano_response = requests.get(ano_url, timeout=30)
                    ano_soup = BeautifulSoup(ano_response.content, 'html.parser')
                    
                    for zip_link in ano_soup.find_all('a'):
                        zip_href = zip_link.get('href', '')
                        
                        # Tentar múltiplos padrões de nome de arquivo
                        trimestre_num = None
                        ano_arquivo = ano
                        
                        # Padrão 1: 1T2025.zip, 2T2024.zip, 20130416_1T2012.zip
                        match1 = re.search(r'(\d)[Tt](\d{4})\.zip$', zip_href, re.IGNORECASE)
                        if match1:
                            trimestre_num = match1.group(1)
                            ano_arquivo = match1.group(2)
                        
                        # Padrão 2: 3-Trimestre.zip, 1-trimestre.zip
                        if not trimestre_num:
                            match2 = re.match(r'^(\d)-?[Tt]rimestre\.zip$', zip_href, re.IGNORECASE)
                            if match2:
                                trimestre_num = match2.group(1)
                        
                        # Padrão 3: 2010_2_trimestre.zip, 2010_1_Trimestre.zip
                        if not trimestre_num:
                            match3 = re.search(r'(\d{4})[_-](\d)[_-]?[Tt]rimestre\.zip$', zip_href, re.IGNORECASE)
                            if match3:
                                ano_arquivo = match3.group(1)
                                trimestre_num = match3.group(2)
                        
                        # Padrão 4: Qualquer ZIP que contenha número + T
                        if not trimestre_num and zip_href.endswith('.zip'):
                            match4 = re.search(r'(\d)[Tt]', zip_href)
                            if match4:
                                trimestre_num = match4.group(1)
                        
                        if trimestre_num and trimestre_num in ['1', '2', '3', '4']:
                            trimestre = f"{trimestre_num}T"
                            url_completa = ano_url + zip_href
                            trimestres.append((ano_arquivo, trimestre, url_completa))
                            logger.info(f"Encontrado: {trimestre}/{ano_arquivo} -> {zip_href}")
            
            # Ordenar por ano e trimestre (mais recentes primeiro)
            trimestres.sort(key=lambda x: (int(x[0]), int(x[1][0])), reverse=True)
            
            logger.info(f"Total de trimestres encontrados: {len(trimestres)}")
            return trimestres
            
        except Exception as e:
            logger.error(f"Erro ao identificar trimestres: {e}")
            return []
    
    def baixar_arquivo_zip(self, ano: str, trimestre: str, url_arquivo: str) -> Path:
        """
        Baixa um arquivo ZIP específico
        Retorna: Caminho do arquivo baixado
        """
        logger.info(f"Baixando arquivo do trimestre {trimestre}/{ano}...")
        
        try:
            arquivo_nome = f"{trimestre}{ano}.zip"
            arquivo_path = self.dados_dir / arquivo_nome
            
            # Verificar se arquivo já existe
            if arquivo_path.exists():
                logger.info(f"Arquivo já existe: {arquivo_path}")
                return arquivo_path
            
            # Baixar arquivo
            logger.info(f"Baixando de: {url_arquivo}")
            file_response = requests.get(url_arquivo, timeout=120, stream=True)
            file_response.raise_for_status()
            
            # Salvar com barra de progresso básica
            total_size = int(file_response.headers.get('content-length', 0))
            logger.info(f"Tamanho: {total_size / (1024*1024):.1f} MB")
            
            with open(arquivo_path, 'wb') as f:
                downloaded = 0
                for chunk in file_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
            
            logger.info(f"Arquivo salvo: {arquivo_path}")
            return arquivo_path
            
        except Exception as e:
            logger.error(f"Erro ao baixar arquivo do trimestre {trimestre}/{ano}: {e}")
            return None
    
    def detectar_encoding(self, arquivo: Path) -> str:
        """Detecta o encoding de um arquivo"""
        try:
            with open(arquivo, 'rb') as f:
                resultado = chardet.detect(f.read(100000))  # Ler primeiros 100KB
                return resultado['encoding'] or 'utf-8'
        except:
            return 'utf-8'
    
    def carregar_cadastro_operadoras(self) -> Dict:
        """
        Baixa e carrega o cadastro de operadoras ativas
        Retorna: Dicionário {REG_ANS: {'CNPJ': ..., 'RazaoSocial': ...}}
        """
        if self.operadoras_cache is not None:
            return self.operadoras_cache
        
        logger.info("Baixando cadastro de operadoras...")
        try:
            # Listar arquivos disponíveis
            response = requests.get(self.operadoras_url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Procurar arquivo CSV mais recente
            csv_url = None
            for link in soup.find_all('a'):
                href = link.get('href', '')
                if href.endswith('.csv'):
                    csv_url = self.operadoras_url + href
                    break
            
            if not csv_url:
                logger.warning("Arquivo de operadoras não encontrado!")
                return {}
            
            logger.info(f"Baixando: {csv_url}")
            csv_response = requests.get(csv_url, timeout=60)
            csv_response.raise_for_status()
            
            # Salvar temporariamente
            temp_file = self.dados_dir / 'operadoras_temp.csv'
            with open(temp_file, 'wb') as f:
                f.write(csv_response.content)
            
            # Ler CSV
            encoding = self.detectar_encoding(temp_file)
            for sep in [';', ',', '\t']:
                try:
                    df = pd.read_csv(temp_file, sep=sep, encoding=encoding, low_memory=False)
                    if len(df.columns) > 1:
                        break
                except:
                    continue
            
            # Criar dicionário REG_ANS -> dados
            cache = {}
            colunas_lower = {col.lower(): col for col in df.columns}
            
            logger.info(f"Colunas disponíveis no CSV: {list(colunas_lower.keys())}")
            
            reg_col = None
            cnpj_col = None
            razao_col = None
            
            for possivel in ['registro_ans', 'reg_ans', 'cd_operadora', 'registro_operadora']:
                if possivel in colunas_lower:
                    reg_col = colunas_lower[possivel]
                    logger.info(f"REG_ANS encontrado: {possivel} -> {reg_col}")
                    break
            
            for possivel in ['cnpj', 'nu_cnpj', 'cd_cnpj']:
                if possivel in colunas_lower:
                    cnpj_col = colunas_lower[possivel]
                    logger.info(f"CNPJ encontrado: {possivel} -> {cnpj_col}")
                    break
            
            for possivel in ['razao_social', 'nm_razao_social', 'nome_fantasia']:
                if possivel in colunas_lower:
                    razao_col = colunas_lower[possivel]
                    logger.info(f"RazaoSocial encontrado: {possivel} -> {razao_col}")
                    break
            
            if not reg_col or not cnpj_col or not razao_col:
                logger.warning(f"Colunas obrigatórias não encontradas! REG={reg_col}, CNPJ={cnpj_col}, RAZAO={razao_col}")
                self.operadoras_cache = {}
                return {}
            
            logger.info(f"Processando {len(df)} operadoras...")
            logger.info(f"Processando {len(df)} operadoras...")
            for _, row in df.iterrows():
                try:
                    reg_ans = str(row[reg_col]).strip()
                    cnpj = str(row[cnpj_col]).strip()
                    razao = str(row[razao_col]).strip()
                    
                    # Validações básicas
                    if reg_ans and cnpj and razao:
                        cache[reg_ans] = {'CNPJ': cnpj, 'RazaoSocial': razao}
                except Exception as e:
                    continue
            
            logger.info(f"Cadastro carregado: {len(cache)} operadoras")
            self.operadoras_cache = cache
            
            # Remover arquivo temporário
            if temp_file.exists():
                temp_file.unlink()
            
            return cache
            
        except Exception as e:
            logger.error(f"Erro ao carregar cadastro de operadoras: {e}")
            # Limpar arquivo temporário em caso de erro
            if temp_file.exists():
                temp_file.unlink()
            return {}
    
    def extrair_e_processar_zip(self, arquivo_zip: Path, ano: str, trimestre: str) -> List[Dict]:
        """
        Extrai arquivos ZIP e processa apenas arquivos de Despesas com Eventos/Sinistros
        Retorna: Lista de dicionários com dados extraídos
        """
        logger.info(f"Extraindo e processando: {arquivo_zip.name}")
        dados_consolidados = []
        
        try:
            # Criar diretório temporário para extração
            extract_dir = self.dados_dir / f"temp_{arquivo_zip.stem}"
            extract_dir.mkdir(exist_ok=True)
            
            # Extrair ZIP
            with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Processar arquivos extraídos (CSVs, TXTs, XLSXs)
            for arquivo in extract_dir.rglob('*'):
                if arquivo.is_file() and arquivo.suffix.lower() in ['.csv', '.txt', '.xlsx']:
                    logger.info(f"Processando arquivo: {arquivo.name}")
                    dados = self.processar_arquivo_despesas(arquivo, ano, trimestre)
                    if dados:
                        logger.info(f"-> Encontrados {len(dados)} registros")
                        dados_consolidados.extend(dados)
                    else:
                        logger.info(f"-> Nenhum registro de despesas encontrado")
            
            # Limpar diretório temporário
            import shutil
            shutil.rmtree(extract_dir)
            
            return dados_consolidados
            
        except Exception as e:
            logger.error(f"Erro ao extrair/processar {arquivo_zip.name}: {e}")
            return []
    
    def processar_arquivo_despesas(self, arquivo: Path, ano: str, trimestre: str) -> List[Dict]:
        """
        Processa um arquivo de despesas (CSV, TXT ou XLSX)
        Retorna lista de dicionários com os dados normalizados
        """
        dados = []
        extensao = arquivo.suffix.lower()
        
        try:
            if extensao in ['.csv', '.txt']:
                # Detectar encoding
                encoding = self.detectar_encoding(arquivo)
                
                # Tentar diferentes delimitadores
                for delimitador in [';', ',', '\t', '|']:
                    try:
                        df = pd.read_csv(
                            arquivo, 
                            sep=delimitador, 
                            encoding=encoding,
                            low_memory=False,
                            on_bad_lines='skip'
                        )
                        
                        # Verificar se conseguiu ler colunas
                        if len(df.columns) > 1:
                            dados = self.normalizar_dados(df, ano, trimestre)
                            if dados:  # Se encontrou dados válidos
                                break
                    except:
                        continue
                        
            elif extensao == '.xlsx':
                df = pd.read_excel(arquivo)
                dados = self.normalizar_dados(df, ano, trimestre)
        
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {arquivo.name}: {e}")
        
        return dados
    
    def normalizar_dados(self, df: pd.DataFrame, ano: str, trimestre: str) -> List[Dict]:
        """
        Normaliza dados do DataFrame para o formato padrão
        Colunas esperadas: REG_ANS (CNPJ), DESCRICAO, VL_SALDO_FINAL
        Filtra apenas contas relacionadas a despesas com eventos/sinistros
        """
        dados = []
        
        # Identificar colunas no DataFrame
        colunas_df = {col.lower(): col for col in df.columns}
        
        # Tentar encontrar colunas no formato ANS
        reg_ans_col = colunas_df.get('reg_ans')
        descricao_col = colunas_df.get('descricao')
        valor_col = colunas_df.get('vl_saldo_final') or colunas_df.get('vl_saldo_inicial')
        conta_col = colunas_df.get('cd_conta_contabil')
        
        # Se não encontrou as colunas ANS, tentar formato genérico
        if not all([reg_ans_col, descricao_col, valor_col]):
            # Mapear possíveis nomes de colunas para nomes padrão
            mapeamento_colunas = {
                'cnpj': ['cnpj', 'cd_cnpj', 'num_cnpj', 'cnpj_operadora', 'reg_ans'],
                'razao_social': ['razao_social', 'razaosocial', 'nome_operadora', 'operadora', 'nm_razao_social', 'descricao'],
                'valor': ['valor', 'vl_despesa', 'despesa', 'valor_despesa', 'vl_saldo', 'vl_saldo_final', 'vl_saldo_inicial']
            }
            
            cnpj_col = None
            razao_col = None
            valor_col = None
            
            for coluna_padrao, variacoes in mapeamento_colunas.items():
                for variacao in variacoes:
                    if variacao in colunas_df:
                        if coluna_padrao == 'cnpj':
                            cnpj_col = colunas_df[variacao]
                        elif coluna_padrao == 'razao_social':
                            razao_col = colunas_df[variacao]
                        elif coluna_padrao == 'valor':
                            valor_col = colunas_df[variacao]
                        break
            
            if not all([cnpj_col, razao_col, valor_col]):
                logger.warning(f"Colunas necessárias não encontradas. Disponíveis: {list(df.columns)}")
                return dados
            
            # Processar formato genérico
            for _, row in df.iterrows():
                try:
                    dados.append({
                        'CNPJ': str(row[cnpj_col]).strip(),
                        'RazaoSocial': str(row[razao_col]).strip(),
                        'Trimestre': trimestre,
                        'Ano': ano,
                        'ValorDespesas': self.converter_valor(row[valor_col])
                    })
                except Exception as e:
                    continue
        else:
            # Processar formato ANS - Filtrar por contas de despesas médicas
            # Contas 4xxxx = Despesas
            # Foco em: 41xxx (Despesas com Eventos/Sinistros), 42xxx (Despesas Administrativas relacionadas)
            total_linhas = len(df)
            logger.info(f"Detectado formato ANS - Processando {total_linhas} linhas...")
            
            # Buscar nome da operadora (precisa fazer join com cadastro ou usar primeiro registro)
            operadoras_cache = {}
            
            for idx, row in df.iterrows():
                try:
                    # Progress bar a cada 10%
                    if idx > 0 and idx % max(1, total_linhas // 10) == 0:
                        percentual = (idx / total_linhas) * 100
                        logger.info(f"Progresso: {percentual:.0f}% ({idx}/{total_linhas} linhas)")
                    
                    conta = str(row[conta_col]) if conta_col else ""
                    descricao = str(row[descricao_col]).lower()
                    
                    # Filtrar apenas contas de despesas médicas/sinistros
                    # Contas que começam com 41, 42 são despesas
                    # 41xxx = Eventos Conhecidos ou Avisados (sinistros)
                    # 42xxx = Provisões Técnicas
                    if not (conta.startswith('41') or conta.startswith('42')):
                        continue
                    
                    reg_ans = str(row[reg_ans_col]).strip()
                    valor = self.converter_valor(row[valor_col])
                    
                    # Usar REG_ANS como identificador (converter para CNPJ format depois)
                    # Por enquanto, usar descrição da conta como "razão social" temporária
                    dados.append({
                        'CNPJ': reg_ans,  # REG_ANS será convertido para CNPJ
                        'RazaoSocial': f"Operadora {reg_ans}",  # Temporário
                        'Trimestre': trimestre,
                        'Ano': ano,
                        'ValorDespesas': valor
                    })
                except Exception as e:
                    continue
            
            logger.info(f"Progresso: 100% ({total_linhas}/{total_linhas} linhas) - Concluído!")
        
        return dados
    
    def converter_valor(self, valor) -> float:
        """Converte valor para float, tratando diferentes formatos"""
        try:
            if pd.isna(valor):
                return 0.0
            
            # Se já for número
            if isinstance(valor, (int, float)):
                return float(valor)
            
            # Se for string, limpar e converter
            valor_str = str(valor).strip()
            # Remover símbolos de moeda e espaços
            valor_str = re.sub(r'[R$\s]', '', valor_str)
            # Substituir vírgula por ponto
            valor_str = valor_str.replace(',', '.')
            
            return float(valor_str)
        except:
            return 0.0
    
    def validar_cnpj(self, cnpj: str) -> bool:
        """
        Valida CNPJ (formato e dígitos verificadores)
        """
        # Remover caracteres não numéricos
        cnpj = re.sub(r'[^0-9]', '', cnpj)
        
        # Verificar se tem 14 dígitos
        if len(cnpj) != 14:
            return False
        
        # Verificar se todos os dígitos são iguais
        if cnpj == cnpj[0] * 14:
            return False
        
        # Validar primeiro dígito verificador
        soma = 0
        peso = 5
        for i in range(12):
            soma += int(cnpj[i]) * peso
            peso -= 1
            if peso < 2:
                peso = 9
        
        digito1 = 11 - (soma % 11)
        if digito1 > 9:
            digito1 = 0
        
        if int(cnpj[12]) != digito1:
            return False
        
        # Validar segundo dígito verificador
        soma = 0
        peso = 6
        for i in range(13):
            soma += int(cnpj[i]) * peso
            peso -= 1
            if peso < 2:
                peso = 9
        
        digito2 = 11 - (soma % 11)
        if digito2 > 9:
            digito2 = 0
        
        if int(cnpj[13]) != digito2:
            return False
        
        return True
    
    def consolidar_dados(self, todos_dados: List[Dict]) -> pd.DataFrame:
        logger.info(f"Consolidando {len(todos_dados)} registros...")
        
        if not todos_dados:
            logger.warning("Nenhum dado para consolidar!")
            return pd.DataFrame()
        
        df = pd.DataFrame(todos_dados)
        
        # Estatísticas antes do tratamento
        total_registros = len(df)
        logger.info(f"Total de registros: {total_registros}")
        
        # AGRUPAR por CNPJ/REG_ANS + Trimestre + Ano (somar despesas)
        logger.info("Agrupando despesas por operadora...")
        df_agrupado = df.groupby(['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano'], as_index=False).agg({
            'ValorDespesas': 'sum'
        })
        logger.info(f"Após agrupamento: {len(df_agrupado)} registros")
        
        # Converter REG_ANS para CNPJ usando cadastro
        logger.info("Convertendo REG_ANS para CNPJ...")
        operadoras = self.carregar_cadastro_operadoras()
        
        if operadoras:
            def converter_reg_ans(row):
                reg_ans = row['CNPJ']
                if reg_ans in operadoras:
                    return operadoras[reg_ans]['CNPJ'], operadoras[reg_ans]['RazaoSocial']
                return reg_ans, row['RazaoSocial']
            
            df_agrupado[['CNPJ', 'RazaoSocial']] = df_agrupado.apply(
                lambda row: pd.Series(converter_reg_ans(row)), axis=1
            )
        
        df = df_agrupado
        
        # 1. Limpar CNPJs
        df['CNPJ'] = df['CNPJ'].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
        
        # 2. Validar CNPJs
        df['CNPJ_Valido'] = df['CNPJ'].apply(self.validar_cnpj)
        cnpjs_invalidos = (~df['CNPJ_Valido']).sum()
        logger.info(f"CNPJs inválidos: {cnpjs_invalidos}")
        
        # 3. Identificar CNPJs com razões sociais diferentes
        cnpj_razoes = df.groupby('CNPJ')['RazaoSocial'].nunique()
        cnpjs_multiplas_razoes = cnpj_razoes[cnpj_razoes > 1].index
        df['CNPJ_Multiplas_Razoes'] = df['CNPJ'].isin(cnpjs_multiplas_razoes)
        logger.info(f"CNPJs com múltiplas razões sociais: {len(cnpjs_multiplas_razoes)}")
        
        # 4. Identificar valores suspeitos
        df['Valor_Zerado'] = df['ValorDespesas'] == 0
        df['Valor_Negativo'] = df['ValorDespesas'] < 0
        valores_zerados = df['Valor_Zerado'].sum()
        valores_negativos = df['Valor_Negativo'].sum()
        logger.info(f"Valores zerados: {valores_zerados}")
        logger.info(f"Valores negativos: {valores_negativos}")
        
        # 5. Normalizar formato de trimestre
        def normalizar_trimestre(trim):
            trim_str = str(trim).upper()
            # Extrair número do trimestre
            numero = re.search(r'[1-4]', trim_str)
            if numero:
                return f"{numero.group()}T"
            return trim_str
        
        df['Trimestre'] = df['Trimestre'].apply(normalizar_trimestre)
        
        # 6. Limpar razão social
        df['RazaoSocial'] = df['RazaoSocial'].str.strip()
        df['RazaoSocial_Vazia'] = df['RazaoSocial'].str.len() == 0
        razoes_vazias = df['RazaoSocial_Vazia'].sum()
        logger.info(f"Razões sociais vazias: {razoes_vazias}")
        
        # Criar CSV consolidado (sem as colunas de flag)
        df_final = df[['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']].copy()
        
        # Relatório de inconsistências é OPCIONAL - não salvar para manter só o ZIP
        # O teste pede apenas o consolidado_despesas.zip
        
        return df_final
    
    def executar(self):
        """Executa o fluxo completo de integração"""
        logger.info("=== Iniciando Integração com Dados da ANS ===")
        
        # 1. Identificar trimestres disponíveis
        trimestres = self.identificar_trimestres_disponiveis()
        
        if len(trimestres) < 3:
            logger.error(f"Apenas {len(trimestres)} trimestres disponíveis! Necessário pelo menos 3.")
            return
        
        # 2. Selecionar últimos 3 trimestres
        ultimos_3_trimestres = trimestres[:3]
        logger.info(f"Processando últimos 3 trimestres:")
        for ano, trim, url in ultimos_3_trimestres:
            logger.info(f"{trim}/{ano}")
        
        # 3. Baixar e processar cada trimestre
        todos_dados = []
        
        for ano, trimestre, url_arquivo in ultimos_3_trimestres:
            # Baixar arquivo
            arquivo_zip = self.baixar_arquivo_zip(ano, trimestre, url_arquivo)
            
            if arquivo_zip and arquivo_zip.exists():
                # Processar arquivo ZIP
                dados = self.extrair_e_processar_zip(arquivo_zip, ano, trimestre)
                todos_dados.extend(dados)
                logger.info(f"Dados extraídos do {trimestre}/{ano}: {len(dados)} registros")
        
        # 4. Consolidar dados
        logger.info(f"\nConsolidando total de {len(todos_dados)} registros...")
        df_consolidado = self.consolidar_dados(todos_dados)
        
        if df_consolidado.empty:
            logger.error("Nenhum dado foi consolidado!")
            return
        
        # 5. Salvar CSV consolidado
        csv_path = self.resultados_dir / 'consolidado_despesas.csv'
        df_consolidado.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"CSV criado: {csv_path}")
        logger.info(f"Total de linhas no consolidado: {len(df_consolidado)}")
        
        # 6. Compactar CSV - ENTREGA FINAL
        zip_path = self.resultados_dir / 'consolidado_despesas.zip'
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path, csv_path.name)
        
        logger.info(f"ZIP criado: {zip_path}")
        logger.info(f"\n Arquivos gerados:")
        logger.info(f"- {csv_path.name} ({len(df_consolidado)} registros)")
        logger.info(f"- {zip_path.name} (arquivo compactado)")
        
        logger.info("\n=== Integração concluída com sucesso! ===")



def main():
    integrador = ANSDataIntegrator()
    integrador.executar()


if __name__ == "__main__":
    main()
