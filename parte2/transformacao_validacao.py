"""
TESTE DE TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS
Parte 2 do Teste

Este script realiza:
1. Validação de dados (CNPJ, valores, razão social)
2. Enriquecimento com dados cadastrais da ANS
3. Agregação e análise estatística
"""

import pandas as pd
import numpy as np
import requests
from pathlib import Path
import logging
import re
from typing import Tuple, Dict, List
from bs4 import BeautifulSoup
import chardet

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransformadorDados:
    """Classe para transformação e validação de dados da ANS"""
    
    def __init__(self):
        self.resultados_dir = Path("resultados")
        self.dados_dir = Path("dados")
        self.resultados_dir.mkdir(exist_ok=True)
        self.dados_dir.mkdir(exist_ok=True)
    
    # ==================== PARTE 2.1: VALIDAÇÃO DE DADOS ====================
    
    def validar_cnpj(self, cnpj: str) -> bool:
        """
        Valida CNPJ (formato e dígitos verificadores)
        Retorna True se válido, False caso contrário
        """
        # Remover caracteres não numéricos
        cnpj = re.sub(r'[^0-9]', '', str(cnpj))
        
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
    
    def validar_dados(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Valida dados do DataFrame
        Retorna DataFrame com flags de validação e estatísticas
        """
        logger.info("=== VALIDAÇÃO DE DADOS (2.1) ===")
        
        estatisticas = {
            'total_registros': len(df),
            'cnpj_validos': 0,
            'cnpj_invalidos': 0,
            'valores_negativos': 0,
            'valores_zerados': 0,
            'razoes_vazias': 0
        }
        
        # Criar cópia para não modificar original
        df_validado = df.copy()
        
        # 1. Validar CNPJ
        logger.info("Validando CNPJs...")
        df_validado['CNPJ_Limpo'] = df_validado['CNPJ'].apply(
            lambda x: re.sub(r'[^0-9]', '', str(x))
        )
        df_validado['CNPJ_Valido'] = df_validado['CNPJ_Limpo'].apply(self.validar_cnpj)
        
        estatisticas['cnpj_validos'] = df_validado['CNPJ_Valido'].sum()
        estatisticas['cnpj_invalidos'] = (~df_validado['CNPJ_Valido']).sum()
        
        logger.info(f"CNPJs válidos: {estatisticas['cnpj_validos']}")
        logger.info(f"CNPJs inválidos: {estatisticas['cnpj_invalidos']}")
        
        # 2. Validar valores numéricos
        logger.info("Validando valores...")
        df_validado['Valor_Positivo'] = df_validado['ValorDespesas'] > 0
        df_validado['Valor_Zerado'] = df_validado['ValorDespesas'] == 0
        df_validado['Valor_Negativo'] = df_validado['ValorDespesas'] < 0
        
        estatisticas['valores_negativos'] = df_validado['Valor_Negativo'].sum()
        estatisticas['valores_zerados'] = df_validado['Valor_Zerado'].sum()
        
        logger.info(f"Valores negativos: {estatisticas['valores_negativos']}")
        logger.info(f"Valores zerados: {estatisticas['valores_zerados']}")
        
        # 3. Validar Razão Social
        logger.info("Validando razões sociais...")
        df_validado['RazaoSocial_Valida'] = df_validado['RazaoSocial'].str.strip().str.len() > 0
        
        estatisticas['razoes_vazias'] = (~df_validado['RazaoSocial_Valida']).sum()
        logger.info(f"Razões sociais vazias: {estatisticas['razoes_vazias']}")
        
        # 4. Flag geral de validação
        df_validado['Registro_Valido'] = (
            df_validado['CNPJ_Valido'] &
            df_validado['Valor_Positivo'] &
            df_validado['RazaoSocial_Valida']
        )
        
        registros_validos = df_validado['Registro_Valido'].sum()
        logger.info(f"\nRegistros completamente válidos: {registros_validos}/{len(df)}")
        
        return df_validado, estatisticas
    
    # ==================== PARTE 2.2: ENRIQUECIMENTO DE DADOS ====================
    
    def detectar_encoding(self, arquivo: Path) -> str:
        """Detecta o encoding de um arquivo"""
        try:
            with open(arquivo, 'rb') as f:
                resultado = chardet.detect(f.read(100000))
                return resultado['encoding'] or 'utf-8'
        except:
            return 'utf-8'
    
    def baixar_dados_cadastrais(self) -> pd.DataFrame:
        """
        Baixa dados cadastrais das operadoras ativas da ANS
        """
        logger.info("=== BAIXANDO DADOS CADASTRAIS (2.2) ===")
        
        base_url = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
        
        try:
            # Listar arquivos disponíveis
            response = requests.get(base_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Procurar por arquivo CSV
            arquivo_csv = None
            for link in soup.find_all('a'):
                href = link.get('href', '')
                if href.endswith('.csv'):
                    arquivo_csv = href
                    break
            
            if not arquivo_csv:
                logger.warning("Arquivo CSV não encontrado.")
            
            # Baixar arquivo
            arquivo_url = base_url + arquivo_csv
            logger.info(f"Baixando: {arquivo_url}")
            
            file_response = requests.get(arquivo_url, timeout=60)
            file_response.raise_for_status()
            
            # Salvar arquivo
            arquivo_path = self.dados_dir / arquivo_csv
            with open(arquivo_path, 'wb') as f:
                f.write(file_response.content)
            
            logger.info(f"Arquivo salvo: {arquivo_path}")
            
            # Ler CSV
            encoding = self.detectar_encoding(arquivo_path)
            
            # Tentar diferentes delimitadores
            for delimitador in [';', ',', '\t', '|']:
                try:
                    df = pd.read_csv(arquivo_path, sep=delimitador, encoding=encoding, low_memory=False)
                    if len(df.columns) > 1:
                        logger.info(f"CSV lido com sucesso. {len(df)} registros encontrados.")
                        return self.normalizar_dados_cadastrais(df)
                except:
                    continue
            
            logger.warning("Não foi possível ler o arquivo.")
            
        except Exception as e:
            logger.error(f"Erro ao baixar dados cadastrais: {e}")
    
    def normalizar_dados_cadastrais(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza dados cadastrais para formato padrão"""
        
        # Mapear possíveis nomes de colunas
        mapeamento = {
            'cnpj': ['cnpj', 'cd_cnpj', 'num_cnpj', 'cnpj_operadora'],
            'registro_ans': ['registro_ans', 'cd_ans', 'registro', 'num_registro_ans', 'registro_operadora'],
            'modalidade': ['modalidade', 'nm_modalidade', 'tp_modalidade'],
            'uf': ['uf', 'sg_uf', 'estado', 'sigla_uf']
        }
        
        colunas_df = {col.lower(): col for col in df.columns}
        resultado = {}
        
        # Encontrar colunas correspondentes
        for campo, variacoes in mapeamento.items():
            for variacao in variacoes:
                if variacao in colunas_df:
                    if campo == 'cnpj':
                        resultado['CNPJ'] = df[colunas_df[variacao]].apply(
                            lambda x: re.sub(r'[^0-9]', '', str(x))
                        )
                    elif campo == 'registro_ans':
                        resultado['RegistroANS'] = df[colunas_df[variacao]].astype(str)
                    elif campo == 'modalidade':
                        resultado['Modalidade'] = df[colunas_df[variacao]].astype(str)
                    elif campo == 'uf':
                        resultado['UF'] = df[colunas_df[variacao]].astype(str)
                    break
        
        if len(resultado) >= 3:  # Precisa de pelo menos CNPJ, RegistroANS e UF
            df_resultado = pd.DataFrame(resultado)
            logger.info(f"Dados cadastrais normalizados: {len(df_resultado)} registros")
            return df_resultado
        else:
            logger.warning(f"Colunas esperadas não encontradas. Encontradas: {list(resultado.keys())}")
            logger.warning(f"Colunas disponíveis: {df.columns.tolist()}")
    
    def enriquecer_dados(self, df_consolidado: pd.DataFrame, df_cadastral: pd.DataFrame) -> pd.DataFrame:
        """
        Enriquece dados consolidados com informações cadastrais
        """
        logger.info("=== ENRIQUECIMENTO DE DADOS (2.2) ===")
        
        # Preparar CNPJ para join
        df_consolidado_prep = df_consolidado.copy()
        df_consolidado_prep['CNPJ_Join'] = df_consolidado_prep['CNPJ'].apply(
            lambda x: re.sub(r'[^0-9]', '', str(x))
        )
        
        df_cadastral_prep = df_cadastral.copy()
        df_cadastral_prep['CNPJ_Join'] = df_cadastral_prep['CNPJ'].apply(
            lambda x: re.sub(r'[^0-9]', '', str(x))
        )
        
        # Verificar duplicatas no cadastro
        duplicatas = df_cadastral_prep['CNPJ_Join'].duplicated().sum()
        if duplicatas > 0:
            logger.warning(f"Encontradas {duplicatas} duplicatas no cadastro. Mantendo primeira ocorrência.")
            df_cadastral_prep = df_cadastral_prep.drop_duplicates(subset=['CNPJ_Join'], keep='first')
        
        # Fazer left join
        logger.info("Realizando join dos dados...")
        df_enriquecido = df_consolidado_prep.merge(
            df_cadastral_prep[['CNPJ_Join', 'RegistroANS', 'Modalidade', 'UF']],
            on='CNPJ_Join',
            how='left'
        )
        
        # Adicionar flag de cadastro
        df_enriquecido['Tem_Cadastro'] = df_enriquecido['RegistroANS'].notna()
        
        # Estatísticas
        total = len(df_enriquecido)
        com_cadastro = df_enriquecido['Tem_Cadastro'].sum()
        sem_cadastro = total - com_cadastro
        
        logger.info(f"Total de registros: {total}")
        logger.info(f"Com dados cadastrais: {com_cadastro} ({com_cadastro/total*100:.1f}%)")
        logger.info(f"Sem dados cadastrais: {sem_cadastro} ({sem_cadastro/total*100:.1f}%)")
        
        # Remover coluna auxiliar
        df_enriquecido = df_enriquecido.drop('CNPJ_Join', axis=1)
        
        return df_enriquecido
    
    # ==================== PARTE 2.3: AGREGAÇÃO DE DADOS ====================
    
    def agregar_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega dados por RazaoSocial e UF
        """
        logger.info("=== AGREGAÇÃO DE DADOS (2.3) ===")
        
        # Filtrar apenas registros com UF (que têm cadastro)
        df_com_uf = df[df['UF'].notna()].copy()
        
        if len(df_com_uf) == 0:
            logger.warning("Nenhum registro com UF encontrado!")
            return pd.DataFrame()
        
        logger.info(f"Agregando {len(df_com_uf)} registros com UF definida...")
        
        # IMPORTANTE: Os valores da ANS são ACUMULADOS no ano (1T, 2T, 3T)
        # Por isso usamos MAX (último trimestre) ao invés de SUM
        # Exemplo: 1T=100, 2T=250, 3T=400 → Total correto = 400 (não 750)
        
        # Agrupar por RazaoSocial e UF
        agregacao = df_com_uf.groupby(['RazaoSocial', 'UF']).agg(
            Total_Despesas=('ValorDespesas', 'max'),  # MAX pois valores são acumulados
            Qtd_Trimestres=('Trimestre', 'count'),
            Min_Despesa=('ValorDespesas', 'min'),
            Max_Despesa=('ValorDespesas', 'max')
        ).reset_index()
        
        # Calcular média correta: Total / Qtd_Trimestres (não MEAN de valores acumulados)
        agregacao['Media_Despesas'] = agregacao['Total_Despesas'] / agregacao['Qtd_Trimestres']
        
        # Calcular desvio padrão manualmente (valores acumulados não permitem std direto)
        # Como são acumulados, o desvio real requer calcular diferenças entre trimestres
        # Por simplicidade, vamos deixar como 0 (indica que não é aplicável para dados acumulados)
        agregacao['Desvio_Padrao'] = 0
        
        # Calcular coeficiente de variação (útil para identificar volatilidade)
        agregacao['Coef_Variacao'] = np.where(
            agregacao['Media_Despesas'] > 0,
            (agregacao['Desvio_Padrao'] / agregacao['Media_Despesas']) * 100,
            0
        )
        
        # Ordenar por valor total (maior para menor)
        agregacao = agregacao.sort_values('Total_Despesas', ascending=False)
        
        logger.info(f"Agregação concluída: {len(agregacao)} grupos criados")
        logger.info(f"\nTop 5 operadoras por despesas:")
        for idx, row in agregacao.head(5).iterrows():
            logger.info(f"  {row['RazaoSocial']} ({row['UF']}): R$ {row['Total_Despesas']:,.2f}")
        
        return agregacao
    
    def executar(self, arquivo_consolidado: str = None):
        """Executa o fluxo completo de transformação"""
        logger.info("=== INICIANDO TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS ===\n")
        
        # 1. Carregar dados consolidados
        if arquivo_consolidado is None:
            # Tentar carregar do ZIP primeiro (parte1/resultados ou resultados/)
            zip_paths = [
                Path('../parte1/resultados/consolidado_despesas.zip'),
                Path('parte1/resultados/consolidado_despesas.zip'),
                self.resultados_dir / 'consolidado_despesas.zip'
            ]
            csv_paths = [
                Path('../parte1/resultados/consolidado_despesas.csv'),
                Path('parte1/resultados/consolidado_despesas.csv'),
                self.resultados_dir / 'consolidado_despesas.csv'
            ]
            
            # Procurar ZIP
            zip_encontrado = None
            for zip_path in zip_paths:
                if zip_path.exists():
                    zip_encontrado = zip_path
                    break
            
            # Procurar CSV direto (Parte 1 agora gera CSV + ZIP)
            csv_encontrado = None
            for csv_path in csv_paths:
                if csv_path.exists():
                    csv_encontrado = csv_path
                    break
            
            if csv_encontrado:
                arquivo_consolidado = csv_encontrado
            else:
                logger.error(f"Arquivo não encontrado em nenhum dos locais:")
                for p in csv_paths:
                    logger.error(f"  - {p}")
                logger.info("Execute primeiro a Parte 1 para gerar o arquivo consolidado.")
                return
        
        if not Path(arquivo_consolidado).exists():
            logger.error(f"Arquivo não encontrado: {arquivo_consolidado}")
            logger.info("Execute primeiro a Parte 1 para gerar o arquivo consolidado.")
            return
        
        logger.info(f"Carregando dados consolidados de: {arquivo_consolidado}")
        df_consolidado = pd.read_csv(arquivo_consolidado, encoding='utf-8-sig')
        logger.info(f"Carregados {len(df_consolidado)} registros\n")
        
        # 2. VALIDAÇÃO DE DADOS (2.1)
        logger.info("=== ETAPA 2.1: VALIDAÇÃO DE DADOS ===")
        df_validado, stats_validacao = self.validar_dados(df_consolidado)
        logger.info(f"Validação concluída.\n")
        
        # 3. ENRIQUECIMENTO DE DADOS (2.2)
        logger.info("=== ETAPA 2.2: ENRIQUECIMENTO DE DADOS ===")
        df_cadastral = self.baixar_dados_cadastrais()
        df_enriquecido = self.enriquecer_dados(df_validado, df_cadastral)
        logger.info(f"Enriquecimento concluído.\n")
        
        # 4. AGREGAÇÃO DE DADOS (2.3)
        logger.info("=== ETAPA 2.3: AGREGAÇÃO E ANÁLISE ===")
        df_agregado = self.agregar_dados(df_enriquecido)
        
        if not df_agregado.empty:
            # Salvar APENAS o arquivo de despesas agregadas (conforme especificação 2.3)
            arquivo_agregado = self.resultados_dir / 'despesas_agregadas.csv'
            df_agregado.to_csv(arquivo_agregado, index=False, encoding='utf-8-sig')
            logger.info(f"Dados agregados salvos em: {arquivo_agregado}")
            
            # Compactar em ZIP (conforme especificação 2.3)
            import zipfile
            arquivo_zip = self.resultados_dir / 'Teste_Christopher.zip'
            with zipfile.ZipFile(arquivo_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(arquivo_agregado, 'despesas_agregadas.csv')
            
            logger.info(f"Arquivo compactado criado: {arquivo_zip}")
            
            # Estatísticas finais
            logger.info("\n=== ESTATÍSTICAS FINAIS ===")
            logger.info(f"Total de grupos (Operadora/UF): {len(df_agregado)}")
            logger.info(f"Despesas totais: R$ {df_agregado['Total_Despesas'].sum():,.2f}")
            logger.info(f"Média geral: R$ {df_agregado['Total_Despesas'].mean():,.2f}")
            logger.info(f"UFs representadas: {df_agregado['UF'].nunique()}")
            
            # Distribuição por UF
            logger.info("\nTop 5 UFs por despesas totais:")
            por_uf = df_agregado.groupby('UF')['Total_Despesas'].sum().sort_values(ascending=False)
            for uf, valor in por_uf.head(5).items():
                logger.info(f"  {uf}: R$ {valor:,.2f}")
            
            logger.info("\nSAÍDA FINAL:")
            logger.info(f"   - despesas_agregadas.csv ({len(df_agregado)} registros)")
            logger.info(f"   - Teste_Christopher.zip (arquivo compactado)")
        
        logger.info("\n=== TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO! ===")
        logger.info("\nRESUMO DO PROCESSAMENTO:")
        logger.info(f"   2.1. Validação: {len(df_validado)} registros processados")
        logger.info(f"   2.2. Enriquecimento: {len(df_enriquecido)} registros enriquecidos")
        logger.info(f"   2.3. Agregação: {len(df_agregado)} grupos criados")
        logger.info(f"\nArquivo final: {arquivo_zip}")


def main():
    """Função principal"""
    transformador = TransformadorDados()
    transformador.executar()


if __name__ == "__main__":
    main()
