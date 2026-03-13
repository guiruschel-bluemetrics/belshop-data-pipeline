import sys
import boto3
import re
import logging
from urllib.parse import urlparse
from typing import Set

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.job import Job
from pyspark.sql.functions import expr

# --- Configuração Inicial de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# --- Obter Parâmetros do Job ---
# O Step Functions envia esses parâmetros via --Arguments
REQUIRED_ARGS = ['JOB_NAME', 'SQL_FILE_NAME', 'TARGET_TABLE'] #comentar se quiser rodar com parâmetros de forma manual
args = getResolvedOptions(sys.argv, REQUIRED_ARGS + ['WRITE_MODE']) #comentar se quiser rodar com parâmetros de forma manual

# FORÇANDO PARÂMETROS PARA RODAR MANUALMENTE (HARDCODE)
# args = {
#     'JOB_NAME': 'gold-processing',
#     'SQL_FILE_NAME': 'classe_produto.sql', 
#     'TARGET_TABLE': 'classe_produto'
# }

# --- Variáveis Globais e Configurações de Caminho ---
CATALOG_NAME = "glue_catalog"
GLUE_DB_SOURCE_BRONZE = "belshop_bronze_db"
GLUE_DB_SOURCE_SILVER = "belshop_silver_db"
GLUE_DB_SOURCE_GOLD = "belshop_gold_db"
GLUE_DB_TARGET = "belshop_gold_db"

SQL_BASE_PATH = "s3://gold-belshop/sql/"
WAREHOUSE = "s3://gold-belshop/belshop_gold_db/"

# Ordem de prioridade para busca de tabelas
DATABASES_TO_SEARCH = [GLUE_DB_SOURCE_GOLD, GLUE_DB_SOURCE_SILVER, GLUE_DB_SOURCE_BRONZE]

sql_file_name = args['SQL_FILE_NAME']
target_table = args['TARGET_TABLE']
write_mode = args.get('WRITE_MODE', 'OVERWRITE').upper()

# --------------------------------------------------------------------
# Função para extrair nomes de tabelas do SQL
# --------------------------------------------------------------------
def extract_table_names(sql_query: str) -> Set[str]:
    # 1. Limpeza inicial (remover comentários)
    sql_clean = re.sub(r'--.*', '', sql_query)
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
    
    # 2. Identificar nomes de CTEs (blocos dentro do WITH)
    # Procura por: nome_da_cte AS (
    cte_pattern = r"(?i)([a-zA-Z0-9_]+)\s+AS\s*\("
    ctes = set(match.lower() for match in re.findall(cte_pattern, sql_clean))
    
    # 3. Identificar todas as tabelas após FROM e JOIN
    patterns = [
        r"(?i)\bFROM\s+([^\s\(\);,]+)", 
        r"(?i)\bJOIN\s+([^\s\(\);,]+)"
    ]

    tables = set()
    for pattern in patterns:
        matches = re.findall(pattern, sql_clean)
        for match in matches:
            clean = match.strip().replace('[', '').replace(']', '').replace('`', '').replace('"', '')
            if '.' in clean:
                clean = clean.split('.')[-1]
            
            if clean:
                tables.add(clean.lower())

    # 4. REMOVER as CTEs da lista de tabelas que precisam ser buscadas no Glue
    source_tables = tables - ctes
    
    return source_tables

# --------------------------------------------------------------------
# Inicialização do Spark + Iceberg
# --------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName(args['JOB_NAME'])
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    #.config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.glue.S3FileIO")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE.rstrip("/"))
    .config("spark.sql.defaultCatalog", CATALOG_NAME)
    # Políticas de tempo legado para compatibilidade com formatos antigos se necessário
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") 
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --------------------------------------------------------------------
# 1. Ler arquivo SQL do S3
# --------------------------------------------------------------------
if not SQL_BASE_PATH.endswith("/"):
    SQL_BASE_PATH += "/"

sql_s3_path = SQL_BASE_PATH + sql_file_name
logger.info(f"Lendo query SQL do S3: {sql_s3_path}")

try:
    parsed = urlparse(sql_s3_path)
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip('/'))
    sql_query = obj['Body'].read().decode('utf-8').strip()
except Exception as e:
    logger.error(f"Erro ao ler arquivo SQL: {e}")
    sys.exit(1)

# --------------------------------------------------------------------
# 2. Localizar e Registrar Tabelas de Origem (Discovery)
# --------------------------------------------------------------------
logger.info("Iniciando descoberta de tabelas de origem nas camadas Silver/Bronze...")
source_tables = extract_table_names(sql_query)
logger.info(f"Tabelas identificadas no SQL: {source_tables}")

for table_name in source_tables:
    table_found = False
    
    for db in DATABASES_TO_SEARCH:
        # Nome completo no catálogo para verificação
        full_catalog_path = f"{CATALOG_NAME}.{db}.{table_name}"
        
        if spark.catalog.tableExists(full_catalog_path):
            logger.info(f"Tabela '{table_name}' encontrada na camada: {db}")
            try:
                df_source = spark.table(full_catalog_path)
                # Registra a view com o nome exato esperado pelo SQL
                df_source.createOrReplaceTempView(table_name)
                logger.info(f"View temporária '{table_name}' registrada com sucesso.")
                table_found = True
                break 
            except Exception as e:
                logger.error(f"Erro ao carregar tabela {full_catalog_path}: {e}")
                sys.exit(1)
                
    if not table_found:
        logger.error(f"ERRO CRÍTICO: Tabela '{table_name}' não localizada em {DATABASES_TO_SEARCH}")
        sys.exit(1)

# --------------------------------------------------------------------
# 3. Executar Transformação SQL
# --------------------------------------------------------------------
logger.info("Executando query de transformação...")
try:
    df_result = spark.sql(sql_query)
except Exception as e:
    logger.error(f"Erro na execução do SQL: {e}")
    sys.exit(1)

# --------------------------------------------------------------------
# 4. Escrita na Camada Gold (Iceberg)
# --------------------------------------------------------------------
if "." not in target_table:
    target_table = f"{GLUE_DB_TARGET}.{target_table}"

full_target_name = f"{CATALOG_NAME}.{target_table}"
logger.info(f"Destino da escrita: {full_target_name} | Modo: {write_mode}")

# Verifica se a tabela destino já existe
if not spark.catalog.tableExists(full_target_name):
    logger.info("Tabela destino não existe. Criando nova tabela Iceberg...")
    df_result.writeTo(full_target_name).create()
else:
    writer = df_result.writeTo(full_target_name)
    if write_mode == "OVERWRITE":
        logger.info("Executando overwrite (1=1)...")
        writer.overwrite(expr("1=1"))
    elif write_mode == "APPEND":
        logger.info("Executando append de dados...")
        writer.append()
    else:
        logger.error(f"Modo de escrita '{write_mode}' não suportado.")
        sys.exit(1)

logger.info("Processamento finalizado com sucesso.")
job.commit()