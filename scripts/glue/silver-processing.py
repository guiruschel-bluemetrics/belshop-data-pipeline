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

# --- Configuração Inicial ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# --------------------------------------------------------------------
# Parâmetros esperados:
# JOB_NAME
# SQL_FILE_NAME (ex: clientes_linx_principal.sql)
# TARGET_TABLE (ex: clientes_linx_principal)
# WRITE_MODE (APPEND / OVERWRITE)
# --------------------------------------------------------------------

REQUIRED_ARGS = ['JOB_NAME', 'SQL_FILE_NAME', 'TARGET_TABLE']
args = getResolvedOptions(sys.argv, REQUIRED_ARGS + ['WRITE_MODE']) #comentar se quiser rodar com parâmetros de forma manual

#FORÇANDO PARÂMETROS PARA RODAR MANUALMENTE (HARDCODE)
# args = {
#     'JOB_NAME': 'silver-processing',
#     'SQL_FILE_NAME': 'vendas_saldos.sql',
#     'TARGET_TABLE': 'vendas_saldos'
# }

# --- Variáveis Globais ---
CATALOG_NAME = "glue_catalog"
GLUE_DB_SOURCE = "belshop_bronze_db"
GLUE_DB_TARGET = "belshop_silver_db"
SQL_BASE_PATH = "s3://silver-belshop/sql/"
WAREHOUSE = "s3://silver-belshop/belshop_silver_db.db/"

sql_file_name = args['SQL_FILE_NAME']
target_table = args['TARGET_TABLE']
write_mode = args.get('WRITE_MODE', 'OVERWRITE').upper()


# --------------------------------------------------------------------
# Função para extrair nomes de tabelas do SQL
# --------------------------------------------------------------------
def extract_table_names(sql_query: str) -> Set[str]:
    query_upper = sql_query.upper()
    patterns = [r"FROM\s+([\w\.\"]+)", r"JOIN\s+([\w\.\"]+)"]

    tables = set()
    for pattern in patterns:
        matches = re.findall(pattern, query_upper, re.DOTALL)
        for match in matches:
            clean = match.strip().replace('"', '')
            if '.' in clean:
                clean = clean.split('.')[-1]
            tables.add(clean.lower())

    return tables


# --------------------------------------------------------------------
# Inicialização do Spark + Iceberg
# --------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName(args['JOB_NAME'])
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE.rstrip("/"))
    .config("spark.sql.defaultCatalog", CATALOG_NAME)
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") 
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
    .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
    .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# --------------------------------------------------------------------
# 1. Ler SQL do S3
# --------------------------------------------------------------------
if not SQL_BASE_PATH.endswith("/"):
    SQL_BASE_PATH += "/"

sql_s3_path = SQL_BASE_PATH + sql_file_name
logger.info(f"Lendo query SQL do caminho: {sql_s3_path}")

try:
    parsed = urlparse(sql_s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    sql_query = obj['Body'].read().decode('utf-8').strip()

except Exception as e:
    logger.error(f"ERRO CRÍTICO ao ler {sql_s3_path}: {e}")
    sys.exit(1)

if not sql_query:
    raise ValueError(f"O arquivo SQL {sql_file_name} está vazio.")


# --------------------------------------------------------------------
# 2. Ler tabelas Bronze referenciadas no SQL
# --------------------------------------------------------------------
logger.info("Analisando SQL para extrair nomes de tabelas de origem...")
source_tables = extract_table_names(sql_query)
logger.info(f"Tabelas detectadas no SQL: {source_tables}")

for table_name in source_tables:
    full_name = f"{CATALOG_NAME}.{GLUE_DB_SOURCE}.{table_name}"

    logger.info(f"Lendo e registrando tabela de origem: {full_name}")

    try:
        df_source = spark.table(full_name)
        df_source.createOrReplaceTempView(table_name)
        logger.info(f"View temporária criada: {table_name}")

    except Exception as e:
        logger.error(f"ERRO ao ler tabela Bronze {full_name}: {e}")
        sys.exit(1)


# --------------------------------------------------------------------
# 3. Executar SQL
# --------------------------------------------------------------------
logger.info("Executando SQL...")
try:
    df = spark.sql(sql_query)
except Exception as e:
    logger.error(f"ERRO ao executar SQL: {e}")
    sys.exit(1)


# --------------------------------------------------------------------
# 4. Escrita no Iceberg (Silver)
# --------------------------------------------------------------------
if "." not in target_table:
    target_table = f"{GLUE_DB_TARGET}.{target_table}"

full_table_name = f"{CATALOG_NAME}.{target_table}"
logger.info(f"Escrevendo no Iceberg: {full_table_name}")

try:
    writer = df.writeTo(full_table_name)

    if write_mode == "OVERWRITE":
        logger.info("Modo de escrita: OVERWRITE (overwrite entire table)")
        writer.overwrite(expr("1=1"))

    elif write_mode == "APPEND":
        logger.info("Modo de escrita: APPEND")
        writer.append()

    else:
        raise ValueError(f"WRITE_MODE inválido: {write_mode}")

except Exception as e:

    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        logger.warning(f"Tabela {full_table_name} não existe. Criando...")

        df.writeTo(full_table_name).create()
        logger.info("Tabela criada com sucesso. Reexecutando escrita...")

        writer = df.writeTo(full_table_name)

        if write_mode == "OVERWRITE":
            writer.overwrite(expr("1=1"))
        elif write_mode == "APPEND":
            writer.append()

    else:
        logger.error(f"ERRO CRÍTICO ao escrever no Iceberg: {e}")
        sys.exit(1)

logger.info(f"Tabela {full_table_name} carregada com sucesso.")

job.commit()
# --------------------------------------------------------------------
# 3. Executar SQL
# --------------------------------------------------------------------
logger.info(f"Executando SQL extraído de {sql_file_name}...")

# Sem o try/except, o erro estoura direto no log do Glue com o stacktrace completo
df = spark.sql(sql_query)

# --------------------------------------------------------------------
# 4. Escrita no Iceberg (Silver)
# --------------------------------------------------------------------
if "." not in target_table:
    target_table = f"{GLUE_DB_TARGET}.{target_table}"

full_table_name = f"{CATALOG_NAME}.{target_table}"
logger.info(f"Iniciando escrita no Iceberg: {full_table_name}")
logger.info(f"Modo de escrita: {write_mode}")

writer = df.writeTo(full_table_name)

# Verificação simples de existência para decidir entre create ou append/overwrite
# Isso substitui o try/except que verificava TABLE_OR_VIEW_NOT_FOUND
table_exists = spark.catalog.tableExists(full_table_name)

if not table_exists:
    logger.info(f"Tabela {full_table_name} não existe. Criando nova tabela...")
    df.writeTo(full_table_name).create()
else:
    if write_mode == "OVERWRITE":
        logger.info("Executando overwrite total (1=1)...")
        writer.overwrite(expr("1=1"))
    elif write_mode == "APPEND":
        logger.info("Executando append...")
        writer.append()
    else:
        raise ValueError(f"WRITE_MODE inválido: {write_mode}")

logger.info(f"Tabela {full_table_name} processada com sucesso.")

job.commit()