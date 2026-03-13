# Script para Glue: Gera a tabela auxiliar de vendas da Linx para os anos de 2020 e 2021, unificando dados de vendas, itens e trocas.
# DEVE RODAR APENAS 1 VEZ, TABELA COM DADOS ANTES DE 2021

import sys
import boto3
import logging
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# --------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

# --------------------------------------------------------------------
# Parâmetros
# --------------------------------------------------------------------
REQUIRED_ARGS = ["JOB_NAME"]
args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

# --------------------------------------------------------------------
# Configurações globais
# --------------------------------------------------------------------
CATALOG_NAME = "glue_catalog"
GLUE_DB_SOURCE = "belshop_bronze_db"
GLUE_DB_TARGET = "belshop_silver_db"
TARGET_TABLE = "aux_vendas_linx_2020_2021"

SQL_BASE_PATH = "s3://silver-belshop/sql/"
WAREHOUSE = "s3://silver-belshop/belshop_silver_db.db/"

# SQLs base (Nomes das views serão baseados nestes arquivos)
SQL_FILES = [
    "vendas_2019.sql",
    "vendas_2020.sql",
    "vendas_itens_2019.sql",
    "vendas_itens_2020_1_6.sql",
    "vendas_itens_2020_6_12.sql",
    "vendas_trocas_2019.sql",
    "vendas_trocas_2020.sql"
]

# --------------------------------------------------------------------
# Spark + Iceberg Configuration
# --------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName(args["JOB_NAME"])
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE.rstrip("/"))
    .config("spark.sql.defaultCatalog", CATALOG_NAME)
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    .getOrCreate()
)

# Define o contexto inicial para o banco Bronze
spark.sql(f"USE {GLUE_DB_SOURCE}")

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3")

def read_sql_from_s3(s3_path: str) -> str:
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8").strip()

# --------------------------------------------------------------------
# 1. Leitura, Count Individual e Registro de Views
# --------------------------------------------------------------------
logger.info("Iniciando leitura e registro das views temporárias...")

for sql_file in SQL_FILES:
    sql_path = f"{SQL_BASE_PATH.rstrip('/')}/{sql_file}"
    view_name = sql_file.replace(".sql", "")
    
    try:
        sql_query = read_sql_from_s3(sql_path).rstrip(';')
        
        logger.info(f"Executando Spark SQL para View: {view_name}")
        df_temp = spark.sql(sql_query)
        
        # Log de Count solicitado
        count_val = df_temp.count()
        logger.info(f"RESULTADO -> VIEW: {view_name} | COUNT: {count_val}")
        
        # Registra como view temporária para o UNION SQL
        df_temp.createOrReplaceTempView(view_name)
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {sql_file}: {e}")
#         sys.exit(1)

# # --------------------------------------------------------------------
# # 2. Consolidação via SQL UNION ALL
# # --------------------------------------------------------------------
logger.info("Executando query de consolidação (UNION ALL)...")

union_sql = """
SELECT * FROM vendas_2019
UNION ALL
SELECT * FROM vendas_2020
UNION ALL
SELECT * FROM vendas_itens_2019
UNION ALL
SELECT * FROM vendas_itens_2020_1_6
UNION ALL
SELECT * FROM vendas_itens_2020_6_12
UNION ALL
SELECT * FROM vendas_trocas_2019
UNION ALL
SELECT * FROM vendas_trocas_2020
"""

try:
    df_final = spark.sql(union_sql)
    # Log do total consolidado
    final_count = df_final.count()
    logger.info(f"TOTAL CONSOLIDADO PARA ESCRITA: {final_count}")
except Exception as e:
    logger.error(f"Erro ao executar a query de UNION: {e}")
    sys.exit(1)

# --------------------------------------------------------------------
# 3. Escrita na Tabela Iceberg (Silver)
# --------------------------------------------------------------------
full_target_name = f"{CATALOG_NAME}.{GLUE_DB_TARGET}.{TARGET_TABLE}"
logger.info(f"Iniciando escrita Iceberg em: {full_target_name}")

table_exists = spark.catalog.tableExists(full_target_name)

try:
    if not table_exists:
        logger.info("Tabela não existe. Criando nova tabela Iceberg...")
        df_final.writeTo(full_target_name).create()
    else:
        logger.info("Tabela existe. Executando Overwrite Total...")
        df_final.writeTo(full_target_name).overwrite(expr("1=1"))
except Exception as e:
    logger.error(f"Erro crítico na escrita: {e}")
    sys.exit(1)

# --------------------------------------------------------------------
# Finalização
# --------------------------------------------------------------------
job.commit()
logger.info("Job finalizado com sucesso.")