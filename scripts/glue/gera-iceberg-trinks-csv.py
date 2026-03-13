#esse código faz a leitura dos arquivos CSV do S3, faz uma limpeza básica nos nomes das colunas e trata a coluna "valor_faturado_bruto" 
# para garantir que seja armazenada como Decimal no formato correto. 
# Em seguida, grava os dados em tabelas Iceberg usando o catálogo Glue.


import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType

# Parâmetros básicos
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Configuração Spark + Iceberg
CATALOG_NAME = "glue_catalog"
TARGET_DB = "belshop_bronze_db"
S3_BASE_SOURCE = "s3://bronze-belshop/belshop-trinks/"
WAREHOUSE_PATH = "s3://bronze-belshop/iceberg_warehouse/"

FOLDERS = ["Clientes", "Empresas", "Pagamentos", "Profissionais", "Servicos", "Vendas"]

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.broken-implicitly-purgable-files", "false")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

for folder in FOLDERS:
    source_path = f"{S3_BASE_SOURCE}{folder}/"
    table_name = f"{folder.lower()}_trinks"
    full_table_path = f"{CATALOG_NAME}.{TARGET_DB}.{table_name}"

    print(f"Lendo CSV de: {source_path}")

    try:
        # 🔹 Bronze deve ler tudo como string
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")  # IMPORTANTE
            .option("delimiter", ";")
            .load(source_path)
        )

        # 🔹 Limpeza de nomes de colunas
        for col in df.columns:
            clean_col = (
                col.replace(" ", "_")
                   .replace(".", "")
                   .replace("(", "")
                   .replace(")", "")
                   .lower()
            )
            df = df.withColumnRenamed(col, clean_col)

        # 🔹 Tratamento específico para coluna financeira (se existir)
        if "valor_faturado_bruto" in df.columns:
            
            df = df.withColumn(
                "valor_faturado_bruto",
                F.regexp_replace("valor_faturado_bruto", r"\.", "")  # remove separador milhar
            )

            df = df.withColumn(
                "valor_faturado_bruto",
                F.regexp_replace("valor_faturado_bruto", ",", ".")  # troca decimal BR → US
            )

            df = df.withColumn(
                "valor_faturado_bruto",
                F.col("valor_faturado_bruto").cast(DecimalType(18, 2))
            )

        print(f"Gravando tabela Iceberg: {full_table_path}")

        df.writeTo(full_table_path).createOrReplace()

    except Exception as e:
        print(f"Erro ao processar a pasta {folder}: {e}")

job.commit()
