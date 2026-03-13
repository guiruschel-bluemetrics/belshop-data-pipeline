# Script para Glue: Carrega o arquivo mais recente dos arquivos do Google Sheets extraídos pelo lambda para tabelas Iceberg no Glue Catalog

import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name

# Parâmetros básicos
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# 1. Configuração Spark + Iceberg
CATALOG_NAME = "glue_catalog"
TARGET_DB = "belshop_bronze_db"
BUCKET_NAME = "bronze-belshop"
S3_BASE_SOURCE = "belshop-sheets-extract/"
WAREHOUSE_PATH = "s3://bronze-belshop/iceberg_warehouse/"

# MAPEAMENTO DAS PASTAS E SUBPASTAS
FOLDERS_MAP = {
    "cadastro_auxiliar_lojas": "cadastro-auxiliar-lojas/",
    "fin_grupos_historicos": "financeiro/grupos_de_historicos/",
    "fin_grupos_dre_n1": "financeiro/grupos_dre_n1/",
    "fin_grupos_dre_n2": "financeiro/grupos_dre_n2/",
    "fin_historicos": "financeiro/historicos/",
    "fin_orcamento_grupo": "financeiro/orcamento_por_grupo/",
    "metas_lojas": "metas/lojas/",
    "metas_geral": "metas/metas/"
}

spark = (SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.broken-implicitly-purgable-files", "false")
    .getOrCreate())

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Inicializa o cliente S3 para buscar o arquivo mais recente
s3_client = boto3.client('s3')

# 2. Execução do Loop de Carga
for table_prefix, relative_path in FOLDERS_MAP.items():
    prefix = f"{S3_BASE_SOURCE}{relative_path}"
    
    # Listar objetos na pasta do S3
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    
    if 'Contents' not in response:
        print(f"PULANDO: NENHUM ARQUIVO ENCONTRADO EM {prefix}")
        continue

    # Filtrar apenas CSVs e ordenar para pegar o último (Data mais recente por nome)
    all_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].lower().endswith('.csv')]
    all_files.sort()
    
    if not all_files:
        print(f"PULANDO: Nenhum CSV válido em {prefix}")
        continue

    # O arquivo mais recente será o último da lista ordenada
    latest_file_key = all_files[-1]
    source_path = f"s3://{BUCKET_NAME}/{latest_file_key}"
    
    table_name = f"{table_prefix}_sheets"
    full_table_path = f"{CATALOG_NAME}.{TARGET_DB}.{table_name}"
    
    print(f"--- PROCESSANDO ARQUIVO RECENTE: {latest_file_key} ---")
    
    try:
        # Leitura do arquivo específico
        df = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("delimiter", ";")
              .load(source_path))
        
        # Adiciona coluna com o nome do arquivo para rastreabilidade
        df = df.withColumn("arquivo_origem", input_file_name())
        
        # Tratamento de colunas para conformidade com Iceberg (remover espaços, pontos e minúsculas)
        for col_name in df.columns:
            clean_col = (col_name.replace(" ", "_")
                                .replace(".", "")
                                .replace("(", "")
                                .replace(")", "")
                                .replace("-", "_")
                                .lower())
            df = df.withColumnRenamed(col_name, clean_col)

        print(f"Escrita Iceberg em: {full_table_path}")
        
        # Gravação no Iceberg (createOrReplace garante que a tabela terá apenas os dados do último arquivo)
        df.writeTo(full_table_path).createOrReplace()
        
    except Exception as e:
        print(f"ERRO ao processar {table_name}: {str(e)}")

job.commit()
print("Job finalizado com sucesso.")