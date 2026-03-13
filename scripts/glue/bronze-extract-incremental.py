# Script genérico de ingestão na Camada BRONZE (Iceberg). Realiza Full Scan na origem e Merge/Overwrite no destino.
import sys
import logging
import time
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, col, row_number, year, expr
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from typing import List

# --- Configuração Inicial ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# --- Obter Argumentos ---

REQUIRED_ARGS = ['JOB_NAME', 'TABLE_NAME'] #comentar se quiser rodar manualmente
args = getResolvedOptions(sys.argv, REQUIRED_ARGS) #comentar se quiser rodar manualmente


# 🛑 FORÇANDO PARÂMETROS PARA A TABELA 'CLIENTES' (HARDCODE)
# Para rodar manualmente com hardcode, você PRECISA descomentar este bloco e os que o chamam.
# args = {
#     'JOB_NAME': 'bronze-extract-incremental',
#     'TABLE_NAME': 'vendas_saldos',
#     'PK': ''
# }

# --- Variáveis Específicas da Tabela (Parametrizadas) ---
DB_TABLE: str = args['TABLE_NAME']
# PK opcional (não gera erro se vier vazia)
PK_STRING = args.get('PK', "").strip()
PK_COLS: List[str] = [col.strip() for col in PK_STRING.split(',') if col.strip()]
HAS_PK: bool = bool(PK_COLS)

# --- Inicialização do Job Glue ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Variáveis de Configuração Global ---
BUCKET_NAME: str = 'bronze-belshop'
CONNECTION_NAME: str = "postgres-conn"
ICEBERG_CATALOG_NAME: str = "glue_catalog"
GLUE_DATABASE_NAME: str = "belshop_bronze_db"

# Nome completo da tabela (CATALOG.DB.TABLE) - Usado para DML (Solução do Erro)
ICEBERG_FULL_TABLE_NAME: str = f"{ICEBERG_CATALOG_NAME}.{GLUE_DATABASE_NAME}.{DB_TABLE}"
# A variável ICEBERG_DB_TABLE_ONLY foi removida e substituída pelo FULL_TABLE_NAME nas cláusulas DML.

# --- Configurações do Spark para Iceberg ---
spark.conf.set(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.warehouse", f"s3://{BUCKET_NAME}/iceberg_warehouse/")
spark.conf.set(f"spark.sql.catalog.{ICEBERG_CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")

# --- Funções Auxiliares ---
def log_time_step(step_name: str, start_time: float, table_name: str = DB_TABLE):
    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"[{table_name}] {step_name} completed in {elapsed_time:.2f} seconds")

# ---------------------------------------------------------------------
## 🗄️ Fluxo de ETL Principal - Carregamento Genérico
# ---------------------------------------------------------------------

logger.info(f"Iniciando Full Scan para {DB_TABLE}. Modo: {'MERGE/HARD DELETE' if HAS_PK else 'FULL OVERWRITE'}")

# --- Etapa 1: Leitura COMPLETA do PostgreSQL (Genérica) ---
start_time_read_pg = time.time()
try:
    dynamic_frame_pg = glueContext.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "dbtable": DB_TABLE,
            "useConnectionProperties": "true",
            "connectionName": CONNECTION_NAME,
        },
        transformation_ctx=f"{DB_TABLE}_read_full_origin_node"
    )
    df_source_pg = dynamic_frame_pg.toDF()
    log_time_step(f"Leitura COMPLETA da tabela {DB_TABLE} no PostgreSQL", start_time_read_pg)

    row_count_pg = df_source_pg.count()
    if row_count_pg == 0:
        logger.warning(f"A tabela {DB_TABLE} retornou 0 registros. Nenhuma operação de escrita será executada.")
        job.commit()
        sys.exit(0)
    else:
        logger.info(f"Lidos {row_count_pg} registros do PostgreSQL.")

except Exception as e:
    logger.error(f"ERRO CRÍTICO ao ler do JDBC para a tabela {DB_TABLE}: {e}")
    sys.exit(1)

# --- Etapa 2: Preparação dos Dados de Origem e Validação ---
df_source_metadata = df_source_pg.withColumn("etl_updated_at", current_timestamp())

if HAS_PK:
    # --- LÓGICA MERGE: Tenta deduplicar e valida PKs ---
    missing_pk_cols = [c for c in PK_COLS if c not in df_source_metadata.columns]
    if missing_pk_cols:
        logger.error(f"Colunas PK ausentes no DataFrame de origem para {DB_TABLE}: {missing_pk_cols}. Abortando.")
        sys.exit(1)
    
    # Deduplicação Crítica para evitar MERGE_CARDINALITY_VIOLATION
    window_spec = Window.partitionBy(*PK_COLS).orderBy(col("etl_updated_at").desc())
    df_source = df_source_metadata.withColumn("rn", row_number().over(window_spec)) \
                                 .filter(col("rn") == 1) \
                                 .drop("rn")
    logger.info(f"Deduplicação concluída. {df_source.count()} registros únicos para merge.")
else:
    df_source = df_source_metadata # Sem PK, usa o DF com metadata diretamente

TEMP_VIEW_NAME = f"{DB_TABLE}_source_view"
df_source.createOrReplaceTempView(TEMP_VIEW_NAME)

# --- Etapa 3: Execução da Escrita Iceberg ---
start_time_write_iceberg = time.time()

# 1. Verifica se a tabela existe antes de tentar criar (substitui o try/except anterior)
table_exists = spark.catalog.tableExists(ICEBERG_FULL_TABLE_NAME)

if not table_exists:
    logger.info(f"[{DB_TABLE}] Criando tabela Iceberg {ICEBERG_FULL_TABLE_NAME}...")
    spark.sql(f"""
        CREATE TABLE {ICEBERG_FULL_TABLE_NAME}
        USING iceberg
        PARTITIONED BY (YEAR(etl_updated_at))
        LOCATION 's3://{BUCKET_NAME}/iceberg_tables/{DB_TABLE}/'
        AS SELECT * FROM {TEMP_VIEW_NAME} WHERE 1=0
    """)
else:
    logger.info(f"[{DB_TABLE}] Tabela já existe no catálogo.")

# 2. Executa a escrita conforme a presença de PK
if HAS_PK:
    # --- MODO MERGE INTO ---
    on_clause = " AND ".join([f"target.{c} = source.{c}" for c in PK_COLS])
    all_cols = df_source.columns
    non_pk_cols_to_update = [c for c in all_cols if c not in PK_COLS and c != "etl_updated_at"]
    
    update_set = ", ".join([f"target.{c} = source.{c}" for c in non_pk_cols_to_update] + ["target.etl_updated_at = source.etl_updated_at"])
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join([f"source.{c}" for c in all_cols])

    merge_sql = f"""
    MERGE INTO {ICEBERG_FULL_TABLE_NAME} AS target
    USING {TEMP_VIEW_NAME} AS source
    ON {on_clause}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({insert_vals})
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE
    """
    spark.sql(merge_sql)
    logger.info(f"Operação MERGE INTO (Hard Delete) concluída para {DB_TABLE}.")

else:
    # --- MODO FULL OVERWRITE ---
    logger.info(f"Executando overwrite total para {DB_TABLE}")
    df_source.writeTo(ICEBERG_FULL_TABLE_NAME).overwrite(expr("1=1"))
    logger.info(f"Operação FULL OVERWRITE concluída para {DB_TABLE}.")

log_time_step(f"Escrita Iceberg {DB_TABLE}", start_time_write_iceberg)

job.commit()
logger.info(f"Carregamento da tabela {DB_TABLE} concluído com sucesso.")