import json
import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build
import datetime
import csv
import io

# --- Configurações
# O ID da planilha (o mesmo para todas as abas)
SPREADSHEET_ID = '1dhoztJa5UQYU5YjbFnL8ooqFyAevpw71GeaNXSHgjUc'
# O nome do segredo no AWS Secrets Manager
SECRET_NAME = 'google-sheets-service-account-key'
# O nome do bucket S3 REAL
BUCKET_NAME = 'bronze-belshop'
# O prefixo base dentro do bucket
BUCKET_PREFIX = 'belshop-sheets-extract/'

# Lista das abas e seus respectivos prefixos/subpastas no S3
SHEETS_TO_EXTRACT = [
    {'name': 'Historicos', 'prefix': 'financeiro/historicos/'},
    {'name': 'Grupos_de_Historicos', 'prefix': 'financeiro/grupos_de_historicos/'},
    {'name': 'Grupos_DRE_N2', 'prefix': 'financeiro/grupos_dre_n2/'},
    {'name': 'Grupos_DRE_N1', 'prefix': 'financeiro/grupos_dre_n1/'}, 
    {'name': 'Orcamento_por_Grupo', 'prefix': 'financeiro/orcamento_por_grupo/'}
]

# --- Funções Auxiliares ---
def get_secret(secret_name):
    # ... (sua função get_secret permanece a mesma) ...
    client = boto3.client('secretsmanager')
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            raise ValueError("Chave JSON não encontrada em SecretString")

# --- Função principal da Lambda ---
def lambda_handler(event, context):
    try:
        # 1. Recuperar a chave de autenticação do Secrets Manager
        print("Recuperando chave de autenticação do Secrets Manager...")
        service_account_info = get_secret(SECRET_NAME)

        # 2. Autenticar com a Google Sheets API
        print("Autenticando na Google Sheets API...")
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=SCOPES
        )
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()

        # 3. Itera sobre a lista de abas para extrair
        s3_client = boto3.client('s3')
        successful_extractions = []
        
        for sheet_info in SHEETS_TO_EXTRACT:
            sheet_name = sheet_info['name']
            s3_path = BUCKET_PREFIX + sheet_info['prefix'] # Cria o caminho completo
            
            try:
                print(f"\n--- Processando aba: {sheet_name} ---")
                
                # 3.1. Ler os dados da aba atual
                print(f"Lendo dados da planilha: {SPREADSHEET_ID}, aba: {sheet_name}...")
                result = sheet.values().get(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f'{sheet_name}!A:Z'
                ).execute()

                values = result.get('values', [])
                
                if not values:
                    print(f"Nenhum dado encontrado na aba '{sheet_name}'. Pulando para a próxima.")
                    continue  # Pula para a próxima aba no loop
                
                print(f"Dados lidos. Total de linhas: {len(values)}")
                
                # 3.2. Construir o conteúdo CSV em memória
                csv_buffer = io.StringIO()
                csv_writer = csv.writer(csv_buffer, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                csv_writer.writerows(values)
                
                # 3.3. Lógica de Full Load: Deletar TODOS os arquivos existentes na subpasta específica
                paginator = s3_client.get_paginator('list_objects_v2')
                # Usa o caminho completo como prefixo
                pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=s3_path)

                objects_to_delete = []
                for page in pages:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            objects_to_delete.append({'Key': obj['Key']})

                if objects_to_delete:
                    print(f"Total de {len(objects_to_delete)} arquivos encontrados em '{s3_path}' para deletar.")
                    for i in range(0, len(objects_to_delete), 1000):
                        s3_client.delete_objects(
                            Bucket=BUCKET_NAME,
                            Delete={'Objects': objects_to_delete[i:i+1000]}
                        )
                    print("Arquivos antigos foram deletados com sucesso.")
                else:
                    print(f"Nenhum arquivo antigo para deletar em '{s3_path}'.")

                # 3.4. Salvar o novo arquivo CSV no S3
                current_date = datetime.datetime.now().strftime("%Y-%m-%d")
                # Concatena o caminho completo com o nome do arquivo
                file_name = f'{s3_path}{sheet_name.lower().replace(" ", "_")}_{current_date}.csv'
                
                print(f"Salvando o novo arquivo CSV em s3://{BUCKET_NAME}/{file_name}...")
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=file_name,
                    Body=csv_buffer.getvalue().encode('utf-8'),
                    ContentType='text/csv'
                )
                
                print(f"Dados da aba '{sheet_name}' salvos com sucesso em s3://{BUCKET_NAME}/{file_name}")
                successful_extractions.append(sheet_name)

            except Exception as e:
                # Trata erros específicos de cada aba, permitindo que as outras continuem
                print(f"\n! ERRO ao processar a aba '{sheet_name}': {e}")
                # A execução continua para a próxima aba

        # 4. Retorno final da função Lambda
        if successful_extractions:
            return {
                'statusCode': 200,
                'body': json.dumps(f'Extração das abas {", ".join(successful_extractions)} concluída com sucesso!')
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps('Nenhuma extração de planilha foi concluída com sucesso.')
            }

    except Exception as e:
        # Erro global (ex: problema no Secrets Manager)
        print(f"Ocorreu um erro global: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro global na extração: {str(e)}')
        }