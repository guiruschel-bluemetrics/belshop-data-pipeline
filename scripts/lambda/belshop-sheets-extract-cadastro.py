import json
import boto3
from google.oauth2 import service_account
from googleapiclient.discovery import build
import datetime
import csv
import io

# --- Configurações
SPREADSHEET_ID = '1psXzNn04P_dBPF9aCQihMKIIvip-FVlg850PIWHk9Fk'
SHEET_NAME = 'Página1'
SECRET_NAME = 'google-sheets-service-account-key'

BUCKET_NAME = 'bronze-belshop'
BUCKET_PREFIX = 'belshop-sheets-extract/cadastro-auxiliar-lojas/'

# --- Funções Auxiliares ---
def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        raise e

    if 'SecretString' in response:
        return json.loads(response['SecretString'])
    else:
        raise ValueError("Chave JSON não encontrada em SecretString.")

# --- Função principal da Lambda ---
def lambda_handler(event, context):
    try:
        print("🔐 Recuperando chave do Secrets Manager...")
        service_account_info = get_secret(SECRET_NAME)

        print("📄 Autenticando na Google Sheets API...")
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=SCOPES
        )
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()

        print(f"📥 Lendo dados da planilha {SPREADSHEET_ID} ({SHEET_NAME})...")
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{SHEET_NAME}!A:Z'
        ).execute()

        values = result.get('values', [])

        if not values:
            print("⚠️ Nenhum dado encontrado na planilha.")
            return {
                'statusCode': 200,
                'body': json.dumps('Nenhum dado encontrado na planilha.')
            }

        print(f"📊 Linhas lidas: {len(values)}")

        # --- Montar CSV em memória ---
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(values)

        # --- Full Load: Deletar arquivos antigos ---
        s3_client = boto3.client('s3')

        print(f"🧹 Limpando arquivos em s3://{BUCKET_NAME}/{BUCKET_PREFIX} ...")

        # Agora lista TUDO no bucket e filtra pela pasta correta
        listed = s3_client.list_objects_v2(Bucket=BUCKET_NAME)

        delete_list = [
            {"Key": obj["Key"]}
            for obj in listed.get("Contents", [])
            if obj["Key"].startswith(BUCKET_PREFIX)
        ]

        if delete_list:
            print(f"🗑️ Deletando {len(delete_list)} arquivos...")

            for i in range(0, len(delete_list), 1000):
                s3_client.delete_objects(
                    Bucket=BUCKET_NAME,
                    Delete={"Objects": delete_list[i:i+1000]}
                )
        else:
            print("🟦 Nenhum arquivo encontrado para remover.")

        # --- Salvar novo CSV ---
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        file_name = f"{BUCKET_PREFIX}{SHEET_NAME.lower().replace(' ', '_')}_{current_date}.csv"

        print(f"⬆️ Salvando arquivo em s3://{BUCKET_NAME}/{file_name} ...")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv"
        )

        print(f"✅ Upload concluído: s3://{BUCKET_NAME}/{file_name}")

        return {
            'statusCode': 200,
            'body': json.dumps("Extração concluída com sucesso!")
        }

    except Exception as e:
        print(f"❌ Erro: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Erro na extração: {str(e)}")
        }
