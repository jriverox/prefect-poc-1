import os
from pymongo import MongoClient
import pandas as pd
import boto3
from prefect import task, flow
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

@task
def fetch_data_from_mongodb():
    client = MongoClient(MONGO_URI)
    db = client.get_database("demo")
    collection = db.products
    data = list(collection.find({"department": "Books"}))
    return data

@task
def create_excel_file(data):
    df = pd.DataFrame(data)
    file_name = f"report_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
    df.to_excel(file_name, index=False)
    return file_name

@task
def upload_to_s3(file_name):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    s3_client.upload_file(file_name, S3_BUCKET_NAME, file_name)
    s3_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{file_name}"
    return s3_url

@task
def publish_sns_message(s3_url):
    sns_client = boto3.client(
        'sns',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    message = f"Reporte generado, haga clic en el siguiente link para descargarlo: {s3_url}"
    sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=message)

@task
def remove_file(file_name):
    try:
        os.remove(file_name)
        print(f"El archivo '{file_name}' ha sido eliminado exitosamente.")
    except FileNotFoundError:
        print(f"El archivo '{file_name}' no existe.")
    except PermissionError:
        print(f"No tienes permisos suficientes para eliminar el archivo '{file_name}'.")

@flow(log_prints=True)
def main_flow():
    data = fetch_data_from_mongodb()
    print("Cantidad de registros:", len(data))
    file_name = create_excel_file(data)
    print("Archivo creado:", file_name)
    s3_url = upload_to_s3(file_name)
    print("Archivo subido a S3:", s3_url)
    publish_sns_message(s3_url)
    print("Mensaje publicado en SNS")
    remove_file(file_name)

if __name__ == "__main__":
    main_flow.serve(name="my-first-deployment")
    # main_flow.deploy(
    #     name="my-first-deployment", 
    #     work_pool_name="my-managed-pool", 
    # )
