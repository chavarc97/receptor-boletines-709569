import boto3
import os
import json
import time
import sqlite3
import uuid
from dotenv import load_dotenv

load_dotenv()

sqs_client = boto3.client("sqs")
sns_client = boto3.client("sns")

sqs_url = os.getenv("SQS_URL")
sns_arn = os.getenv("SNS_TOPIC_ARN")

# --- FUNCIONES DE BASE DE DATOS LOCAL ---
def init_db():
    conn = sqlite3.connect('boletines.db')
    cursor = conn.cursor()
    # Crear la tabla si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS boletines (
            id TEXT PRIMARY KEY,
            contenido TEXT,
            correo TEXT,
            s3_url TEXT,
            leido BOOLEAN
        )
    ''')
    conn.commit()
    conn.close()

def guardar_db(contenido, correo, s3_url):
    conn = sqlite3.connect('boletines.db')
    cursor = conn.cursor()
    boletin_id = str(uuid.uuid4())
    # Faltante: Generar el query INSERT para guardar: boletin_id, contenido, correo, s3_url, y False (para leido)
    cursor.execute(
        "INSERT INTO boletines (id, contenido, correo, s3_url, leido) VALUES (?, ?, ?, ?, ?)",
        (boletin_id, contenido, correo, s3_url, False)
    )
    conn.commit()
    conn.close()
    return boletin_id


# --- FUNCIONES PRINCIPALES ---
def procesar_mensaje(msg):
    body = json.loads(msg["Body"])
    # body contiene: {"contenido": "...", "correo": "...", "s3_url": "..."}
    
    contenido = body["contenido"]
    correo = body["correo"]
    s3_url = body["s3_url"]
    
    # 1. Guardar en Base de Datos usando la funcion que hicimos arriba
    boletin_id = guardar_db(contenido, correo, s3_url)
    
    # 2. Enviar correo via SNS (notificacion sencilla)
    mensaje_sns = f"Nuevo boletin generado! Puedes visualizarlo en el sistema con el ID: {boletin_id}"
    sns_client.publish(
        TopicArn=sns_arn,
        Message=mensaje_sns,
        Subject="Nuevo Boletin!"
    )
    
    # 3. Borrar el mensaje de la cola para no procesarlo dos veces
    sqs_client.delete_message(
        QueueUrl=sqs_url,
        ReceiptHandle=msg["ReceiptHandle"]
    )
    
    print(f"Mensaje procesado. ID generado: {boletin_id}")

def consumir():
    print("Iniciando monitoreo de la cola SQS...")
    while True:
        # Peticion a SQS
        response = sqs_client.receive_message(
            QueueUrl=sqs_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10 # Long polling para evitar costos excesivos
        )
        
        mensajes = response.get("Messages", [])
        for msg in mensajes:
            procesar_mensaje(msg)
            
        # Esperamos 1 segundo antes de volver a consultar para que no se sature
        time.sleep(1)

if __name__ == "__main__":
    print("Servicio receptor listo y arrancando BD...")
    init_db() # Arrancamos de manera segura creando la bd si no existe
    consumir()
