import os
import csv
import boto3
import botocore
import mysql.connector
from mysql.connector import Error

# --- MySQL ---
DB_HOST = os.getenv("MYSQL_HOST", "host.docker.internal")
DB_USER = os.getenv("MYSQL_USER", "usuario")
DB_PASS = os.getenv("MYSQL_PASSWORD", "contraseña")
DB_NAME = os.getenv("MYSQL_DATABASE", "nombre_base_datos")
DB_TABLE = os.getenv("MYSQL_TABLE", "nombre_tabla")

# --- S3 ---
BUCKET = os.getenv("S3_BUCKET", "joelxd-storage")  # <-- cambia si quieres otro
CSV_OUT = os.getenv("CSV_FILENAME", "data_2.csv")
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"

def ensure_bucket(bucket: str, region: str):
    s3 = boto3.client("s3", region_name=region)
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' existe.")
        return
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        # 404 / NoSuchBucket => lo creamos
        if code in ("404", "NoSuchBucket", "NotFound"):
            print(f"Bucket '{bucket}' no existe. Creándolo en región {region}...")
            if region == "us-east-1":
                s3.create_bucket(Bucket=bucket)
            else:
                s3.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": region},
                )
            # esperar a que esté listo
            waiter = s3.get_waiter("bucket_exists")
            waiter.wait(Bucket=bucket)
            print(f"Bucket '{bucket}' creado.")
        else:
            raise

def main():
    conn = None
    cursor = None
    try:
        # Conexión a MySQL
        conn = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
        )
        cursor = conn.cursor()
        query = f"SELECT * FROM {DB_TABLE}"
        cursor.execute(query)

        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description] if cursor.description else []

        # Escribir CSV
        with open(CSV_OUT, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if headers:
                writer.writerow(headers)
            for row in rows:
                writer.writerow(row)

        print(f"CSV generado: {CSV_OUT} ({len(rows)} filas).")

        # S3: asegurar bucket y subir
        ensure_bucket(BUCKET, AWS_REGION)
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.upload_file(CSV_OUT, BUCKET, CSV_OUT)
        print(f"Ingesta completada. Subido s3://{BUCKET}/{CSV_OUT} (región {AWS_REGION})")

    except Error as e:
        print("Error de MySQL:", e)
        raise
    except botocore.exceptions.ClientError as e:
        print("Error de S3:", e)
        raise
    except Exception as e:
        print("Error general:", e)
        raise
    finally:
        try:
            if cursor: cursor.close()
            if conn: conn.close()
        except:
            pass

if __name__ == "__main__":
    main()
