import boto3
import mysql.connector
import csv

# Configuración de MySQL
db_config = {
    'host': 'localhost',  # Si MySQL está en la misma instancia EC2, usa 'localhost' o '127.0.0.1'
    'user': 'usuario',    # Reemplaza con el nombre de usuario de MySQL que creaste
    'password': 'contraseña',  # Reemplaza con la contraseña del usuario MySQL
    'database': 'nombre_base_datos'  # Reemplaza con el nombre de la base de datos que deseas usar
}

# Configuración de S3
ficheroUpload = "data.csv"  # El nombre del archivo CSV que se subirá
nombreBucket = "gcr-output-01"  # Reemplaza con tu bucket S3

# Conexión a MySQL
conexion = mysql.connector.connect(**db_config)
cursor = conexion.cursor()

# Consultar la base de datos y obtener los registros de la tabla
query = "SELECT * FROM nombre_tabla"  # Cambia 'nombre_tabla' por la tabla que deseas leer
cursor.execute(query)

# Guardar los registros en un archivo CSV
with open(ficheroUpload, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Escribir el encabezado del CSV (opcional, solo si es necesario)
    writer.writerow([desc[0] for desc in cursor.description])  # Escribe los nombres de las columnas
    # Escribir las filas
    writer.writerows(cursor.fetchall())

# Subir el archivo CSV a S3
s3 = boto3.client('s3')
response = s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)
print("Ingesta completada:", response)

# Cerrar la conexión
cursor.close()
conexion.close()
