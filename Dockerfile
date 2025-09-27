# Usa una imagen base de Python (slim para una versión más ligera)
FROM python:3-slim

# Establece el directorio de trabajo en el contenedor
WORKDIR /programas/ingesta

# Instala las dependencias necesarias: boto3 (para interactuar con S3) y mysql-connector-python (para MySQL)
RUN pip install boto3 mysql-connector-python

# Copia todo el contenido del directorio actual (donde está el Dockerfile) al directorio de trabajo del contenedor
COPY . .

# Define el comando predeterminado para ejecutar el script ingesta.py cuando se inicie el contenedor
CMD ["python3", "./ingesta.py"]
