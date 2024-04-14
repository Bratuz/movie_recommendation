FROM python:3.10

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt

COPY . /app

# Set the environment variable PORT
EXPOSE 8080

# Commande pour démarrer l'application
CMD ["python", "backend.py"]
