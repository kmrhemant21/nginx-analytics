FROM python:3.9-slim-bullseye
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get upgrade -y && pip install --upgrade pip && pip install -r requirements.txt && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY main.py .
COPY wait-for-it.sh .
RUN chmod +x wait-for-it.sh
CMD ["python", "main.py"]