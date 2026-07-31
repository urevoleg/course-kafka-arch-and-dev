FROM python:3.10-slim

# рабочая директория
WORKDIR /app

# зависимости
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY consumer_batch.py .

# Запускаем producer
CMD ["python3", "consumer_batch.py"]