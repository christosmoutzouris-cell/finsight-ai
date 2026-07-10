FROM python:3.11-slim

WORKDIR /app

# Εγκατάσταση dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "-u", "train_and_predict.py"]