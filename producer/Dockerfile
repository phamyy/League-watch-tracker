FROM python:3.10

WORKDIR /app

COPY producer/requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-m", "producer.producer"]