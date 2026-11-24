FROM python:3.12-slim
WORKDIR /app
COPY images/ingest-world-bank/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY images/ingest-world-bank/world_bank_ingest ./world_bank_ingest
COPY images/ingest-world-bank/tests ./tests
COPY config/pipeline.yml ./config/pipeline.yml
ENTRYPOINT ["python", "-m", "world_bank_ingest"]
