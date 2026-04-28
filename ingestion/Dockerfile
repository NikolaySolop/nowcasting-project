FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN pip install --no-cache-dir poetry==2.3.4

COPY ingestion/pyproject.toml ingestion/poetry.lock ./ingestion/

WORKDIR /app/ingestion
RUN poetry config virtualenvs.create false \
    && poetry install --no-root

WORKDIR /app
COPY ingestion ./ingestion
COPY storage ./storage

CMD ["uvicorn", "ingestion.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
