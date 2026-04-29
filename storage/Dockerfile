FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN pip install --no-cache-dir poetry==2.3.4

COPY storage/pyproject.toml storage/poetry.lock ./storage/

WORKDIR /app/storage
RUN poetry config virtualenvs.create false \
    && poetry install --no-root

WORKDIR /app
COPY storage ./storage

CMD ["alembic", "-c", "storage/alembic.ini", "upgrade", "head"]
