FROM python:3.13-slim

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync
COPY . .

CMD uv run flask --app parquet_fe_prototype run --host 0.0.0.0 --port $PORT