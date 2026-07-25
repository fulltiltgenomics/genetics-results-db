FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml .
RUN uv pip install --system --no-cache -r pyproject.toml

COPY api/ .

ENV PORT=8080

# drop root: the service only reads its code and the read-only datasets ConfigMap mount
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app
USER 10001

CMD ["python", "main.py"]
