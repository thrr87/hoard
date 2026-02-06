FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOARD_DATA_DIR=/data

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY hoard /app/hoard

RUN pip install --no-cache-dir .

VOLUME ["/data"]
EXPOSE 19850

ENTRYPOINT ["hoard"]
CMD ["serve", "--host", "0.0.0.0", "--allow-remote", "--port", "19850"]
