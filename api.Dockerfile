# Dockerfile
FROM python:3.12-slim AS builder

COPY requirements_api.txt .
RUN pip install --user --no-cache-dir -r requirements_api.txt

# Runtime stage
FROM python:3.12-slim

WORKDIR /app
COPY --from=builder /root/.local /root/.local
COPY ./api .

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

