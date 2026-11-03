FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY hf_upload_server.py hf_push.py hfpush server_config.example.json ./

EXPOSE 8000

# 运行时提供 -e HF_TOKEN=... -e HF_SERVER_API_KEY=...
# 仓库可用 --repo-id 指定，或挂载 /app/server_config.json
CMD ["python", "hf_upload_server.py", "--host", "0.0.0.0", "--port", "8000"]
