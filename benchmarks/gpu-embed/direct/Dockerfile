FROM pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime

WORKDIR /app

RUN pip install --no-cache-dir \
    "transformers>=4.40.0,<4.49.0" \
    "sentence-transformers>=2.5.0,<3.5.0" \
    "accelerate>=0.28.0" \
    "fastapi>=0.110.0" \
    "uvicorn[standard]>=0.28.0" \
    "orjson>=3.9.0"

COPY app.py /app/app.py

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
