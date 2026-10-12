FROM rayproject/ray:2.55.1-py311-gpu

WORKDIR /home/ray

RUN pip install --no-cache-dir \
    "torch" \
    "transformers>=4.40.0,<4.49.0" \
    "sentence-transformers>=2.5.0,<3.5.0" \
    "accelerate>=0.28.0" \
    "orjson>=3.9.0"

COPY serve_embed.py /home/ray/serve_embed.py

EXPOSE 8000 8080 30000

CMD ["serve", "run", "serve_embed:app"]
