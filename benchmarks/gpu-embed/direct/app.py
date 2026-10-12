"""Direct serving GPU baseline for sentence-transformers embedding model.

Features:
- SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2") in FP16 on CUDA
- Async background queue worker accumulating batches up to max_batch_size (32)
  or waiting up to batch_wait_timeout (5 ms)
- Resolves caller futures upon batch completion
- Uses orjson for rapid serialization
"""

import asyncio
import os
import time
from typing import List, Tuple
import orjson
import torch
from fastapi import FastAPI, Request, Response, status
from sentence_transformers import SentenceTransformer

app = FastAPI(title="Direct GPU Embedding Server (Async Micro-Batching)")

MODEL_NAME = os.environ.get("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
MAX_BATCH_SIZE = int(os.environ.get("MAX_BATCH_SIZE", "32"))
BATCH_WAIT_TIMEOUT_S = float(os.environ.get("BATCH_WAIT_TIMEOUT_S", "0.005"))

device = "cuda:0" if torch.cuda.is_available() else "cpu"
print(f"[Direct GPU] Loading {MODEL_NAME} on {device} (FP16={torch.cuda.is_available()})...")

model = SentenceTransformer(MODEL_NAME, device=device)
if torch.cuda.is_available():
    model.half()

# Async request queue: (text, Future)
request_queue: asyncio.Queue[Tuple[str, asyncio.Future]] = asyncio.Queue()


async def batch_worker():
    """Background consumer accumulating requests into dynamic batches."""
    print(f"[Direct GPU] Started batch worker (max_batch={MAX_BATCH_SIZE}, wait={BATCH_WAIT_TIMEOUT_S}s)")
    while True:
        try:
            # Wait for first item
            first_text, first_fut = await request_queue.get()
            batch: List[Tuple[str, asyncio.Future]] = [(first_text, first_fut)]
            deadline = time.time() + BATCH_WAIT_TIMEOUT_S

            # Collect subsequent arrivals up to MAX_BATCH_SIZE or deadline
            while len(batch) < MAX_BATCH_SIZE:
                timeout = deadline - time.time()
                if timeout <= 0:
                    break
                try:
                    item = await asyncio.wait_for(request_queue.get(), timeout=timeout)
                    batch.append(item)
                except asyncio.TimeoutError:
                    break

            texts = [item[0] for item in batch]
            futures = [item[1] for item in batch]

            t0 = time.perf_counter()
            with torch.inference_mode():
                embeddings = model.encode(texts, batch_size=len(texts), convert_to_numpy=True)
            t1 = time.perf_counter()
            model_ms = (t1 - t0) * 1000.0

            # Resolve all client futures
            for i, fut in enumerate(futures):
                if not fut.done():
                    fut.set_result((embeddings[i].tolist(), model_ms))

        except Exception as exc:
            print(f"[Direct GPU] Error in batch worker: {exc}")
            for _, fut in batch:
                if not fut.done():
                    fut.set_exception(exc)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(batch_worker())


_READINESS_RESP = Response(
    orjson.dumps({"status": "ready", "cuda": torch.cuda.is_available(), "device": device}),
    media_type="application/json",
)


@app.get("/readiness")
def readiness() -> Response:
    return _READINESS_RESP


@app.get("/healthz")
def healthz() -> Response:
    return _READINESS_RESP


@app.post("/embed")
async def embed(request: Request) -> Response:
    t0 = time.perf_counter()
    raw = await request.body()
    try:
        payload = orjson.loads(raw)
    except Exception as exc:
        return Response(
            orjson.dumps({"detail": f"Invalid JSON body: {exc}"}),
            status_code=status.HTTP_400_BAD_REQUEST,
            media_type="application/json",
        )

    text = payload.get("text", "")
    req_id = payload.get("id", "")

    # Enqueue to dynamic batcher
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    await request_queue.put((text, fut))

    try:
        embedding, model_ms = await fut
    except Exception as exc:
        return Response(
            orjson.dumps({"detail": f"Inference failed: {exc}"}),
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            media_type="application/json",
        )

    t1 = time.perf_counter()
    total_ms = (t1 - t0) * 1000.0

    return Response(
        orjson.dumps({
            "id": req_id,
            "embedding": embedding,
            "model_latency_ms": round(model_ms, 3),
            "total_latency_ms": round(total_ms, 3),
        }),
        media_type="application/json",
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, workers=1, access_log=False)
