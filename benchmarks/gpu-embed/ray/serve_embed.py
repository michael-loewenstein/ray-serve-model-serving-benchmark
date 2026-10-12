"""Ray Serve GPU embedding deployment using @serve.batch for dynamic micro-batching.

Features:
- SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2") on CUDA in FP16
- Ray Serve native @serve.batch(max_batch_size=32, batch_wait_timeout_s=0.005)
- Worker HAProxy port 30000 fast-path routing
- orjson serialization
"""

import os
import time
from typing import List
import orjson
import torch
from ray import serve
from ray.util.metrics import Histogram
from starlette.requests import Request
from starlette.responses import Response

_NOT_FOUND_RESP = Response(b'{"detail":"Not Found"}', status_code=404, media_type="application/json")
_METHOD_NOT_ALLOWED_RESP = Response(b'{"detail":"Method Not Allowed"}', status_code=405, media_type="application/json")


@serve.deployment(
    name="EmbeddingDeployment",
    ray_actor_options={"num_gpus": 1, "num_cpus": 2, "resources": {"worker_only": 1}},
    max_ongoing_requests=100,
    autoscaling_config=None,
)
class EmbeddingDeployment:
    def __init__(self):
        from sentence_transformers import SentenceTransformer
        self.device = "cuda:0" if torch.cuda.is_available() else "cpu"
        print(f"[Ray GPU] Initializing model on {self.device} (CUDA available: {torch.cuda.is_available()})...")
        self.model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device=self.device)
        if torch.cuda.is_available():
            self.model.half()

        _boundaries = [0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0]
        self._total_latency = Histogram(
            "ray_embed_request_latency_ms",
            description="End-to-end /embed latency in ms",
            boundaries=_boundaries,
        )
        self._batch_latency = Histogram(
            "ray_embed_batch_inference_latency_ms",
            description="Pure batch GPU model.encode latency in ms",
            boundaries=_boundaries,
        )

    @serve.batch(max_batch_size=32, batch_wait_timeout_s=0.005)
    async def batched_encode(self, texts: List[str]) -> List[List[float]]:
        t0 = time.perf_counter()
        with torch.inference_mode():
            embeddings = self.model.encode(texts, batch_size=len(texts), convert_to_numpy=True)
        t1 = time.perf_counter()
        self._batch_latency.observe((t1 - t0) * 1000.0)
        return embeddings.tolist()

    async def __call__(self, request: Request) -> Response:
        path = request.url.path
        method = request.method

        if path in ("/readiness", "/healthz"):
            if method == "GET":
                return Response(
                    orjson.dumps({"status": "ok", "cuda": torch.cuda.is_available(), "device": self.device}),
                    media_type="application/json",
                )
            return _METHOD_NOT_ALLOWED_RESP

        if path == "/embed":
            if method == "POST":
                return await self._handle_embed(await request.body())
            return _METHOD_NOT_ALLOWED_RESP

        return _NOT_FOUND_RESP

    async def _handle_embed(self, raw: bytes) -> Response:
        t0 = time.perf_counter()
        try:
            payload = orjson.loads(raw)
        except Exception as exc:
            return Response(
                orjson.dumps({"detail": f"Invalid JSON body: {exc}"}),
                status_code=400,
                media_type="application/json",
            )

        text = payload.get("text", "")
        req_id = payload.get("id", "")

        t_m0 = time.perf_counter()
        embedding = await self.batched_encode(text)
        t_m1 = time.perf_counter()

        t1 = time.perf_counter()
        total_ms = (t1 - t0) * 1000.0
        model_ms = (t_m1 - t_m0) * 1000.0
        self._total_latency.observe(total_ms)

        body = orjson.dumps({
            "id": req_id,
            "embedding": embedding,
            "model_latency_ms": round(model_ms, 3),
            "total_latency_ms": round(total_ms, 3),
        })
        return Response(body, media_type="application/json")


app = EmbeddingDeployment.bind()
