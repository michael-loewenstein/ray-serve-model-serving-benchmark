"""Ray Serve Deployment for XGBoost CPU Tabular Model.

Optimized with:
- Direct Starlette Request/Response (bypassing FastAPI router overhead)
- orjson parsing and serialization
- HAProxy routing support via worker port 30000
- Custom latency histograms for server and model execution
"""

import os
import time
import orjson
from ray import serve
from ray.util.metrics import Histogram
from starlette.requests import Request
from starlette.responses import Response

_READINESS_RESP = Response(b'{"status":"ok"}', media_type="application/json")
_NOT_FOUND_RESP = Response(b'{"detail":"Not Found"}', status_code=404, media_type="application/json")
_METHOD_NOT_ALLOWED_RESP = Response(b'{"detail":"Method Not Allowed"}', status_code=405, media_type="application/json")


@serve.deployment(
    name="XGBoostDeployment",
    ray_actor_options={"num_cpus": 1.0},
    max_ongoing_requests=50,
)
class XGBoostDeployment:
    def __init__(self) -> None:
        import xgboost as xgb
        self._xgb = xgb

        model_path = os.environ.get("MODEL_PATH", "/models/model.json")
        self.booster = self._xgb.Booster()
        if os.path.exists(model_path):
            self.booster.load_model(model_path)
            print(f"[Ray Serve XGBoost] Loaded model from {model_path}")
        else:
            print(f"[Ray Serve XGBoost] WARNING: Model path not found at {model_path}")

        _boundaries = [0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0]
        self._total_latency_ms = Histogram(
            "ray_xgboost_request_latency_ms",
            description="End-to-end /infer latency in ms (parse + model + serialization)",
            boundaries=_boundaries,
        )
        self._model_latency_ms = Histogram(
            "ray_xgboost_model_latency_ms",
            description="Pure XGBoost booster.predict latency in ms",
            boundaries=_boundaries,
        )

    async def __call__(self, request: Request) -> Response:
        path = request.url.path
        method = request.method

        if path in ("/readiness", "/healthz"):
            if method == "GET":
                return _READINESS_RESP
            return _METHOD_NOT_ALLOWED_RESP

        if path == "/infer":
            if method == "POST":
                return await self._handle_infer(await request.body())
            return _METHOD_NOT_ALLOWED_RESP

        return _NOT_FOUND_RESP

    async def _handle_infer(self, raw: bytes) -> Response:
        t0 = time.perf_counter()
        try:
            payload = orjson.loads(raw)
        except Exception as exc:
            return Response(
                orjson.dumps({"detail": f"Invalid JSON body: {exc}"}),
                status_code=400,
                media_type="application/json",
            )

        features = payload.get("features")
        if not isinstance(features, list):
            return Response(
                b'{"detail":"Field \'features\' must be a list of floats"}',
                status_code=400,
                media_type="application/json",
            )

        t1 = time.perf_counter()
        dmat = self._xgb.DMatrix([features])
        preds = self.booster.predict(dmat)
        t2 = time.perf_counter()

        body = orjson.dumps({
            "request_id": payload.get("request_id"),
            "prediction": float(preds[0]),
            "total_latency_ms": round((t2 - t0) * 1000.0, 3),
            "model_latency_ms": round((t2 - t1) * 1000.0, 3),
        })
        t3 = time.perf_counter()

        total_ms = (t3 - t0) * 1000.0
        model_ms = (t2 - t1) * 1000.0
        self._total_latency_ms.observe(total_ms)
        self._model_latency_ms.observe(model_ms)

        return Response(body, media_type="application/json")


app = XGBoostDeployment.bind()
