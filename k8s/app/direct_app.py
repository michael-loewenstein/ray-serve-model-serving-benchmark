"""Direct Serving Baseline for XGBoost CPU Tabular Model.

High-performance baseline utilizing FastAPI + Uvicorn + orjson.
Exposes:
- GET /healthz
- GET /readiness
- POST /infer
"""

import os
import time
import orjson
import xgboost as xgb
from fastapi import FastAPI, Request, Response, status

app = FastAPI(title="Direct CPU XGBoost Serving Baseline")

MODEL_PATH = os.environ.get("MODEL_PATH", "/models/model.json")
booster = xgb.Booster()
if os.path.exists(MODEL_PATH):
    booster.load_model(MODEL_PATH)
    print(f"[Direct CPU] Loaded model from {MODEL_PATH}")
else:
    print(f"[Direct CPU] WARNING: Model path not found at {MODEL_PATH}")

_READINESS_RESP = Response(b'{"status":"ready"}', media_type="application/json")


@app.get("/readiness")
def readiness() -> Response:
    return _READINESS_RESP


@app.get("/healthz")
def healthz() -> Response:
    return _READINESS_RESP


@app.post("/infer")
async def infer(request: Request) -> Response:
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

    features = payload.get("features")
    if not isinstance(features, list):
        return Response(
            b'{"detail":"Field \'features\' must be a list of floats"}',
            status_code=status.HTTP_400_BAD_REQUEST,
            media_type="application/json",
        )

    t1 = time.perf_counter()
    dmat = xgb.DMatrix([features])
    preds = booster.predict(dmat)
    t2 = time.perf_counter()

    body = orjson.dumps({
        "request_id": payload.get("request_id"),
        "prediction": float(preds[0]),
        "total_latency_ms": round((t2 - t0) * 1000.0, 3),
        "model_latency_ms": round((t2 - t1) * 1000.0, 3),
    })
    return Response(body, media_type="application/json")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("direct_app:app", host="0.0.0.0", port=8000, workers=1, access_log=False)
