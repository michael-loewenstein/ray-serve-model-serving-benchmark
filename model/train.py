#!/usr/bin/env python3
"""Synthetic XGBoost Model Generator and Test Fixture Builder.

This script creates:
1. model/artifacts/model.json — trained XGBoost binary classification model
   (50 features, 100 trees, max depth 6)
2. loadtest/fixtures/sample_payloads.jsonl — sample request payloads for k6 load testing

Deterministic execution is guaranteed via explicit random seeds.
"""

import argparse
import json
import os
import uuid
import numpy as np
import xgboost as xgb


def train_and_export(
    n_samples: int = 5000,
    n_features: int = 50,
    n_estimators: int = 100,
    max_depth: int = 6,
    seed: int = 42,
    output_model_path: str = "artifacts/model.json",
    output_fixtures_path: str = "../loadtest/fixtures/sample_payloads.jsonl",
    n_test_payloads: int = 1000,
) -> None:
    # Ensure destination directories exist
    os.makedirs(os.path.dirname(os.path.abspath(output_model_path)), exist_ok=True)
    if output_fixtures_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_fixtures_path)), exist_ok=True)

    print(f"[1/4] Generating synthetic training dataset: {n_samples} samples, {n_features} features (seed={seed})...")
    np.random.seed(seed)
    X = np.random.randn(n_samples, n_features).astype(np.float32)

    # Synthetic non-linear decision boundary
    logits = (
        0.5 * X[:, 0]
        - 0.8 * X[:, 1]
        + 0.3 * (X[:, 2] ** 2)
        + 0.6 * np.sin(X[:, 3])
        + np.sum(X[:, 4:10] * 0.1, axis=1)
    )
    prob = 1.0 / (1.0 + np.exp(-logits))
    y = (prob > 0.5).astype(np.int32)

    dtrain = xgb.DMatrix(X, label=y)
    params = {
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": max_depth,
        "eta": 0.1,
        "tree_method": "hist",
        "seed": seed,
        "nthread": 2,
    }

    print(f"[2/4] Training XGBoost booster ({n_estimators} rounds, max_depth={max_depth})...")
    booster = xgb.train(params, dtrain, num_boost_round=n_estimators)

    booster.save_model(output_model_path)
    model_size_kb = os.path.getsize(output_model_path) / 1024.0
    print(f"[3/4] Exported model to {output_model_path} ({model_size_kb:.1f} KB)")

    # Validation inference check
    test_booster = xgb.Booster()
    test_booster.load_model(output_model_path)
    sample_vec = X[0:1]
    pred = float(test_booster.predict(xgb.DMatrix(sample_vec))[0])
    print(f"      Validation check: test prediction = {pred:.4f}")

    # Generate test fixtures if requested
    if output_fixtures_path and n_test_payloads > 0:
        print(f"[4/4] Generating {n_test_payloads} test fixtures to {output_fixtures_path}...")
        test_vectors = np.random.randn(n_test_payloads, n_features).astype(np.float32)

        with open(output_fixtures_path, "w", encoding="utf-8") as f:
            for i in range(n_test_payloads):
                record = {
                    "request_id": str(uuid.uuid4()),
                    "features": [round(float(v), 5) for v in test_vectors[i]],
                }
                f.write(json.dumps(record) + "\n")

        fixtures_size_kb = os.path.getsize(output_fixtures_path) / 1024.0
        print(f"      Exported {n_test_payloads} fixtures ({fixtures_size_kb:.1f} KB)")


def main():
    parser = argparse.ArgumentParser(description="Train and export synthetic XGBoost model and benchmark fixtures.")
    parser.add_argument("--n-samples", type=int, default=5000, help="Number of synthetic training rows")
    parser.add_argument("--n-features", type=int, default=50, help="Number of float input features")
    parser.add_argument("--n-estimators", type=int, default=100, help="Number of boosting rounds (trees)")
    parser.add_argument("--max-depth", type=int, default=6, help="Maximum tree depth")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--output-model", default="artifacts/model.json", help="Path to export model.json")
    parser.add_argument("--output-fixtures", default="../loadtest/fixtures/sample_payloads.jsonl", help="Path to export fixtures")
    parser.add_argument("--n-test-payloads", type=int, default=1000, help="Number of test JSON payload lines")
    args = parser.parse_args()

    train_and_export(
        n_samples=args.n_samples,
        n_features=args.n_features,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        seed=args.seed,
        output_model_path=args.output_model,
        output_fixtures_path=args.output_fixtures,
        n_test_payloads=args.n_test_payloads,
    )


if __name__ == "__main__":
    main()
