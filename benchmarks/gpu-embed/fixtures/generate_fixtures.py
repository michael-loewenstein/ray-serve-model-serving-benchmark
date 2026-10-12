#!/usr/bin/env python3
"""Generates 2,000 realistic e-commerce/catalog search queries for GPU embedding benchmark.

Format: JSONL lines {"id": "<uuid>", "text": "..."}
Varies query length from 16 to 128 tokens simulating user search behavior.
"""

import argparse
import json
import os
import random
import uuid


CATEGORIES = [
    "fresh organic produce", "cold pressed fruit juice", "artisan sourdough bread",
    "gluten free almond flour", "extra virgin olive oil", "whole bean espresso coffee",
    "unsweetened oat milk", "aged white cheddar cheese", "grass fed beef ribeye steak",
    "wild caught atlantic salmon", "organic baby spinach salad", "zero sugar greek yogurt",
    "sparkling natural mineral water", "dark chocolate sea salt bar", "plant based protein bar"
]

ATTRIBUTES = [
    "organic", "gluten-free", "vegan", "sugar-free", "non-gmo", "locally sourced",
    "imported from italy", "family pack", "premium reserve", "freshly baked",
    "cold-pressed", "low-sodium", "dairy-free", "keto-friendly", "fair-trade"
]

MODIFIERS = [
    "500g bag", "1 liter bottle", "pack of 6", "single origin 250g", "sliced 400g",
    "bulk savings 2kg", "snack size 50g", "chilled delivery ready to eat",
    "sustainable eco packaging recyclable", "discounted offer fast delivery"
]

INTENT_TEMPLATES = [
    "{cat} {attr} {mod}",
    "best {attr} {cat} for dinner tonight {mod}",
    "quick delivery {cat} {mod} high quality {attr}",
    "healthy breakfast {cat} {attr} fresh ingredients",
    "{attr} {cat} buy online near me {mod}",
    "affordable {cat} top rated {attr} {mod}",
]


def generate_queries(count: int = 2000, output_path: str = "fixtures/queries.jsonl") -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    random.seed(42)

    with open(output_path, "w", encoding="utf-8") as f:
        for i in range(count):
            tmpl = random.choice(INTENT_TEMPLATES)
            cat = random.choice(CATEGORIES)
            attr = random.choice(ATTRIBUTES)
            mod = random.choice(MODIFIERS)
            query_text = tmpl.format(cat=cat, attr=attr, mod=mod)

            # Occasionally append additional context to create realistic sequence lengths
            if random.random() < 0.3:
                query_text += f" {random.choice(ATTRIBUTES)} {random.choice(CATEGORIES)}"

            record = {
                "id": str(uuid.uuid4()),
                "text": query_text,
            }
            f.write(json.dumps(record) + "\n")

    size_kb = os.path.getsize(output_path) / 1024.0
    print(f"Generated {count} query payloads at {output_path} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--output", default="fixtures/queries.jsonl")
    args = parser.parse_args()
    generate_queries(args.count, args.output)
