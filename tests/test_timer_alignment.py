# tests/test_timer_alignment.py
import ast
from pathlib import Path

ROOT = Path(__file__).parents[1]

def function(tree, cls, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == cls:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == name:
                    return item
    raise AssertionError(f"{cls}.{name} not found")

def first_line(node, predicate):
    for child in ast.walk(node):
        if predicate(child):
            return child.lineno
    return None

def test_direct_samples_before_body_read():
    tree = ast.parse((ROOT / "k8s/app/direct_app.py").read_text())
    fn = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "infer":
            fn = node
    assert fn is not None
    t0 = first_line(fn, lambda n: isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "t0" for t in n.targets))
    body = first_line(fn, lambda n: isinstance(n, ast.Await)
                      and isinstance(n.value, ast.Call)
                      and getattr(n.value.func, "attr", "") == "body")
    assert t0 < body

def test_ray_samples_before_body_read():
    tree = ast.parse((ROOT / "k8s/app/ray_serve_app.py").read_text())
    fn = function(tree, "XGBoostDeployment", "__call__")
    t0 = first_line(fn, lambda n: isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id == "t0" for t in n.targets))
    body = first_line(fn, lambda n: isinstance(n, ast.Await)
                      and isinstance(n.value, ast.Call)
                      and getattr(n.value.func, "attr", "") == "body")
    assert t0 is not None and body is not None and t0 < body

def test_ray_uses_the_passed_t0():
    tree = ast.parse((ROOT / "k8s/app/ray_serve_app.py").read_text())
    fn = function(tree, "XGBoostDeployment", "_handle_infer")

    # No local t0: the timer must come from __call__, not be restarted here.
    reassigns = [
        n for n in ast.walk(fn)
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "t0" for t in n.targets)
    ]
    assert not reassigns, "t0 must not be reassigned in _handle_infer"

    # The response's total_latency_ms must subtract the passed t0.
    dict_node = None
    for node in ast.walk(fn):
        if (isinstance(node, ast.Call)
                and getattr(node.func, "attr", "") == "dumps"
                and node.args
                and isinstance(node.args[0], ast.Dict)):
            dict_node = node.args[0]
            break
    assert dict_node is not None, "orjson.dumps({...}) not found in _handle_infer"

    value = None
    for key, val in zip(dict_node.keys, dict_node.values):
        if isinstance(key, ast.Constant) and key.value == "total_latency_ms":
            value = val
    assert value is not None, "total_latency_ms not found in response dict"

    # The expression is wrapped in round(...); inspect the expression itself.
    if (isinstance(value, ast.Call)
            and getattr(value.func, "id", "") == "round"
            and value.args):
        value = value.args[0]

    subs = [
        n for n in ast.walk(value)
        if isinstance(n, ast.BinOp)
        and isinstance(n.op, ast.Sub)
        and isinstance(n.right, ast.Name)
        and n.right.id == "t0"
    ]
    assert subs, "total_latency_ms must subtract the passed t0"
