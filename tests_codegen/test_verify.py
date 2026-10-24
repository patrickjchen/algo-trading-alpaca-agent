"""Offline tests for the harness itself (no LLM, no network).

Run: python -m pytest -q tests_codegen
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from codegen import verify as V            # noqa: E402
from codegen.stages import Stage, STAGE_BY_NAME, dependents_of   # noqa: E402


def _gen(tmp_path: Path, files: dict[str, str]) -> Path:
    g = tmp_path / "generated"
    g.mkdir(parents=True)
    for rel, text in files.items():
        p = g / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    return g


def test_python_ok_with_contract(tmp_path):
    st = Stage("runtime", ("runtime.py",), "python", "x.md", must_define=("foo", "BAR"))
    g = _gen(tmp_path, {"runtime.py": "foo = lambda: 1\nBAR = 2\n"})
    ok, out = V.verify_stage(g, st)
    assert ok, out


def test_python_missing_name(tmp_path):
    st = Stage("runtime", ("runtime.py",), "python", "x.md", must_define=("foo", "BAR"))
    g = _gen(tmp_path, {"runtime.py": "foo = 1\n"})
    ok, out = V.verify_stage(g, st)
    assert not ok and "BAR" in out


def test_python_syntax_error(tmp_path):
    st = Stage("trader", ("trader.py",), "python", "x.md")
    g = _gen(tmp_path, {"trader.py": "def (:\n"})
    ok, out = V.verify_stage(g, st)
    assert not ok and "COMPILE_FAIL" in out


def test_python_import_error_cross_module(tmp_path):
    st = Stage("cli", ("cli.py",), "python", "x.md", must_define=("main",))
    g = _gen(tmp_path, {"runtime.py": "X=1\n", "cli.py": "import runtime\nimport nothere\ndef main(): pass\n"})
    ok, out = V.verify_stage(g, st)
    assert not ok and "IMPORT_FAIL" in out and "nothere" in out


def test_python_import_time_network_is_isolated(tmp_path):
    """A module that connects to Redis at import must fail fast in the sandbox, not hang."""
    st = Stage("bad", ("bad.py",), "python", "x.md")
    g = _gen(tmp_path, {"bad.py": "import os,redis\nredis.Redis.from_url(os.environ['REDIS_URL'],socket_connect_timeout=1).ping()\n"})
    ok, out = V.verify_stage(g, st)
    assert not ok and "IMPORT_FAIL" in out


def test_flask_routes_contract(tmp_path):
    st = Stage("webgui", ("webgui.py",), "python", "x.md", must_define=("app",),
               contract=("/api/health", "/api/orderbook/<path:symbol>"))
    good = ("from flask import Flask\napp=Flask(__name__)\n"
            "@app.route('/api/health')\ndef h(): return 'x'\n"
            "@app.route('/api/orderbook/<path:symbol>')\ndef o(symbol): return symbol\n")
    ok, out = V.verify_stage(_gen(tmp_path, {"webgui.py": good}), st)
    assert ok, out
    bad = "from flask import Flask\napp=Flask(__name__)\n@app.route('/api/health')\ndef h(): return 'x'\n"
    ok, out = V.verify_stage(_gen(tmp_path / "b", {"webgui.py": bad}), st)
    assert not ok and "/api/orderbook/<path:symbol>" in out


def test_html_contract(tmp_path):
    st = Stage("webgui_html", ("web/index.html",), "html", "x.md", contract=("/api/health",))
    page = "<!DOCTYPE html><html><body>" + "x" * 2100 + "<script>fetch('/api/health')</script></body></html>"
    ok, out = V.verify_stage(_gen(tmp_path, {"web/index.html": page}), st)
    assert ok, out
    page2 = "<!DOCTYPE html><html><body>" + "x" * 2100 + "<script src='https://cdn.x/y.js'></script></body></html>"
    ok, out = V.verify_stage(_gen(tmp_path / "b", {"web/index.html": page2}), st)
    assert not ok and "external dependency" in out and "/api/health" in out


def test_pytest_unit_and_live_stages(tmp_path):
    files = {
        "tests/__init__.py": "",
        "pytest.ini": "[pytest]\nmarkers =\n    live: needs Alpaca creds + Redis\n",
        "tests/conftest.py": "import pytest\n",
        "tests/test_unit_a.py": "def test_a():\n    assert 1\n",
        "tests/test_live_a.py": "import pytest\n@pytest.mark.live\ndef test_live():\n    assert 1\n",
    }
    g = _gen(tmp_path, files)
    ok, out = V.verify_stage(g, STAGE_BY_NAME["tests_unit"])
    assert ok, out
    ok, out = V.verify_stage(g, STAGE_BY_NAME["tests_live"])
    assert ok, out
    (g / "tests/test_live_b.py").write_text("def test_unmarked():\n    assert 1\n")
    ok, out = V.verify_stage(g, STAGE_BY_NAME["tests_live"])
    assert not ok and "not marked live" in out


def test_dependents():
    names = [s.name for s in dependents_of("runtime")]
    assert "subscriber" in names and "webgui" in names and "research" not in names
