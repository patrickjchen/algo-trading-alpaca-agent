"""Deterministic verifiers for generated artifacts.

Ported from TradingWithAlpaca/alpaca_codegen.py (verify_python_file /
verify_html_file) with two changes:
  * the AG2 LocalCommandLineCodeExecutor is replaced by subprocess.run;
  * every check runs against a COPY of the whole generated/ directory in a
    temp dir, so cross-module imports resolve exactly as on disk and the
    check can never dirty generated/ (no stray alpaca.db, no __pycache__).

Additional contract checks (new): `must_define` names after import, Flask
url rules for webgui, required substrings for the HTML, and pytest runs for
the test stages. The SAME function (`verify_stage`) is exposed to the Coder
as an MCP tool and run by the driver after the Coder stops, so the agent and
the harness can never disagree about what "passing" means.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from .stages import Stage, STAGE_BY_NAME

SUBPROC_TIMEOUT = 120          # seconds for compile+import
PYTEST_TIMEOUT = 900           # seconds for a pytest run
OUTPUT_CAP = 6000              # chars of output fed back to the Coder

# Files/dirs never copied into the verification sandbox.
_SKIP = {"__pycache__", ".pytest_cache", "alpaca.db", "alpaca.db-wal", "alpaca.db-shm"}


def _copy_generated(gen_dir: Path) -> str:
    work = tempfile.mkdtemp(prefix="twc_verify_")
    for item in gen_dir.iterdir():
        if item.name in _SKIP:
            continue
        dst = Path(work) / item.name
        if item.is_dir():
            shutil.copytree(item, dst, ignore=shutil.ignore_patterns(*_SKIP))
        else:
            shutil.copy2(item, dst)
    return work


def _sandbox_env() -> dict[str, str]:
    """Env for the sandbox: no creds, isolated DB path, isolated Redis URL.

    Generated modules must not touch the network at import time anyway, but
    stripping creds makes an accidental call fail loudly instead of trading.
    """
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("ALPACA_", "ANTHROPIC_", "OPENROUTER_"))}
    env["ALPACA_DB_PATH"] = os.path.join(tempfile.gettempdir(), "twc_verify_unused.db")
    env["REDIS_URL"] = "redis://127.0.0.1:1/0"   # nothing listens; must not be contacted
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def _tail(s: str) -> str:
    s = s.strip()
    return s if len(s) <= OUTPUT_CAP else "...[truncated]...\n" + s[-OUTPUT_CAP:]


# ---------------------------------------------------------------- python

def verify_python(work: str, stage: Stage) -> tuple[bool, str]:
    target = stage.primary
    mod = stage.module
    must = list(stage.must_define)
    routes = list(stage.contract) if stage.name == "webgui" else []
    code = f"""
import subprocess, sys, importlib, json, os
sys.path.insert(0, os.getcwd())
r = subprocess.run([sys.executable, '-m', 'py_compile', {target!r}], capture_output=True, text=True)
if r.returncode != 0:
    print('COMPILE_FAIL:'); print(r.stdout); print(r.stderr); print('EXIT: 1'); sys.exit(0)
print('COMPILE_OK')
try:
    m = importlib.import_module({mod!r})
except BaseException as e:
    import traceback; traceback.print_exc()
    print('IMPORT_FAIL: ' + type(e).__name__ + ': ' + str(e)); print('EXIT: 1'); sys.exit(0)
print('IMPORT_OK')
missing = [n for n in {must!r} if not hasattr(m, n)]
if missing:
    print('CONTRACT_FAIL: module must define top-level names: ' + ', '.join(missing)); print('EXIT: 1'); sys.exit(0)
routes = {routes!r}
if routes:
    rules = {{str(x.rule) for x in m.app.url_map.iter_rules()}}
    miss = [x for x in routes if x not in rules]
    if miss:
        print('CONTRACT_FAIL: Flask app is missing routes: ' + ', '.join(miss))
        print('present: ' + ', '.join(sorted(rules))); print('EXIT: 1'); sys.exit(0)
print('EXIT: 0')
"""
    try:
        r = subprocess.run([sys.executable, "-I", "-c", code], cwd=work, env=_sandbox_env(),
                           capture_output=True, text=True, timeout=SUBPROC_TIMEOUT)
        out = r.stdout + r.stderr
    except subprocess.TimeoutExpired:
        return False, (f"TIMEOUT: compile+import of {target} exceeded {SUBPROC_TIMEOUT}s. "
                       "Something runs at import time (a loop, a blocking connect). "
                       "Move all work into main().")
    ok = "EXIT: 0" in out
    return ok, _tail(out)


# ------------------------------------------------------------------ html

def verify_html(work: str, stage: Stage) -> tuple[bool, str]:
    p = Path(work) / stage.primary
    if not p.exists():
        return False, f"{stage.primary} was not written"
    text = p.read_text(errors="replace")
    head = text.lstrip()[:200].lower()
    problems = []
    if len(text.strip()) < 2000:
        problems.append(f"file is suspiciously short ({len(text)} chars)")
    if not head.startswith("<!doctype"):
        problems.append("missing leading <!DOCTYPE html> declaration")
    if "</html>" not in text.lower():
        problems.append("missing closing </html> tag (truncated?)")
    if text.lstrip().startswith("```"):
        problems.append("file starts with a markdown fence; write raw HTML")
    lowered = text.lower()
    for s in ("<script src=", "import(", "unpkg.com", "cdn.jsdelivr", "cdnjs."):
        if s in lowered:
            problems.append(f"external dependency detected ({s!r}); the page must be self-contained")
    for needle in stage.contract:
        if needle not in text:
            problems.append(f"required substring not found: {needle!r}")
    if problems:
        return False, "HTML verification failed:\n  - " + "\n  - ".join(problems)
    return True, f"HTML verify OK ({len(text)} chars)"


# -------------------------------------------------------------- markdown

def verify_markdown(work: str, stage: Stage) -> tuple[bool, str]:
    p = Path(work) / stage.primary
    if not p.exists():
        return False, f"{stage.primary} was not written"
    text = p.read_text(errors="replace")
    problems = []
    if len(text.strip()) < 1500:
        problems.append(f"brief is too short ({len(text)} chars)")
    for h in ("## Part A", "## Part B", "## Part C"):
        if h not in text:
            problems.append(f"missing section header {h!r}")
    if text.lstrip().startswith("```"):
        problems.append("file starts with a fence; write raw markdown")
    if problems:
        return False, "RESEARCH.md verification failed:\n  - " + "\n  - ".join(problems)
    return True, f"RESEARCH.md OK ({len(text)} chars)"


# ---------------------------------------------------------------- pytest

def run_pytest(cwd: str, args: list[str], env: dict[str, str] | None = None,
               timeout: int = PYTEST_TIMEOUT) -> tuple[bool, str]:
    cmd = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *args]
    try:
        r = subprocess.run(cmd, cwd=cwd, env=env or {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                           capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return False, f"TIMEOUT: pytest {' '.join(args)} exceeded {timeout}s"
    out = r.stdout + ("\n" + r.stderr if r.stderr.strip() else "")
    # exit 5 == no tests collected; treat as failure for unit, handled by caller for live
    return r.returncode == 0, _tail(out) + f"\n[pytest exit code {r.returncode}]"


def verify_pytest_unit(work: str, stage: Stage) -> tuple[bool, str]:
    # compile everything under tests first for a crisp error message
    for p in sorted(Path(work, "tests").glob("*.py")) if Path(work, "tests").exists() else []:
        r = subprocess.run([sys.executable, "-m", "py_compile", str(p)], capture_output=True, text=True)
        if r.returncode != 0:
            return False, _tail(r.stderr)
    ok, out = run_pytest(work, ["-m", "not live", "tests"], env=_sandbox_env())
    return ok, out


def verify_pytest_live(work: str, stage: Stage) -> tuple[bool, str]:
    """Live tests are NOT executed here (needs creds + Redis + running
    workers). We require that they compile, collect, and are all marked live."""
    for p in sorted(Path(work, "tests").glob("test_live_*.py")):
        r = subprocess.run([sys.executable, "-m", "py_compile", str(p)], capture_output=True, text=True)
        if r.returncode != 0:
            return False, _tail(r.stderr)
    ok, out = run_pytest(work, ["--collect-only", "-m", "live", "tests"], env=_sandbox_env())
    if not ok:
        return False, out
    if "test_live_" not in out:
        return False, "no live tests collected under -m live; mark every live test with @pytest.mark.live\n" + out
    # every test in test_live_*.py must carry the marker: collect the complement
    ok2, out2 = run_pytest(work, ["--collect-only", "-m", "not live", "tests"], env=_sandbox_env())
    if "test_live_" in out2:
        return False, "some tests in test_live_*.py are not marked live:\n" + out2
    return True, out


# ------------------------------------------------------------- dispatch

_VERIFIERS = {
    "python": verify_python,
    "html": verify_html,
    "markdown": verify_markdown,
    "pytest_unit": verify_pytest_unit,
    "pytest_live": verify_pytest_live,
}


def verify_stage(gen_dir: Path, stage: Stage | str) -> tuple[bool, str]:
    """Copy generated/ to a sandbox and run the stage's verifier there."""
    st = STAGE_BY_NAME[stage] if isinstance(stage, str) else stage
    missing = [t for t in st.targets if "*" not in t and not (gen_dir / t).exists()
               and not t.endswith(("conftest.py", "__init__.py", "pytest.ini"))]
    if missing:
        return False, f"target file(s) not written: {missing}"
    work = _copy_generated(gen_dir)
    try:
        return _VERIFIERS[st.kind](work, st)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def verify_all_imports(gen_dir: Path) -> tuple[bool, str]:
    """Import every generated module (used after a --fix to catch broken dependents)."""
    work = _copy_generated(gen_dir)
    try:
        mods = [p.stem for p in sorted(Path(work).glob("*.py"))]
        code = ("import importlib,sys,os\nsys.path.insert(0, os.getcwd())\nbad=[]\n"
                f"for m in {mods!r}:\n"
                "    try: importlib.import_module(m)\n"
                "    except BaseException as e: bad.append(f'{m}: {type(e).__name__}: {e}')\n"
                "print('\\n'.join(bad)); print('EXIT:', 1 if bad else 0)")
        r = subprocess.run([sys.executable, "-I", "-c", code], cwd=work, env=_sandbox_env(),
                           capture_output=True, text=True, timeout=SUBPROC_TIMEOUT)
        out = r.stdout + r.stderr
        return "EXIT: 0" in out, _tail(out)
    finally:
        shutil.rmtree(work, ignore_errors=True)
