"""In-process MCP tools offered to the Coder / Tester agents.

The agents never get Bash. Everything they can *run* is one of these
tools, and each tool is the same code path the driver uses for its own
deterministic decision, so an agent cannot "pass" by running something
different from what the harness will check.

Tool names as the model sees them: mcp__verify__verify_stage,
mcp__verify__run_unit_tests, mcp__verify__run_live_tests.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from claude_agent_sdk import create_sdk_mcp_server, tool

from . import verify as V
from .stages import STAGE_BY_NAME

_ARG_OK = re.compile(r"^[\w./:\-\[\]= \"']*$")


def _text(s: str, is_error: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {"content": [{"type": "text", "text": s}]}
    if is_error:
        out["is_error"] = True
    return out


def build_verify_server(gen_dir: Path, live_env_ok) -> Any:
    """Return an McpSdkServerConfig named "verify" bound to `gen_dir`.

    live_env_ok: zero-arg callable -> (bool, reason) saying whether creds and
    Redis are available for live tests.
    """

    @tool("verify_stage",
          "Run the harness's deterministic verification for one codegen stage "
          "(py_compile + import in a sandbox copy of generated/, then contract "
          "checks: required top-level names, Flask routes, HTML substrings, or "
          "pytest for the test stages). Call this after writing the file and fix "
          "until it returns OK. Args: stage (one of: " + ", ".join(STAGE_BY_NAME) + ")",
          {"stage": str})
    async def verify_stage(args: dict[str, Any]) -> dict[str, Any]:
        name = str(args.get("stage", "")).strip()
        if name not in STAGE_BY_NAME:
            return _text(f"unknown stage {name!r}; valid: {', '.join(STAGE_BY_NAME)}", True)
        ok, out = V.verify_stage(gen_dir, name)
        return _text(("OK\n" if ok else "FAILED\n") + out, not ok)

    @tool("run_unit_tests",
          "Run `pytest -m 'not live'` on generated/tests in a sandbox copy (fakeredis, "
          "temp SQLite, no network). Optional `extra` pytest args such as "
          "'-k twap -x' or 'tests/test_unit_oms.py'.",
          {"extra": str})
    async def run_unit_tests(args: dict[str, Any]) -> dict[str, Any]:
        extra = str(args.get("extra", "") or "")
        if not _ARG_OK.match(extra) or "--" in extra.replace("--collect-only", "").replace("-x", ""):
            return _text("extra args rejected; use only -k/-x/-q/paths under tests/", True)
        work = V._copy_generated(gen_dir)
        try:
            ok, out = V.run_pytest(work, ["-m", "not live", *extra.split(), *( [] if "tests" in extra else ["tests"])],
                                   env=V._sandbox_env())
        finally:
            import shutil
            shutil.rmtree(work, ignore_errors=True)
        return _text(("OK\n" if ok else "FAILED\n") + out, not ok)

    @tool("run_live_tests",
          "Run `pytest -m live` against the REAL Alpaca paper account and local Redis, "
          "in-place in generated/ (so it shares the workers' alpaca.db). Only works when "
          "ALPACA_API_KEY_ID/ALPACA_API_SECRET_KEY are set and Redis answers PING; "
          "otherwise returns SKIPPED. Optional `extra` pytest args (-k/-x).",
          {"extra": str})
    async def run_live_tests(args: dict[str, Any]) -> dict[str, Any]:
        ok_env, reason = live_env_ok()
        if not ok_env:
            return _text("SKIPPED: " + reason)
        extra = str(args.get("extra", "") or "")
        if not _ARG_OK.match(extra):
            return _text("extra args rejected", True)
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        ok, out = V.run_pytest(str(gen_dir), ["-m", "live", *extra.split(), "tests"], env=env)
        return _text(("OK\n" if ok else "FAILED\n") + out, not ok)

    return create_sdk_mcp_server(name="verify", version="1.0.0",
                                 tools=[verify_stage, run_unit_tests, run_live_tests])


VERIFY_TOOL_NAMES = ["mcp__verify__verify_stage", "mcp__verify__run_unit_tests",
                     "mcp__verify__run_live_tests"]
