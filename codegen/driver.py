"""Deterministic orchestration of the Claude Agent SDK codegen.

Roles are separate `query()` calls, chosen by THIS code, not by a lead model:

  Researcher  -> generated/RESEARCH.md   (reads the installed alpaca-py source)
  Coder       -> one Stage at a time; must call mcp__verify__verify_stage itself
  (harness)   -> re-runs the SAME verifier after the Coder stops; retries with
                 feedback via `resume=<session>` (fresh session after 3 fails)
  Tester      -> only triages pytest failures into {stage, traceback} so the
                 harness can re-engage the right Coder. Pass/fail is decided by
                 the harness's own pytest run, never by the agent.

Nothing here knows Alpaca; all domain knowledge lives in codegen/prompts/.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import fnmatch
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any

from claude_agent_sdk import (AssistantMessage, ClaudeAgentOptions, HookMatcher,
                              ResultMessage, TextBlock, ToolUseBlock, query)

from . import verify as V
from .stages import STAGES, STAGE_BY_NAME, Stage, dependents_of
from .tools import VERIFY_TOOL_NAMES, build_verify_server

ROOT = Path(__file__).resolve().parent.parent
GEN_DIR = ROOT / "generated"
PROMPTS = ROOT / "codegen" / "prompts"
LOG_DIR = ROOT / ".codegen_logs"
BACKUP_DIR = ROOT / ".codegen_backup"
FAILED_DIR = ROOT / ".codegen_failed"

DEFAULT_MODEL = os.environ.get("CLAUDE_CODEGEN_MODEL", "claude-opus-5-5")
DEFAULT_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]
MAX_RETRIES = 5
FRESH_SESSION_AFTER = 3          # resumed attempts before we start a new session
TEST_ROUNDS = 3

RESEARCH_URLS = (
    "https://docs.alpaca.markets/docs/real-time-crypto-pricing-data",
    "https://docs.alpaca.markets/docs/crypto-trading",
    "https://docs.alpaca.markets/docs/websocket-streaming",
)

_RUN_TS = dt.datetime.now().strftime("%Y%m%d-%H%M%S")


# ------------------------------------------------------------------ utils

def _log(event: dict[str, Any]) -> None:
    LOG_DIR.mkdir(exist_ok=True)
    event = {"ts": dt.datetime.now().isoformat(timespec="seconds"), **event}
    with (LOG_DIR / f"run-{_RUN_TS}.jsonl").open("a") as f:
        f.write(json.dumps(event, default=str) + "\n")


def _say(msg: str) -> None:
    print(msg, flush=True)


def _prompt(name: str) -> str:
    p = PROMPTS / name
    if not p.exists():
        raise SystemExit(f"missing prompt file {p}")
    return p.read_text()


def _rel(path_str: str) -> str | None:
    """Path as the agent wrote it -> path relative to generated/, or None if outside."""
    p = Path(path_str)
    if not p.is_absolute():
        p = GEN_DIR / p
    try:
        return p.resolve().relative_to(GEN_DIR.resolve()).as_posix()
    except ValueError:
        return None


def _matching_files(patterns: tuple[str, ...]) -> list[str]:
    out: set[str] = set()
    for pat in patterns:
        for p in GEN_DIR.glob(pat):
            if p.is_file():
                out.add(p.relative_to(GEN_DIR).as_posix())
    return sorted(out)


def _stage_present(st: Stage) -> bool:
    return bool(_matching_files(st.targets))


def check_auth() -> None:
    if os.environ.get("ANTHROPIC_API_KEY"):
        return
    if any(os.environ.get(k) for k in ("CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX",
                                        "CLAUDE_CODE_USE_FOUNDRY", "ANTHROPIC_AUTH_TOKEN")):
        return
    raise SystemExit(
        "ANTHROPIC_API_KEY is not set.\n"
        "The Claude Agent SDK needs an Anthropic Console API key (a claude.ai\n"
        "subscription login is not permitted for SDK-built tools, and the SDK cannot\n"
        "route to OpenRouter/Gemini). Create one at https://console.anthropic.com/\n"
        "then:  export ANTHROPIC_API_KEY=sk-ant-...")


def live_env_ok() -> tuple[bool, str]:
    missing = [k for k in ("ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY") if not os.environ.get(k)]
    if missing:
        return False, f"env missing: {missing}"
    try:
        import redis  # runtime dep
        r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
                                 socket_connect_timeout=2)
        r.ping()
    except Exception as e:  # noqa: BLE001
        return False, f"redis not reachable: {e}"
    return True, "creds + redis OK"


# ------------------------------------------------------------------ hooks

def make_write_guard(allowed: tuple[str, ...]):
    async def guard(input_data, tool_use_id, context):  # noqa: ANN001
        tool = input_data.get("tool_name", "")
        ti = input_data.get("tool_input") or {}
        if tool in ("Write", "Edit", "MultiEdit", "NotebookEdit"):
            fp = ti.get("file_path") or ti.get("notebook_path") or ""
            rel = _rel(fp)
            if rel is None or not any(fnmatch.fnmatch(rel, g) for g in allowed):
                return {"hookSpecificOutput": {
                    "hookEventName": "PreToolUse", "permissionDecision": "deny",
                    "permissionDecisionReason":
                        f"This stage may only write {list(allowed)} (relative to generated/). "
                        f"Refused: {fp}. Other files are priors; use their public surface."}}
        return {}
    return guard


async def webfetch_guard(input_data, tool_use_id, context):  # noqa: ANN001
    if input_data.get("tool_name") == "WebFetch":
        url = (input_data.get("tool_input") or {}).get("url", "")
        if not any(url.startswith(u) for u in RESEARCH_URLS):
            return {"hookSpecificOutput": {
                "hookEventName": "PreToolUse", "permissionDecision": "deny",
                "permissionDecisionReason": f"Only these URLs may be fetched: {RESEARCH_URLS}. "
                                            "The installed alpaca-py source is the ground truth."}}
    return {}


# ------------------------------------------------------------------ query

def _summ(block: ToolUseBlock) -> str:
    i = block.input or {}
    for k in ("file_path", "stage", "url", "pattern", "extra", "command"):
        if k in i:
            return f"{block.name}({k}={str(i[k])[:90]})"
    return block.name


async def run_query(role: str, prompt: str, options: ClaudeAgentOptions) -> dict[str, Any]:
    res: dict[str, Any] = {"session_id": None, "cost": 0.0, "subtype": None, "result": None,
                           "structured": None, "error": None, "num_turns": 0}
    t0 = time.monotonic()
    try:
        async for m in query(prompt=prompt, options=options):
            if isinstance(m, AssistantMessage):
                for b in m.content:
                    if isinstance(b, ToolUseBlock):
                        _say(f"      [{role}] -> {_summ(b)}")
                    elif isinstance(b, TextBlock) and b.text.strip():
                        txt = " ".join(b.text.split())
                        _say(f"      [{role}] {txt[:220]}{'...' if len(txt) > 220 else ''}")
            elif isinstance(m, ResultMessage):
                res.update(session_id=m.session_id, cost=m.total_cost_usd or 0.0,
                           subtype=m.subtype, result=m.result, structured=m.structured_output,
                           num_turns=m.num_turns)
                if m.is_error:
                    res["error"] = f"{m.subtype}: {m.errors or m.result}"
    except Exception as e:  # noqa: BLE001  (ProcessError, CLINotFoundError, ...)
        res["error"] = f"{type(e).__name__}: {e}"
    res["secs"] = round(time.monotonic() - t0, 1)
    _say(f"      [{role}] done: {res['subtype']} turns={res['num_turns']} "
         f"cost=${res['cost']:.2f} {res['secs']}s" + (f" ERROR={res['error']}" if res["error"] else ""))
    return res


def base_options(model: str, budget: float, max_turns: int, system_prompt: str,
                 tools: list[str], hooks: dict | None, resume: str | None = None,
                 mcp: dict | None = None, output_format: dict | None = None) -> ClaudeAgentOptions:
    return ClaudeAgentOptions(
        model=model, cwd=str(GEN_DIR), system_prompt=system_prompt,
        tools=[t for t in tools if not t.startswith("mcp__")],
        allowed_tools=tools, permission_mode="acceptEdits",
        setting_sources=[],                 # never load this repo's CLAUDE.md / user settings
        max_turns=max_turns, max_budget_usd=budget, resume=resume,
        hooks=hooks, mcp_servers=mcp or {}, output_format=output_format,
        env={"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"},
    )


# ------------------------------------------------------------- backups

def backup(st: Stage) -> dict[str, bytes]:
    snap: dict[str, bytes] = {}
    for rel in _matching_files(st.targets):
        snap[rel] = (GEN_DIR / rel).read_bytes()
        dst = BACKUP_DIR / _RUN_TS / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(snap[rel])
    return snap


def restore(st: Stage, snap: dict[str, bytes]) -> None:
    """Move the failed attempt to .codegen_failed/ and put the last-good files back.
    If there was no last-good version, leave the failed attempt in place for inspection."""
    for rel in _matching_files(st.targets):
        dst = FAILED_DIR / _RUN_TS / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(GEN_DIR / rel, dst)
    if not snap:
        _say(f"  [{st.name}] no previous version; failed attempt left in generated/ for inspection")
        return
    for rel in _matching_files(st.targets):
        if rel not in snap:
            (GEN_DIR / rel).unlink()
    for rel, data in snap.items():
        (GEN_DIR / rel).write_bytes(data)
    _say(f"  [{st.name}] restored last-good files; failed attempt in {FAILED_DIR / _RUN_TS}")


# ----------------------------------------------------------- researcher

def alpaca_pkg_dir() -> str:
    try:
        import alpaca  # noqa
        return str(Path(alpaca.__file__).parent)
    except Exception:  # noqa: BLE001
        return "(alpaca-py not installed: pip install -r requirements.txt)"


async def run_research(model: str, budget: float) -> bool:
    st = STAGE_BY_NAME["research"]
    _say(f"=== [research] -> generated/RESEARCH.md  ({model})")
    snap = backup(st)
    hooks = {"PreToolUse": [HookMatcher(matcher="Write|Edit|MultiEdit", hooks=[make_write_guard(st.targets)]),
                            HookMatcher(matcher="WebFetch", hooks=[webfetch_guard])]}
    opts = base_options(model, min(budget, st.max_budget_usd), st.max_turns, _prompt("researcher.md"),
                        ["Read", "Glob", "Grep", "Write", "Edit", "WebFetch"], hooks)
    prompt = (f"The installed alpaca-py package is at: {alpaca_pkg_dir()}\n"
              f"Installed version: {_alpaca_version()}\n"
              f"You may also WebFetch exactly these URLs: {list(RESEARCH_URLS)}\n\n"
              "Write generated/RESEARCH.md (cwd is generated/, so the path is RESEARCH.md). "
              "Read the SDK source for every claim you make; mark anything unverified "
              "with `# TODO verify`. When the file is written, reply DONE.")
    res = await run_query("researcher", prompt, opts)
    ok, out = V.verify_stage(GEN_DIR, st)
    _log({"stage": "research", "ok": ok, "cost": res["cost"], "error": res["error"]})
    if not ok:
        _say(f"  [research] verify failed:\n{out}")
        restore(st, snap)
    return ok


def _alpaca_version() -> str:
    try:
        from importlib.metadata import version
        return version("alpaca-py")
    except Exception:  # noqa: BLE001
        return "unknown"


# ---------------------------------------------------------------- coder

def _priors_md(st: Stage) -> str:
    out = ""
    for rel in st.priors:
        p = GEN_DIR / rel
        if not p.exists():
            out += f"\n\n### EXISTING: {rel} -- NOT GENERATED YET (missing on disk)"
            continue
        lang = {"py": "python", "html": "html", "md": "markdown"}.get(p.suffix.lstrip("."), "text")
        out += (f"\n\n### EXISTING: {rel} (already on disk; do NOT modify; use its public surface)\n"
                f"```{lang}\n{p.read_text()}\n```")
    return out


def _coder_prompt(st: Stage, symbols: list[str], feedback: str) -> str:
    research = (GEN_DIR / "RESEARCH.md").read_text() if (GEN_DIR / "RESEARCH.md").exists() else "(none)"
    ask = ("Research brief (API facts; the installed SDK source is the final authority):\n\n"
           f"```markdown\n{research}\n```" + _priors_md(st) +
           f"\n\nSYMBOLS (defaults): {symbols}\n\n"
           f"TASK: write {list(st.targets)} (paths relative to the cwd, which is generated/). "
           f"Then call mcp__verify__verify_stage with stage=\"{st.name}\" and fix until it reports OK. "
           "Do not write any other file. When verification is OK, reply with the single word DONE.")
    if feedback:
        ask += ("\n\nPREVIOUS ATTEMPT FAILED. Fix this (do not regress anything else):\n"
                f"```\n{feedback.strip()}\n```")
    return ask


async def run_stage(st: Stage, symbols: list[str], model: str, budget: float,
                    feedback: str = "", max_retries: int = MAX_RETRIES) -> bool:
    if st.name == "research":
        return await run_research(model, budget)
    _say(f"=== [{st.name}] -> {list(st.targets)}  ({model})")
    snap = backup(st)
    system_prompt = _prompt("coder_preamble.md") + "\n\n" + _prompt(st.prompt_file)
    verify_server = build_verify_server(GEN_DIR, live_env_ok)
    hooks = {"PreToolUse": [HookMatcher(matcher="Write|Edit|MultiEdit|NotebookEdit",
                                        hooks=[make_write_guard(st.targets)])]}
    tools = ["Read", "Glob", "Grep", "Write", "Edit", *VERIFY_TOOL_NAMES]
    session: str | None = None
    total_cost = 0.0
    for attempt in range(1, max_retries + 1):
        fresh = session is None or attempt > FRESH_SESSION_AFTER
        if fresh:
            session = None
        _say(f"  [{st.name}] attempt {attempt}/{max_retries} ({'fresh' if fresh else 'resume'})")
        opts = base_options(model, min(budget, st.max_budget_usd), st.max_turns, system_prompt,
                            tools, hooks, resume=session, mcp={"verify": verify_server})
        prompt = (_coder_prompt(st, symbols, feedback) if fresh else
                  f"The harness re-ran the deterministic verifier and it FAILED:\n```\n{feedback.strip()}\n```\n"
                  f"Fix {list(st.targets)} in place, call mcp__verify__verify_stage(stage=\"{st.name}\") "
                  "until OK, then reply DONE.")
        res = await run_query("coder", prompt, opts)
        total_cost += res["cost"]
        if res["session_id"]:
            session = res["session_id"]
        err = str(res["error"] or "").lower()
        if res["error"] and ("api key" in err or "authentication" in err or "anthropic_api_key" in err):
            _say(f"  [{st.name}] auth error, aborting: {res['error']}")
            restore(st, snap)
            return False
        ok, out = V.verify_stage(GEN_DIR, st)
        _log({"stage": st.name, "attempt": attempt, "ok": ok, "cost": res["cost"],
              "turns": res["num_turns"], "secs": res["secs"], "error": res["error"]})
        if ok:
            _say(f"  [{st.name}] verify OK  (stage cost ${total_cost:.2f})")
            return True
        _say(f"  [{st.name}] verify FAILED:\n" + "\n".join("        " + l for l in out.splitlines()[-25:]))
        feedback = out if not res["error"] else f"{res['error']}\n\n{out}"
    _say(f"  [{st.name}] FAILED after {max_retries} attempts (cost ${total_cost:.2f})")
    restore(st, snap)
    return False


# --------------------------------------------------------------- tester

TRIAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "failures": {"type": "array", "items": {
            "type": "object",
            "properties": {
                "test": {"type": "string"},
                "stage": {"type": "string", "enum": [s.name for s in STAGES]},
                "summary": {"type": "string"},
                "traceback": {"type": "string"},
            },
            "required": ["test", "stage", "summary", "traceback"],
            "additionalProperties": False}},
        "notes": {"type": "string"},
    },
    "required": ["failures", "notes"],
    "additionalProperties": False,
}


def _harness_pytest(live: bool) -> tuple[bool, str]:
    if not live:
        work = V._copy_generated(GEN_DIR)
        try:
            return V.run_pytest(work, ["-m", "not live", "tests"], env=V._sandbox_env())
        finally:
            shutil.rmtree(work, ignore_errors=True)
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    return V.run_pytest(str(GEN_DIR), ["-m", "live", "tests"], env=env)


async def test_gate(symbols: list[str], model: str, budget: float, rounds: int = TEST_ROUNDS) -> bool:
    if not (GEN_DIR / "tests").exists():
        _say("=== [test gate] no generated/tests directory; skipping")
        return False
    for rnd in range(1, rounds + 1):
        _say(f"=== [test gate] round {rnd}/{rounds}: pytest -m 'not live'")
        ok_u, out_u = _harness_pytest(live=False)
        _say("    unit: " + ("OK" if ok_u else "FAILED"))
        live_ok, reason = live_env_ok()
        if live_ok:
            _say("=== [test gate] pytest -m live (real Alpaca paper + Redis)")
            ok_l, out_l = _harness_pytest(live=True)
            _say("    live: " + ("OK" if ok_l else "FAILED"))
        else:
            ok_l, out_l = True, f"live tests skipped: {reason}"
            _say(f"    live: skipped ({reason})")
        _log({"stage": "test_gate", "round": rnd, "unit_ok": ok_u, "live_ok": ok_l})
        if ok_u and ok_l:
            return True
        report = ("UNIT TESTS:\n" + out_u + "\n\nLIVE TESTS:\n" + out_l)
        _say("    asking Tester to triage failures -> stages")
        opts = base_options(model, min(budget, 4.0), 25, _prompt("tester.md"),
                            ["Read", "Glob", "Grep", *VERIFY_TOOL_NAMES], None,
                            mcp={"verify": build_verify_server(GEN_DIR, live_env_ok)},
                            output_format={"type": "json_schema", "schema": TRIAGE_SCHEMA})
        stage_table = "\n".join(f"  {s.name}: {list(s.targets)}" for s in STAGES if s.name != "research")
        res = await run_query("tester", f"Stages and the files they own:\n{stage_table}\n\n"
                              f"pytest output:\n```\n{report[-12000:]}\n```", opts)
        triage = res["structured"]
        if not isinstance(triage, dict):
            try:
                triage = json.loads(res["result"] or "{}")
            except Exception:  # noqa: BLE001
                triage = {}
        failures = triage.get("failures", []) if isinstance(triage, dict) else []
        if not failures:
            _say("    Tester produced no actionable triage; stopping test gate")
            return False
        by_stage: dict[str, list[str]] = {}
        for f in failures:
            by_stage.setdefault(f["stage"], []).append(f"{f['test']}: {f['summary']}\n{f['traceback']}")
        for name in list(by_stage)[:2]:
            fb = ("Tests fail at RUN TIME (compile+import pass). Fix the root cause in this file; "
                  "if the test itself is wrong, say so in your final message instead of hacking the code.\n\n"
                  + "\n\n".join(by_stage[name]))
            await run_stage(STAGE_BY_NAME[name], symbols, model, budget, feedback=fb)
        ok_i, out_i = V.verify_all_imports(GEN_DIR)
        if not ok_i:
            _say("    import check after fixes FAILED:\n" + out_i)
    return False


# ----------------------------------------------------------------- main

def _select(stage_args: list[str], force_research: bool) -> list[Stage]:
    if stage_args == ["missing"]:
        sel = [s for s in STAGES if not _stage_present(s)]
        skipped = [s.name for s in STAGES if _stage_present(s)]
        if skipped:
            _say(f"  skipping (already present): {skipped}")
    elif stage_args == ["all"]:
        sel = [s for s in STAGES if s.name != "research" or force_research or not _stage_present(s)]
    else:
        bad = [a for a in stage_args if a not in STAGE_BY_NAME]
        if bad:
            raise SystemExit(f"unknown stage(s) {bad}; valid: missing, all, {', '.join(STAGE_BY_NAME)}")
        sel = [STAGE_BY_NAME[a] for a in stage_args]
    if force_research and STAGE_BY_NAME["research"] not in sel:
        sel.insert(0, STAGE_BY_NAME["research"])
    return sel


async def amain(ns: argparse.Namespace) -> int:
    check_auth()
    GEN_DIR.mkdir(exist_ok=True)
    (GEN_DIR / "web").mkdir(exist_ok=True)
    symbols = ns.symbols or DEFAULT_SYMBOLS
    _say(f"=== claude_codegen | model={ns.model} | budget/stage<=${ns.budget} | gen={GEN_DIR}")
    results: dict[str, bool] = {}

    if ns.fix:
        text = sys.stdin.read() if ns.fix == "-" else Path(ns.fix).read_text()
        if ns.stage in (["missing"], ["all"]) or len(ns.stage) != 1:
            raise SystemExit("--fix requires exactly one --stage <name>")
        st = STAGE_BY_NAME[ns.stage[0]]
        fb = ("Previous version PASSES compile/import but CRASHES at run time with this "
              f"traceback. Fix the root cause.\n\nTraceback:\n```\n{text.strip()}\n```")
        results[st.name] = await run_stage(st, symbols, ns.model, ns.budget, feedback=fb,
                                           max_retries=ns.max_retries)
        deps = dependents_of(st.name)
        if deps:
            ok, out = V.verify_all_imports(GEN_DIR)
            _say(f"  dependents {[d.name for d in deps]} import check: {'OK' if ok else 'FAILED\n' + out}")
    elif not ns.only_tests:
        if not (GEN_DIR / "RESEARCH.md").exists() or ns.force_research:
            results["research"] = await run_research(ns.model, ns.budget)
            if not results["research"]:
                _say("research failed; the Coder would run without a brief. Aborting.")
                return 1
        for st in _select(ns.stage, False):
            if st.name == "research":
                continue
            results[st.name] = await run_stage(st, symbols, ns.model, ns.budget,
                                               max_retries=ns.max_retries)
            if not results[st.name] and ns.fail_fast:
                break

    if not ns.skip_tests and not ns.fix:
        results["test_gate"] = await test_gate(symbols, ns.model, ns.budget)

    _say("\n=== Summary ===")
    for k, v in results.items():
        _say(f"  {k:12s} {'OK' if v else 'FAILED'}")
    _say(f"  log: {LOG_DIR / f'run-{_RUN_TS}.jsonl'}")
    return 0 if all(results.values()) else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="claude_codegen.py",
                                description="Generate the Alpaca pipeline with the Claude Agent SDK.")
    p.add_argument("--stage", nargs="+", default=["missing"],
                   help="missing (default) | all | one or more stage names: " + " ".join(STAGE_BY_NAME))
    p.add_argument("--fix", metavar="FILE", help="runtime traceback file ('-' = stdin); needs one --stage")
    p.add_argument("--force-research", action="store_true", help="regenerate RESEARCH.md")
    p.add_argument("--skip-tests", action="store_true", help="skip the pytest gate at the end")
    p.add_argument("--only-tests", action="store_true", help="run only the pytest gate")
    p.add_argument("--fail-fast", action="store_true", help="stop at the first failed stage")
    p.add_argument("--budget", type=float, default=6.0, help="max USD per agent call (default 6)")
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--max-retries", type=int, default=MAX_RETRIES)
    p.add_argument("symbols", nargs="*", help="crypto symbols (default BTC/USD ETH/USD SOL/USD)")
    return p


def main(argv: list[str] | None = None) -> None:
    ns = build_parser().parse_args(argv)
    sys.exit(asyncio.run(amain(ns)))
