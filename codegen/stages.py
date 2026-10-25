"""Stage registry for the Claude Agent SDK codegen.

Each Stage describes ONE generation step: which file(s) the Coder must write,
which earlier files it is shown as context (priors), and what the
deterministic verifier checks afterwards. Later stages see earlier ones, so
the order below is the generation order.

Fields
------
name        short id used by --stage.
targets     glob patterns (relative to generated/) the Coder is ALLOWED to
            write. The PreToolUse hook denies any Write/Edit outside them.
            The first pattern is also the "primary" file reported in logs.
kind        verifier dispatch: python | html | markdown | pytest_unit | pytest_live
prompt_file file under codegen/prompts/ with the stage-specific brief.
priors      files (relative to generated/) pasted into the prompt as
            `### EXISTING:` blocks. Keep this list small: Opus handles it,
            but a tight context keeps the Coder focused on the contract.
must_define top-level names that must exist after `import <module>`.
contract    extra checks: for webgui, Flask url rules that must exist; for
            html, substrings that must appear (the /api URLs it calls).
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Stage:
    name: str
    targets: tuple[str, ...]
    kind: str
    prompt_file: str
    priors: tuple[str, ...] = ()
    must_define: tuple[str, ...] = ()
    contract: tuple[str, ...] = ()
    max_budget_usd: float = 6.0
    max_turns: int = 40

    @property
    def primary(self) -> str:
        return self.targets[0]

    @property
    def module(self) -> str | None:
        p = self.primary
        return p[:-3].replace("/", ".") if p.endswith(".py") and "*" not in p else None


WEBGUI_ROUTES = (
    "/", "/api/symbols", "/api/orderbook/<path:symbol>", "/api/latest/<path:symbol>",
    "/api/orders", "/api/fills", "/api/summary", "/api/order", "/api/order/<order_id>",
    "/api/algo", "/api/algos", "/api/algo/<algo_id>", "/api/algos/strategies", "/api/health",
)

# Substrings the frontend must contain (the endpoints its JS calls).
HTML_CONTRACT = (
    "/api/symbols", "/api/orderbook/", "/api/orders", "/api/fills", "/api/summary",
    "/api/order", "/api/algo", "/api/algos", "/api/algos/strategies", "/api/health",
    "id=\"tab-algos\"", "id=\"tab-escalator\"", "id=\"tab-activity\"", "id=\"tab-summary\"",
)

STAGES: list[Stage] = [
    Stage("research", ("RESEARCH.md",), "markdown", "researcher.md",
          max_budget_usd=4.0, max_turns=30),
    Stage("runtime", ("runtime.py",), "python", "runtime.md",
          must_define=("get_redis", "redis_call", "run_forever", "Heartbeat",
                       "sqlite_connect", "log", "new_client_order_id",
                       "send_command", "wait_reply",
                       "STREAM_TRADE_UPDATES", "STREAM_OMS_COMMANDS",
                       "STREAM_EMS_REQUESTS", "TERMINAL_STATUSES",
                       "DB_PATH", "REDIS_URL", "heartbeat_key", "reply_key")),
    Stage("subscriber", ("subscriber.py",), "python", "subscriber.md",
          priors=("runtime.py",), must_define=("main",)),
    Stage("trader", ("trader.py",), "python", "trader.md",
          priors=("runtime.py",),
          must_define=("get_client", "submit_limit", "submit_market", "cancel",
                       "get_order", "get_order_by_client_id", "open_orders",
                       "get_asset_rules", "latest_quote", "account", "positions",
                       "_to_dict")),
    Stage("trade_stream", ("trade_stream.py",), "python", "trade_stream.md",
          priors=("runtime.py", "trader.py"), must_define=("on_trade_update", "main")),
    Stage("oms", ("oms.py",), "python", "oms.md",
          priors=("runtime.py", "trader.py", "trade_stream.py"),
          must_define=("setup_db", "handle_payload", "handle_command", "forward_to_ems", "reconcile",
                       "main", "SCHEMA")),
    Stage("cli", ("cli.py",), "python", "cli.md",
          priors=("runtime.py", "trader.py", "oms.py"), must_define=("main",)),
    Stage("algos", ("algos.py",), "python", "algos.md",
          priors=("runtime.py",),
          must_define=("Strategy", "TWAP", "VWAP", "ImplementationShortfall",
                       "REGISTRY", "available_strategies", "round_qty", "round_price",
                       "Slice", "build_schedule")),
    Stage("ems", ("ems.py",), "python", "ems.md",
          priors=("runtime.py", "trader.py", "algos.py", "oms.py"),
          must_define=("setup_db", "handle_request", "execute_submit", "execute_cancel", "publish_report",
                       "refresh_from_oms", "tick", "main", "SCHEMA")),
    Stage("oms_ems", ("oms_ems.py", "tests/test_unit_oms_ems.py"), "python", "oms_ems.md",
          priors=("runtime.py", "oms.py", "ems.py", "tests/conftest.py", "tests/test_unit_runtime.py"),
          must_define=("WORKER_NAMES", "run_oms", "run_ems", "start_workers", "main"),
          max_budget_usd=4.0),
    Stage("webgui_html", ("web/index.html",), "html", "webgui_html.md",
          contract=HTML_CONTRACT, max_budget_usd=8.0),
    Stage("webgui", ("webgui.py",), "python", "webgui.md",
          priors=("runtime.py", "trader.py", "oms.py", "ems.py", "web/index.html"),
          must_define=("app", "main"), contract=WEBGUI_ROUTES),
    Stage("tests_unit", ("tests/conftest.py", "tests/test_unit_*.py", "tests/__init__.py",
                         "pytest.ini"),
          "pytest_unit", "tests_unit.md",
          priors=("runtime.py", "trader.py", "algos.py", "oms.py", "ems.py"),
          max_budget_usd=8.0, max_turns=60),
    Stage("tests_live", ("tests/test_live_*.py", "tests/conftest.py"),
          "pytest_live", "tests_live.md",
          priors=("runtime.py", "trader.py", "oms.py", "ems.py", "oms_ems.py", "tests/conftest.py"),
          max_budget_usd=8.0, max_turns=60),
]

STAGE_BY_NAME: dict[str, Stage] = {s.name: s for s in STAGES}

# Python modules in generation order (used by the import check and by the
# "re-verify dependents" step after a --fix).
PY_MODULES = [s.module for s in STAGES if s.module]


def dependents_of(stage_name: str) -> list[Stage]:
    """Stages that list any of `stage_name`'s targets as a prior."""
    st = STAGE_BY_NAME[stage_name]
    return [s for s in STAGES if any(t in s.priors for t in st.targets)]
