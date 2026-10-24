#!/usr/bin/env python
"""Entry point: python claude_codegen.py [--stage ...] [--fix err.txt --stage X] [SYMBOLS...]

See codegen/driver.py for the orchestration and codegen/prompts/ for the briefs.
Requires ANTHROPIC_API_KEY and `pip install -r requirements-codegen.txt`.
"""
from codegen.driver import main

if __name__ == "__main__":
    main()
