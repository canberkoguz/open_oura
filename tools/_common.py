"""Shared helpers for the tools/ model runners (run_models, run_activity_model,
run_sleep_model). Imported as a sibling module — each runner's directory is on
sys.path[0] when invoked as `python tools/run_*.py`.
"""
import sys
from pathlib import Path


def resolve_db(arg, repo):
    """Resolve the SQLite events DB path.

    Explicit ``arg`` wins; otherwise pick the first existing default among
    ./oura.db, repo/oura.db, repo/captures/ring5.db, falling back to
    repo/oura.db. Exit with a clear error if the resolved DB is missing.
    """
    if arg:
        db = Path(arg)
    else:
        db = next(
            (c for c in (Path.cwd() / "oura.db", repo / "oura.db",
                         repo / "captures" / "ring5.db") if c.exists()),
            repo / "oura.db",
        )
    if not db.exists():
        sys.exit(f"error: database not found: {db} (run `oura sync` first)")
    return db


RUNNER_VERSION = 1


def emit_json(payload):
    """Print a runner payload with the version stamp every consumer checks."""
    import json
    import sys
    print(json.dumps({"runner_version": RUNNER_VERSION, **payload}, indent=2))
    sys.stdout.flush()


def fail_no_data(kind, message):
    """Exit 3 with a structured diagnosis of missing input.

    Distinct from a crash on purpose: a consumer can tell "this database has no
    MET events" from "the model blew up", and say so, instead of showing a
    traceback for a condition the user can fix.
    """
    import json
    import sys
    print(json.dumps({"runner_version": RUNNER_VERSION,
                      "error": {"kind": kind, "message": message}}))
    raise SystemExit(3)
