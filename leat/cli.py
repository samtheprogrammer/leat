"""leat command-line interface.

Operate leat pipelines without writing a runner script. The user's Python file
calls ``lt = leat.connect(...)`` and defines ``@lt.model(...)`` functions; the CLI
imports that file, finds the resulting :class:`leat.pipeline.Session`, and drives
its registered models.

    leat run <file.py> [--model NAME] [--once] [--loop]
    leat status <file.py>
    leat reset <file.py> --model NAME [--to earliest|latest|<offset>]
    leat --version
    leat --help

Also runnable as ``python -m leat.cli``.
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import uuid
from typing import List, Optional

import leat
from leat.pipeline import Session


# --------------------------------------------------------------------------- #
# user-file loading + model discovery
# --------------------------------------------------------------------------- #
def _load_module(path: str):
    """Import a user's .py file as a module and return it."""
    abspath = os.path.abspath(path)
    if not os.path.isfile(abspath):
        raise FileNotFoundError(f"no such file: {path}")
    mod_name = "leat_user_" + uuid.uuid4().hex
    spec = importlib.util.spec_from_file_location(mod_name, abspath)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    # make sibling imports / __file__ behave from the file's own directory
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


def _find_sessions(module) -> List[Session]:
    """All Session objects living in the module's globals (in definition order)."""
    seen = []
    for value in vars(module).values():
        if isinstance(value, Session) and value not in seen:
            seen.append(value)
    return seen


def _load_sessions(path: str) -> List[Session]:
    module = _load_module(path)
    sessions = _find_sessions(module)
    if not sessions:
        raise SystemExit(
            f"error: no leat Session found in {path} "
            f"(does it call leat.connect(...)?)"
        )
    return sessions


def _all_models(sessions: List[Session]) -> "dict[str, object]":
    """Merge every session's registered models, name -> Model (first wins on clash)."""
    models: dict = {}
    for s in sessions:
        for name, m in s._models.items():
            models.setdefault(name, m)
    return models


def _pipeline(model):
    return model._p


# --------------------------------------------------------------------------- #
# subcommands
# --------------------------------------------------------------------------- #
def cmd_run(args) -> int:
    sessions = _load_sessions(args.file)
    models = _all_models(sessions)
    once = bool(args.once) and not args.loop  # --loop wins if both somehow given

    if args.model:
        if args.model not in models:
            print(f"error: no model named {args.model!r}. "
                  f"known: {', '.join(models) or '(none)'}", file=sys.stderr)
            return 2
        targets = {args.model: models[args.model]}
    else:
        targets = models

    if not targets:
        print("error: no models registered in file", file=sys.stderr)
        return 2

    for name, model in targets.items():
        mode = "once" if once else "loop"
        print(f"[leat] running {name} ({mode})")
        model.run(once=once)
    return 0


def _format_table(headers: List[str], rows: List[List[str]]) -> str:
    cols = [headers] + rows
    widths = [max(len(str(r[i])) for r in cols) for i in range(len(headers))]
    def fmt(row):
        return "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row))
    line = "  ".join("-" * w for w in widths)
    out = [fmt(headers), line]
    out += [fmt(r) for r in rows]
    return "\n".join(out)


def cmd_status(args) -> int:
    sessions = _load_sessions(args.file)
    models = _all_models(sessions)
    if not models:
        print("(no models registered)")
        return 0

    headers = ["model", "source", "sink", "position", "lag"]
    rows = []
    for name, model in models.items():
        p = _pipeline(model)
        source = getattr(p.src, "_id", str(p.src))
        sink = getattr(p.snk, "_id", str(p.snk))
        try:
            pos = p.position()
        except Exception as e:  # pragma: no cover - defensive
            pos = f"err:{e}"
        pos_str = "earliest" if (pos is None or (isinstance(pos, int) and pos < 0)) else str(pos)
        try:
            lag = p.lag()
        except Exception as e:  # pragma: no cover - defensive
            lag = f"err:{e}"
        rows.append([name, source, sink, pos_str, str(lag)])

    print(_format_table(headers, rows))
    return 0


def _parse_to(value: str):
    """--to earliest|latest|<int>  ->  ('earliest'|'latest'|int)."""
    if value in ("earliest", "latest"):
        return value
    try:
        return int(value)
    except ValueError:
        raise SystemExit(f"error: --to must be 'earliest', 'latest', or an integer, got {value!r}")


def cmd_reset(args) -> int:
    sessions = _load_sessions(args.file)
    models = _all_models(sessions)
    if args.model not in models:
        print(f"error: no model named {args.model!r}. "
              f"known: {', '.join(models) or '(none)'}", file=sys.stderr)
        return 2

    model = models[args.model]
    p = _pipeline(model)
    consumer = p.consumer
    target = _parse_to(args.to)

    before = p.position()
    if target == "earliest":
        consumer.reset()                       # offset -> None (full reprocess)
    elif target == "latest":
        consumer.seek(p.src.latest_offset())
    else:
        consumer.seek(target)

    # persist the new offset into the checkpoint store (its 'consumer group')
    _persist(consumer)

    after = p.position()
    print(f"[leat] reset {args.model}: position {_show(before)} -> {_show(after)}")
    return 0


def _show(v) -> str:
    return "earliest" if (v is None or (isinstance(v, int) and v < 0)) else str(v)


def _persist(consumer) -> None:
    """Write the consumer's current in-memory offset to the checkpoint store.

    ``commit()`` only persists a *pending* (post-poll) offset; after seek/reset we
    must write the live ``_offset`` directly so ``status``/subsequent runs see it.
    """
    consumer._ckpt.set(consumer._name, consumer._offset)


# --------------------------------------------------------------------------- #
# argument parser
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="leat",
        description="Operate leat incremental-ETL pipelines from the command line.",
    )
    parser.add_argument("--version", action="version",
                        version=f"leat {getattr(leat, '__version__', 'unknown')}")
    sub = parser.add_subparsers(dest="command", metavar="<command>")

    p_run = sub.add_parser("run", help="import a file and run its model(s)")
    p_run.add_argument("file", help="path to the user's Python pipeline file")
    p_run.add_argument("--model", help="run only this model (default: run all)")
    p_run.add_argument("--once", action="store_true",
                       help="process a single batch then exit (DAG task mode)")
    p_run.add_argument("--loop", action="store_true",
                       help="run the continuous incremental loop (default)")
    p_run.set_defaults(func=cmd_run)

    p_status = sub.add_parser("status", help="show each model's position and lag")
    p_status.add_argument("file", help="path to the user's Python pipeline file")
    p_status.set_defaults(func=cmd_status)

    p_reset = sub.add_parser("reset", help="reset a model's committed offset")
    p_reset.add_argument("file", help="path to the user's Python pipeline file")
    p_reset.add_argument("--model", required=True, help="model whose offset to reset")
    p_reset.add_argument("--to", default="earliest",
                         help="earliest | latest | <offset>  (default: earliest)")
    p_reset.set_defaults(func=cmd_reset)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
