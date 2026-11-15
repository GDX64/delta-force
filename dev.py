#!/usr/bin/env python3
"""Developer commands for delta-force. Run `./dev --help` to list them."""
import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run(*command: str) -> int:
    print("$", " ".join(command), flush=True)
    return subprocess.call(command, cwd=ROOT)


def benchmark(args: argparse.Namespace) -> int:
    """Ingest a generated CSV and report time, CPU and peak memory."""
    return run(
        "cargo", "run", "--release", "--features", "test-util",
        "--example", "ingest_1gb", "--", args.size,
    )


def main() -> int:
    parser = argparse.ArgumentParser(prog="dev", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    bench = commands.add_parser("benchmark", help=benchmark.__doc__, description=benchmark.__doc__)
    bench.add_argument("size", nargs="?", default="1GB", help="source file size, e.g. 100MB (default: 1GB)")
    bench.set_defaults(run=benchmark)

    args = parser.parse_args()
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
