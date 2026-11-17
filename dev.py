#!/usr/bin/env python3
"""Developer commands for delta-force. Run `./dev --help` to list them."""
import argparse
import shutil
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


def profile(args: argparse.Namespace) -> int:
    """Record a CPU profile of the benchmark with samply and open it."""
    if shutil.which("samply") is None:
        print("samply is not installed: cargo install --locked samply", file=sys.stderr)
        return 1
    # Release with debug symbols, so the profile shows function names.
    built = run(
        "cargo", "build", "--profile", "profiling", "--features", "test-util",
        "--example", "ingest_1gb",
    )
    if built != 0:
        return built
    exe = ROOT / "target" / "profiling" / "examples" / "ingest_1gb"
    if sys.platform == "win32":
        exe = exe.with_suffix(".exe")
    output = ROOT / "target" / "bench" / "profile.json.gz"
    output.parent.mkdir(parents=True, exist_ok=True)
    if sys.platform == "win32":
        print(
            "note: on Windows samply needs an administrator terminal and xperf, from the "
            "Windows Performance Toolkit (in the Windows ADK installer)",
            flush=True,
        )
    # The .syms.json sidecar keeps function names next to the profile, so it
    # can be read later without the build that produced it.
    return run(
        "samply", "record", "--unstable-presymbolicate", "--output", str(output),
        str(exe), args.size,
    )


def main() -> int:
    parser = argparse.ArgumentParser(prog="dev", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    bench = commands.add_parser("benchmark", help=benchmark.__doc__, description=benchmark.__doc__)
    bench.add_argument("size", nargs="?", default="1GB", help="source file size, e.g. 100MB (default: 1GB)")
    bench.set_defaults(run=benchmark)

    prof = commands.add_parser("profile", help=profile.__doc__, description=profile.__doc__)
    prof.add_argument("size", nargs="?", default="1GB", help="source file size, e.g. 100MB (default: 1GB)")
    prof.set_defaults(run=profile)

    args = parser.parse_args()
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
