#!/usr/bin/env python

import argparse
import os
import sys
import subprocess
import signal
import shlex
import resource
from pathlib import Path


basedir = Path(__file__).parent

# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Generic snakemake wrapper",
    allow_abbrev=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-d",
    "--workdir",
    "--directory",
    action="store",
    type=Path,
    help="Path to working dir.",
)
parser.add_argument(
    "--target",
    action="store",
    nargs="*",
    default=[""],
    help="Snakemake target rules or files.",
)
parser.add_argument("--snakeface", action="store", default="", help="Snakeface token.")
parser.add_argument(
    "--cache-dir",
    action="store",
    default=Path.home() / ".cache" / "snakemake",
    type=Path,
    help="Path for cache storage (both conda and workflow's).",
)

args, extra_args = parser.parse_known_args()
extra_args = " ".join(extra_args)


# Set workdir
if args.workdir:
    Path(args.workdir).mkdir(parents=True, exist_ok=True)
    os.chdir(args.workdir)

# Build Snakemake command
args.target = " ".join(args.target)
command = f"snakemake {args.target}"

for key in ["place_holder"]:
    value = getattr(args, key, False)
    if value:
        command += f" --{key.replace('_','-')} {value.resolve()}"

if args.cache_dir:
    conda_cache = args.cache_dir / "conda"
    command += f" --use-conda --conda-prefix {conda_cache}"

    workflow_cache = args.cache_dir / "workflow"
    workflow_cache.mkdir(parents=True, exist_ok=True)
    os.environ["SNAKEMAKE_OUTPUT_CACHE"] = str(workflow_cache)

if args.snakeface:
    command += " --wms-monitor http://127.0.0.1:5555"
    os.environ["WMS_MONITOR_TOKEN"] = args.snakeface


if extra_args:
    command += f" {extra_args}"


# Print the full command line
print(f"----------\n{command}\n", file=sys.stderr)
sys.stderr.flush()


# Run pipeline
try:
    # Increase max number of open files
    limit_nofile_soft, limit_nofile_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if limit_nofile_soft < 10240 and limit_nofile_hard > 10240:
        resource.setrlimit(resource.RLIMIT_NOFILE, (10240, limit_nofile_hard))
    # Run workflow
    process = subprocess.Popen(
        shlex.split(command), stdout=sys.stdout, stderr=sys.stderr
    )
    out = process.communicate()
except KeyboardInterrupt:
    process.send_signal(signal.SIGINT)
    process.wait()
