#!/usr/bin/env python

import argparse
import logging
import pandas as pd
from pathlib import Path


# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Launch runs.",
    allow_abbrev=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-i",
    "--job-list",
    action="store",
    type=Path,
    nargs="+",
    default=["/dev/stdin"],
    help="Path to stats file",
)
parser.add_argument(
    "--workflow",
    action="store",
    choices=["prod", "prod-legacy", "prod-test", "caterpillar"],
    default="prod",
    help="Workflow to use",
)
parser.add_argument(
    "--scheduler",
    action="store",
    default="slurm",
    help="HPC scheduler",
)
parser.add_argument(
    "-l",
    "--loglevel",
    action="store",
    default="INFO",
    choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    help="Log verbosity level",
)
args, extra_args = parser.parse_known_args()
extra_args = " ".join(extra_args)


# Set logger
loglevel = getattr(logging, args.loglevel.upper(), None)
logging.basicConfig(
    encoding="utf-8",
    level=loglevel,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# Read job list
logging.info("Reading input file(s)")
df = pd.concat(
    [
        pd.read_table(
            job_list, header=None, index_col=0, usecols=[0], comment="#"
        ).assign(filename=job_list)
        for job_list in args.job_list
    ]
)


n_jobs = df.shape[0]
logging.info(f"Launching {n_jobs} jobs")
logging.debug(df)


logging.info("Build base command")
if args.workflow == "prod":
    pixi_path = workflow_path = "/projects/caeg/apps/aeDNA"
elif args.workflow == "prod-legacy":
    pixi_path = "/projects/caeg/apps/aeDNA"
    workflow_path = "/projects/caeg/apps/aeDNA-legacy"
elif args.workflow == "prod-test":
    pixi_path = workflow_path = "/projects/caeg/people/lnc113/workflows/aeDNA/aeDNA"
elif args.workflow == "caterpillar":
    pixi_path = workflow_path = "/projects/caeg/people/lnc113/workflows/caterpillar"

cmd = f"pixi run --manifest-path {pixi_path} snakemake --snakefile {workflow_path}/workflow/Snakefile --workflow-profile /projects/caeg/data/resources/profile {extra_args}"


# Print command
for id in df.index:
    if args.scheduler == "":
        print(f"env --chdir={id} {cmd}")
    elif args.scheduler == "slurm":
        print(
            f'sbatch --chdir {id} --job-name {id} --account prod --partition compsnake --cpus-per-task 1 --mem 1G --time 5-00 --no-requeue --wrap "{cmd} --profile /projects/caeg/data/resources/profile_production --executor slurm --retries 1"; sleep 1'
        )
    else:
        logging.warning(f"HPC scheduler {args.scheduler} not supported!")

exit(0)
