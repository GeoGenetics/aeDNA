#!/usr/bin/env python

import socket
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
    "-w",
    "--workflow",
    action="store",
    default="prod",
    choices=["prod", "prod-legacy", "prod-test", "caterpillar"],
    help="Workflow to use",
)
parser.add_argument(
    "-t",
    "--target",
    action="store",
    default="",
    help="Workflow target.",
)
parser.add_argument(
    "-r",
    "--run",
    action="store",
    default="local",
    choices=["local", "slurm"],
    help="Where to run?",
)
parser.add_argument(
    "--submit-workflow",
    action="store_true",
    default=False,
    help="Submit workflow as a job to HPC",
)
parser.add_argument(
    "--submit-jobs",
    action="store_true",
    default=False,
    help="Submit jobs to HPC",
)
parser.add_argument(
    "--default-args",
    action="store",
    nargs="+",
    default=[
        "--configfile",
        "config/config.yaml",
        "--workflow-profile",
        "/datasets/caeg_production/resources/profile/PROD.profile.yaml",
    ],
    help="Snakemake logger command",
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
extra_args.extend(args.default_args)
assert args.run == "local" or (
    args.run != "local" and (args.submit_workflow or args.submit_jobs)
), "cannot submit jobs to local"


# Set logger
loglevel = getattr(logging, args.loglevel.upper(), None)
logging.basicConfig(
    encoding="utf-8",
    level=loglevel,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# Infer pixi_env/workflow paths, and add extra options
if args.workflow == "prod":
    pixi_env = workflow_path = "/projects/caeg/apps/aeDNA"
elif args.workflow == "prod-legacy":
    pixi_env = "/projects/caeg/apps/aeDNA"
    workflow_path = "/projects/caeg/apps/aeDNA-legacy"
elif args.workflow == "prod-test":
    pixi_env = workflow_path = "/projects/caeg/people/lnc113/workflows/aeDNA/aeDNA"
elif args.workflow == "caterpillar":
    pixi_env = workflow_path = "/projects/caeg/people/lnc113/workflows/caterpillar"


# Infer hostname, HPC account and partition
hostname = socket.gethostname()
if args.run != "local":
    if hostname.startswith("dandy"):
        hpc_snakemake_account = hpc_job_account = "prod"
        hpc_snakemake_partition = "compsnake"
        hpc_job_partition = "compregular,compdragen"
        hpc_snakemake_qos = ""
        hpc_job_qos = ""
    elif hostname.startswith("rubus"):
        hpc_snakemake_account = hpc_job_account = "bench"
        hpc_snakemake_partition = hpc_job_partition = "rubus"
        hpc_snakemake_qos = "long"
        hpc_job_qos = "normal"
    else:
        logging.error(f"Host {hostname} not supported yet!")
        exit(-1)


# Workflow run command
if args.submit_workflow:
    cmd = f"sbatch --chdir {{id}} --job-name {{id}} --account {hpc_snakemake_account} --partition {hpc_snakemake_partition}"
    if hpc_snakemake_qos:
        cmd += f" --qos {hpc_snakemake_qos}"
    cmd += " --cpus-per-task 1 --mem 1G --time 5-00 --no-requeue --wrap="
    logging.info(f"Workflows will be submitted to the {args.run} HPC with:\n{cmd}")
else:
    cmd = "env --chdir={id} bash -c "
    logging.info(f"Workflows will be run locally on host {hostname}")


# Submit jobs
if args.submit_jobs:
    extra_args.extend(["--executor", args.run, "--slurm-jobname-prefix", "PROD_"])
    if hpc_job_qos:
        extra_args.append(f"--slurm-qos {hpc_job_qos}")
    logging.info(f"Jobs will be submitted to the {args.run} HPC with {extra_args}")
else:
    logging.info(f"Jobs will be run locally on host {hostname} with {extra_args}")


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


# Print command
logging.info("Build command")

extra_args = " ".join(extra_args)

for id in df.index:
    print(
        f'{cmd.format(id=id)}"pixi run --manifest-path {pixi_env} snakemake {args.target} --snakefile {workflow_path}/workflow/Snakefile {extra_args.format(id=id)}"; sleep 0.5'
    )

exit(0)
