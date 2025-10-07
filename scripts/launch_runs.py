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
    "-w",
    "--workflow",
    action="store",
    default="prod",
    choices=["prod", "prod-legacy", "prod-test", "caterpillar"],
    help="Workflow to use",
)
parser.add_argument(
    "-r",
    "--run",
    action="store",
    default="local",
    choices=["local", "slurm"],
    help="How to run jobs?",
)
parser.add_argument(
    "--submit-snakemake",
    action="store_true",
    default=False,
    help="Submit Snakemake as an HPC job",
)
parser.add_argument(
    "--tmp-dir",
    action="store",
    default="/projects/caeg/scratch/production/",
    help="Path to temp folder",
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


# Infer Pixi env and workflow paths
if args.workflow == "prod":
    pixi_env = workflow_path = "/projects/caeg/apps/aeDNA"
    extra_args += " --profile /projects/caeg/data/resources/profile_production"
elif args.workflow == "prod-legacy":
    pixi_env = "/projects/caeg/apps/aeDNA"
    workflow_path = "/projects/caeg/apps/aeDNA-legacy"
elif args.workflow == "prod-test":
    pixi_env = workflow_path = "/projects/caeg/people/lnc113/workflows/aeDNA/aeDNA"
elif args.workflow == "caterpillar":
    pixi_env = workflow_path = "/projects/caeg/people/lnc113/workflows/caterpillar"


# Infer account/partition
import socket
hostname = socket.gethostname()
if hostname.startswith("dandy"):
    hpc_snakemake_account = hpc_job_account = "prod"
    hpc_job_partition = "compregular"
    hpc_snakemake_partition = "compsnake"
elif hostname.startswith("rubus"):
    hpc_snakemake_account = hpc_job_account = "bench"
    hpc_snakemake_partition = "epyc_long,xeon_fat_long"
    hpc_job_partition = "epyc,epyc_noht,xeon_fat"
else:
    logging.error(f"Host {hostname} not supported yet!")
    exit(-1)


# Infer hostname, HPC account and partition
opts = "--jobs 300 --retries 1"
if args.run == "local":
    logging.info(f"Running jobs locally")
    opts += f" --default-resources tmpdir='{args.tmp_dir}'"
elif args.run == "slurm":
    logging.info(f"Jobs will be submitted to the {args.run} HPC, on account '{hpc_job_account}' and partition '{hpc_job_partition}'.")
    opts += f" --executor {args.run} --default-resources slurm_account={hpc_job_account} slurm_partition={hpc_job_partition} tmpdir='{args.tmp_dir}'"


# Submit snakemake process
if args.submit_snakemake:
    cmd = f"sbatch --chdir {{id}} --job-name {{id}} --account {hpc_snakemake_account} --partition {hpc_snakemake_partition} --cpus-per-task 1 --mem 1G --time 5-00 --no-requeue --wrap="
    logging.info(f"Workflows will be submitted to the {args.run} HPC, on account '{hpc_snakemake_account}' and partition '{hpc_snakemake_partition}'.")
else:
    cmd = f"env --chdir={{id}} bash -c "
    logging.info(f"Workflows will be run locally on host {hostname}")


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

for id in df.index:
    print(f'{cmd.format(id=id)}"pixi run --manifest-path {pixi_env} snakemake --snakefile {workflow_path}/workflow/Snakefile --workflow-profile /projects/caeg/data/resources/profile {extra_args} {opts}"; sleep 0.5')

exit(0)
