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
    "--snakemake-submit",
    action="store_true",
    default=False,
    help="Submit Snakemake as an HPC job",
)
parser.add_argument(
    "--snakemake-logger",
    action="store",
    default="--logger snkmt --logger-snkmt-db snkmt.db",
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
extra_args = " ".join(extra_args)
extra_args += " --jobs 300 --retries 1"


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
    import os

    if os.environ.get("CAEG_QC_USER") and os.environ.get("CAEG_QC_PASSWORD"):
        # extra_args += """ --config 'report={multiqc_db_url: "postgresql+psycopg2://dandypdb01fl.unicph.domain:5432/caeg_qc"}'"""
        extra_args += " --profile /projects/caeg/data/resources/profile_production"
elif args.workflow == "prod-legacy":
    pixi_env = "/projects/caeg/apps/aeDNA"
    workflow_path = "/projects/caeg/apps/aeDNA-legacy"
elif args.workflow == "prod-test":
    pixi_env = workflow_path = "/projects/caeg/people/lnc113/workflows/aeDNA/aeDNA"
elif args.workflow == "caterpillar":
    pixi_env = workflow_path = "/projects/caeg/people/lnc113/workflows/caterpillar"


# Infer hostname, HPC account and partition
import socket

hostname = socket.gethostname()
if hostname.startswith("dandy"):
    hpc_snakemake_account = hpc_job_account = "prod"
    hpc_job_partition = "compregular"
    hpc_snakemake_partition = "compsnake"
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
if args.snakemake_submit:
    logging.info(f"Workflows will be submitted to the {args.run} HPC, on:")
    cmd = f"sbatch --chdir {{id}} --job-name {{id}}"
    if hpc_snakemake_account:
        logging.info(f"  - account: {hpc_snakemake_account}")
        cmd += f" --account {hpc_snakemake_account}"
    if hpc_snakemake_partition:
        logging.info(f"  - partition: {hpc_snakemake_partition}")
        cmd += f" --partition {hpc_snakemake_partition}"
    if hpc_snakemake_qos:
        logging.info(f"  - qos: {hpc_snakemake_qos}")
        cmd += f" --qos {hpc_snakemake_qos}"
    cmd += f" --cpus-per-task 1 --mem 1G --time 5-00 --no-requeue --wrap="
else:
    logging.info(f"Workflows will be run locally on host {hostname}")
    cmd = f"env --chdir={{id}} bash -c "


# Jobs run command
if args.run == "local":
    logging.info(f"Running jobs locally")
elif args.run == "slurm":
    logging.info(
        f"Jobs will be submitted to the {args.run} HPC, on account '{hpc_job_account}' and partition '{hpc_job_partition}'."
    )
    extra_args += (
        f" --executor {args.run} --default-resources slurm_account={hpc_job_account} slurm_partition={hpc_job_partition}"
        + (f" --slurm-qos {hpc_job_qos}" if hpc_job_qos else "")
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


# Print command
logging.info("Build command")

for id in df.index:
    print(
        f'{cmd.format(id=id)}"pixi run --manifest-path {pixi_env} snakemake --snakefile {workflow_path}/workflow/Snakefile --workflow-profile /projects/caeg/data/resources/profile {args.snakemake_logger} {extra_args}"; sleep 0.5'
    )

exit(0)
