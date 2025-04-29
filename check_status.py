#!/usr/bin/env python

import argparse
import logging
import pandas as pd
from pathlib import Path


# Allowed status; order matters!
status_choices = ["RUNNING", "PENDING", "OK", "ERROR", "NOT_RUN"]


# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Check status of jobs.",
    allow_abbrev=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-i",
    "--job-list",
    action="store",
    type=Path,
    nargs="+",
    help="Path to stats file",
)
parser.add_argument(
    "--scheduler",
    action="store",
    default="slurm",
    help="HPC scheduler",
)
parser.add_argument(
    "--hpc-extra",
    action="store",
    default="--partition compsnake --starttime now-5days",
    help="HPC extra query arguments",
)
parser.add_argument(
    "-s",
    "--status",
    choices=status_choices,
    action="store",
    help="Status of jobs to print",
)
parser.add_argument(
    "-l",
    "--loglevel",
    action="store",
    default="INFO",
    choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    help="Log verbosity level",
)
args = parser.parse_args()


# Set logger
loglevel = getattr(logging, args.loglevel.upper(), None)
logging.basicConfig(encoding="utf-8", level=loglevel)


# Read job list
logging.info("Reading input file(s)")
df = pd.concat(
    [
        pd.read_table(job_list, index_col="id", comment="#").assign(filename=job_list)
        for job_list in args.job_list
    ]
)

n_jobs = df.shape[0]
logging.info(f"Checking {n_jobs} jobs")


# Looking up Snakemake logs
logging.info("Looking up Snakemake log")
df["snakemake_log"] = df.index.map(
    lambda x: (
        [pd.NA] + sorted((Path(x) / ".snakemake" / "log").glob("20*.*.snakemake.log"))
    ).pop()
)


logging.info("Querying HPC account manager")
if args.scheduler == "slurm":
    # Add HPC scheduler stats
    import subprocess
    from io import StringIO

    res = subprocess.run(
        [
            "sacct",
            "--allocations",
            "--parsable2",
            "--endtime",
            "now",
            "--format",
            "JobName%-500, State, End",
        ]
        + args.hpc_extra.split(" "),
        stdout=subprocess.PIPE,
    )
    res = (
        pd.read_csv(StringIO(res.stdout.decode("utf-8")), sep="|")
        .rename(columns={"JobName": "id", "State": "hpc_status", "End": "time_end"})
        .set_index("id")
        .sort_values("time_end")
    )
    # Keep only most recent job
    df = df.join(res[~res.index.duplicated(keep="last")])

    # Add scheduler status
    df["hpc_log"] = df.index.map(
        lambda x: ([pd.NA] + sorted(Path(x).glob("slurm-*"))).pop()
    )
else:
    logging.warning("No supported HPC scheduler supplied!")


# Parse snakemake status
logging.info("Add Snakemake stats")


def get_snakemake_status(log, n_lines=10):
    # Take most recent log
    if isinstance(log, Path):
        with open(log, "r") as log_fh:
            tail = [
                line
                for line in log_fh.read().splitlines()[-n_lines:]
                if line.startswith("aeDNA workflow finished")
                or line.startswith("Cleaning up log files older than ")
            ]
            return tail.pop(0) if len(tail) > 0 else pd.NA
    else:
        return pd.NA


df["snakemake_status"] = df[
    "{}_log".format("hpc" if args.scheduler else "snakemake")
].map(lambda x: get_snakemake_status(x))
logging.debug(df.iloc[0])


# Fill in missing status
logging.info("Filling in missing status")
df.loc[df.snakemake_log.isna(), "snakemake_status"] = "NOT_RUN"
df.loc[
    df.snakemake_status.eq("aeDNA workflow finished successfully!"),
    ["snakemake_status", "status"],
] = "OK"
df.loc[
    df.snakemake_status.eq("aeDNA workflow finished with an error!"),
    ["snakemake_status", "status"],
] = "ERROR"
df.loc[
    df.snakemake_status.str.startswith("Cleaning up log files older than ", na=False),
    "snakemake_status",
] = "NOT_RUNNING"
df.loc[
    df.snakemake_status.isna(),
    "snakemake_status",
] = "RUNNING"
logging.debug(df.iloc[0])


# Compare with HPC status
if args.scheduler:
    df.loc[
        df.hpc_status.ne("PENDING")
        & df.snakemake_status.isin(["NOT_RUN", "NOT_RUNNING"]),
        "status",
    ] = "NOT_RUN"
    df.loc[
        df.hpc_status.eq("PENDING")
        & df.snakemake_status.isin(["NOT_RUN", "NOT_RUNNING"]),
        "status",
    ] = "PENDING"
    df.loc[
        df.hpc_status.eq("RUNNING") & df.snakemake_status.eq("RUNNING"), "status"
    ] = "RUNNING"
    df.loc[
        (
            df.hpc_status.isin(["FAILED", "NODE_FAIL"])
            | df.hpc_status.str.startswith("CANCELLED", na=False)
        )
        & df.snakemake_status.isin(["ERROR", "NOT_RUNNING"]),
        "status",
    ] = "ERROR"
else:
    df["status"] = df["snakemake_status"]


assert (
    not df.status.isna().any()
), f"Missing status values:\n{df[df.status.isna()].iloc[0]}"


# Sort by status and remove duplicates
df["status"] = pd.Categorical(df["status"], status_choices)
df = df.sort_values("status")
df = df[~df.index.duplicated()]
logging.debug(df[["hpc_status", "snakemake_status", "status"]])
assert (
    n_jobs == df.shape[0]
), f"Number of jobs ({n_jobs}) and HPC status ({df.shape[0]}) does not match"


# Print summary
df["total_samples"] = df.groupby("filename").status.transform("count")
df_status = df.groupby(["filename", "total_samples"]).status.value_counts(sort=False)
logging.info(df_status[df_status > 0])

# Filter runs by status and print
if args.status:
    for id in df[df.status.eq(args.status)].index:
        print(id)

exit(0)
