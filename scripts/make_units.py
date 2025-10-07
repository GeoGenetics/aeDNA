#!/usr/bin/env python

import argparse
import sys
import gzip
import re
import logging
import numpy as np
import pandas as pd
from pathlib import Path


def gzip_n_lines(in_gzip):
    i = 0
    with gzip.open(in_gzip, "rb") as f:
        for i, l in enumerate(f):
            pass
    return i + 1


# Default adapters
adapters = {
    "ss": "AGATCGGAAGAGCACACGTCTGAACTCCAGTCA,GGAAGAGCGTCGTGTAGGGAAAGAGTGT",
    "ds": "AGATCGGAAGAGCACACGTCTGAACTCCAGTCA,AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT",
}


# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Generate `sample` and `unit` files from list of input files.",
    allow_abbrev=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-i",
    "--in-folder",
    action="store",
    type=Path,
    help="Folder with input files",
)
parser.add_argument(
    "--in-regex",
    action="store",
    type=str,
    default="^(?!Undetermined).*.(sam|bam|fastq|fq)(.gz)?$",
    help="Regex to filter input files",
)
parser.add_argument(
    "-r",
    "--regex",
    action="store",
    default=r"\/(?P<date>\d{8})_(?P<machine>A\d{5})_(?P<run_n>\d{4})_(?P<flowcell_pos>[AB])(?P<flowcell>H[A-Z0-9]{8})(_(?P<pool_tag>[A-Z0-9]+))?(_\w+)?\/(?P<project>[^\/]+)\/(?P<library>LV\d{10})-(?P<subsample>[^_-]+)-(?P<archive>[^_]+)_(?P<sample_n>S\d+)_(?P<lane>L\d{3})_(?P<read>R[12])_001",
    help="Regex to extract identifiers. For help, see: https://docs.python.org/3/library/re.html#regular-expression-syntax",
)
parser.add_argument(
    "--min-file-size",
    action="store",
    type=int,
    default=100,
    help="Minimum file size (KiB) for detailed file info.",
)
parser.add_argument(
    "--rm-chars",
    action="store",
    default=r"[\s\-_\/\\]",
    help="Invalid characters (will be removed).",
)
parser.add_argument(
    "-c",
    "--condition",
    action="store",
    default="ancient",
    choices=["modern", "ancient"],
    help="Material type.",
)
parser.add_argument(
    "--library-type",
    action="store",
    default=np.nan,
    choices=["ss", "ds"],
    help="Library type.",
)
parser.add_argument(
    "--adapters",
    action="store",
    default=np.nan,
    help="Adapters (comma-separated).",
)
parser.add_argument(
    "--metadata-default",
    action="store",
    nargs="+",
    default=[
        "sample=Lib",
        "material=DNA",
        "flowcell_pos=X",
        "flowcell=HXXXXXXXX",
        "lane=L001",
        "platform=ILLUMINA",
        "center=CAEG",
    ],
    help="Default metadata. These values are used, in case the are not exracted from other sources (e.g. input regex or read name).",
)
parser.add_argument(
    "--out-path",
    action="store",
    default="{library[0]}{library[1]}{library[2]}/{library[3]}{library[4]}{library[5]}/{library[6]}{library[7]}{library[8]}/{library}/{date:%Y%m%d}_{flowcell_pos}{flowcell}/{workflow_ver}/{extra_file_md5}/config",
    help="Output folder structure.",
)
parser.add_argument(
    "--extra-file",
    action="store",
    type=str,
    default="config.yaml:/projects/caeg/data/resources/config/PROD.latest.config.yaml",
    help="Extra file (e.g. config.yaml) to be copied to the final folder.",
)
parser.add_argument(
    "--force",
    action="store_true",
    default=False,
    help=argparse.SUPPRESS,
)
parser.add_argument(
    "--out-stats",
    action="store",
    type=Path,
    help="File path to stats file.",
)
parser.add_argument(
    "-l",
    "--loglevel",
    action="store",
    default="WARNING",
    choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    help="Log verbosity level",
)
parser.add_argument(
    "-n",
    "--dryrun",
    action="store_true",
    default=False,
    help="Dry run",
)
args = parser.parse_args()


####################
### ARGS PROCESS ###
####################
if args.in_folder:
    # Read all files from input folder
    in_files = args.in_folder.glob("*.*")
else:
    # If no input folder, read from STDIN
    in_files = [
        Path(in_file.strip())
        for in_file in sys.stdin.readlines()
        if not in_file.startswith("#")
    ]

# Apply regex filter to input files
in_files = [
    in_file
    for in_file in in_files
    if in_file.is_file() and re.search(args.in_regex, in_file.name)
]


# Extra file
if args.extra_file.find(":") > 0:
    extra_file_name, ori_name = args.extra_file.split(":", 1)
    args.extra_file = Path(ori_name)
else:
    args.extra_file = Path(args.extra_file)
    extra_file_name = args.extra_file.name
assert args.extra_file.exists(), "Extra file does not exist."


# Add extra metadata
if args.library_type and np.isnan(args.adapters):
    args.adapters = adapters.get(args.library_type, np.nan)
for key in ["library_type", "adapters"]:
    if value := getattr(args, key):
        args.metadata_default.append(f"{key}={value}")


# Define output path wildcards (groupings)
from string import Formatter

out_path_wildcards = list(
    set([key.split("[")[0] for _, key, _, _ in Formatter().parse(args.out_path) if key])
)


###########
### LOG ###
###########
loglevel = getattr(logging, args.loglevel.upper(), None)
logging.basicConfig(
    encoding="utf-8",
    level=loglevel,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.info(f"Found {len(in_files)} input files")


###################
### PARSE INPUT ###
###################
units = pd.DataFrame()
for in_file in sorted(in_files):
    row = dict()
    row["data"] = str(in_file.resolve(strict=True))
    # Add data size
    row["size_kb"] = round(Path(row["data"]).stat().st_size / 1024, 1)
    # Add number of reads
    row["n_reads"] = (
        int(gzip_n_lines(row["data"]) / 4)
        if row["size_kb"] < args.min_file_size
        else pd.NA
    )
    # Get metadata from regexp
    matches = re.search(args.regex, row["data"])
    if not matches:
        logging.warning(f"cannot match regex to sample {row['data']}. Skipping...")
        continue
    for key, value in matches.groupdict().items():
        # Remove leading zeros
        if key in ["run_n"]:
            value = value.lstrip("0")
        logging.debug(f"adding {key} to metadata: {value}")
        assert (
            key not in row or row[key] == value
        ), f"{key} metadata does not match: {value} != {row[key]}."
        row[key] = value
        # Replace read info with generic placeholder
        if key == "read":
            read_pos = matches.span("read")
            row["data"] = (
                row["data"][: read_pos[0]] + "R{Read}" + row["data"][read_pos[1] :]
            )

    # Add row to DF
    row = pd.DataFrame([row])
    logging.debug(f"\n{row.iloc[0]}")
    units = pd.concat([units, row])


if units.shape[0] == 0:
    logging.warning("No valid data files found!")
    exit(0)


# Add metadata
for metadata_default in args.metadata_default:
    if metadata_default.find("=") > 0:
        key, value = metadata_default.split("=")
        if key not in units:
            units[key] = value


######################
### FORMAT COLUMNS ###
######################
# Format date column (if present)
if "date" in units.columns.values:
    units["date"] = pd.to_datetime(units["date"])


# Fix invalid values
fix_cols = units.columns.drop("data")
units[fix_cols] = units[fix_cols].replace(args.rm_chars, value="", regex=True)


# Fix seq_type info and collapse
if "read" in units:
    del units["read"]
    # Set all to SE by default
    units["seq_type"] = "SE"
    # If duplicated data, then it is PE
    units = units.reset_index(drop=True).sort_values(by=["data"])
    units.loc[units["data"].duplicated(keep=False), "seq_type"] = "PE"
    # Fix data for SE
    mask = units["seq_type"].eq("SE")
    units.loc[mask, "data"] = units.loc[mask, "data"].str.replace(
        "R{Read}", "R1", regex=False
    )
    units = (
        units.groupby(
            units.columns.drop(["size_kb", "n_reads"]).to_list(), dropna=False
        )[["size_kb", "n_reads"]]
        .agg(lambda x: pd.NA if x.isna().all() else x.tolist())
        .reset_index()
    )


# Add extra_file_md5 (MD5 hash of extra file), if present in out_path
if args.extra_file.exists() and "extra_file_md5" in out_path_wildcards:
    import hashlib

    units["extra_file_md5"] = hashlib.md5(
        open(args.extra_file, "rb").read()
    ).hexdigest()


# Add workflow_ver (current workflow version), if present in out_path
if "workflow_ver" in out_path_wildcards:
    import git

    repo = git.Repo(Path(__file__).resolve(strict=True).parent.parent)
    commits = pd.DataFrame([[commit.hexsha, commit.committed_date] for commit in repo.iter_commits()], columns=["hexsha", "date"]).sort_values(by="date")
    tags = pd.DataFrame([[tag.commit.hexsha, tag.name] for tag in repo.tags], columns=["hexsha", "tag"])
    commits = pd.merge(commits, tags, how="left", on="hexsha").ffill()

    mask = commits.duplicated(subset="tag")
    commits.loc[mask, "tag"] = "+" + commits.loc[mask, "tag"]

    units["workflow_ver"] = commits.iloc[-1]["tag"]


# Reorder columns
col_order = {
    "sample": 1,
    "library": 2,
    "flowcell": 3,
    "lane": 4,
    "seq_type": 5,
    "library_type": 6,
    "material": 7,
    "data": 8,
}
units = units[sorted(units.columns.values, key=lambda x: col_order.get(x, 999))]
# Sort rows
units = units.sort_values(by=list(units.columns.drop(["size_kb", "n_reads"]).values))
logging.info(f"Units file has {units.shape[0]} rows and {units.shape[1]} columns.")
logging.debug(f"\n{units}")
logging.debug(f"\n{units.dtypes}")


#####################
### Define groups ###
#####################
datasets = list()
if out_path_wildcards:
    for keys, units in units.groupby(out_path_wildcards, group_keys=True):
        name = dict(zip(out_path_wildcards, keys))
        logging.debug(f"Group units with output path wildcards: {name}")
        logging.debug(f"\n{units}")
        # Create output path
        out_path = Path(args.out_path.format(**name))
        datasets.append((out_path, units))
else:
    # Create output path
    out_path = Path(args.out_path)
    datasets.append((out_path, units))


###################
### Save Output ###
###################
# Stats file
if args.out_stats:
    logging.info(f"Saving stats to file {args.out_stats}")

    # Output stats as ID\tsize_kb\tn_reads
    out_stats = []
    for out_path, units in sorted(datasets):
        logging.debug(f"Gathering stats for group {out_path}")
        out_stats.append(
            units[["size_kb", "n_reads"]]
            .assign(id=out_path.parent)
            .groupby("id")[["size_kb", "n_reads"]]
            .agg(lambda x: pd.NA if x.isna().all() else x.tolist())
            .reset_index()
        )
    logging.debug(pd.concat(out_stats))

    with open(args.out_stats, "x") as out_stat_fh:
        np.set_printoptions(legacy="1.21")
        out_stat_fh.write(f"# {args}\n")
        pd.concat(out_stats).dropna(axis=1, how="all").to_csv(
            out_stat_fh,
            sep="\t",
            na_rep="<NA>",
            header=False,
            index=False,
            float_format="%g",
        )


if not args.dryrun:
    for out_path, units in datasets:
        # Create folders
        out_path.mkdir(parents=True, exist_ok=args.force)
        # Save units.tsv file
        units.dropna(axis=1, how="all").to_csv(
            out_path / "units.tsv",
            sep="\t",
            index=False,
            mode="w" if args.force else "x",
        )
        if "sample" in units:
            samples = (
                units["sample"]
                .copy()
                .to_frame()
                .drop_duplicates()
                .sort_values(by=["sample"])
            )
            samples[["alias", "group", "condition"]] = [np.nan, np.nan, args.condition]
            # Save samples.tsv file
            samples.to_csv(
                out_path / "samples.tsv",
                sep="\t",
                index=False,
                mode="w" if args.force else "x",
            )
        # Copy extra file
        if args.extra_file.exists():
            logging.debug("Copying extra file.")
            open(out_path / extra_file_name, "w" if args.force else "x").write(
                open(args.extra_file, "r").read()
            )

        logging.info(f"Unit {out_path} created!")


exit(0)
