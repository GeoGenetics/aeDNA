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
    description="Generate `sample` and `unit` files from list of FASTQ files (on stdin).",
    allow_abbrev=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-i",
    "--fq-files",
    action="store",
    nargs="+",
    help="List of FASTQ files",
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
    "--metadata-ignore-header",
    action="store_true",
    default=False,
    help="Do not parse info from read header.",
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
    type=Path,
    default=Path("/projects/caeg/data/resources/config/config.yaml"),
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
# If not FASTQ files, read from STDIN
if not args.fq_files:
    args.fq_files = [fq_file.strip() for fq_file in sys.stdin.readlines()]


# Add extra metadata
if args.library_type and np.isnan(args.adapters):
    args.adapters = adapters.get(args.library_type, np.nan)
for key in ["library_type", "adapters"]:
    if value := getattr(args, key):
        args.metadata_default.append(f"{key}={value}")


# Set logger
loglevel = getattr(logging, args.loglevel.upper(), None)
logging.basicConfig(encoding="utf-8", level=loglevel)
logging.info(f"Found {len(args.fq_files)} input files")


units = pd.DataFrame()
for fq_file in sorted(args.fq_files):
    row = dict()
    row["data"] = str(Path(fq_file.rstrip()).resolve(strict=True))
    # Get metadata from read IDs
    if not args.metadata_ignore_header:
        with gzip.open(row["data"], "rt") as gz:
            read_header = gz.readline().lstrip("@").rstrip().split(":")
            if len(read_header) > 2:
                row["machine"] = read_header[0]
                row["run_n"] = read_header[1]
                row["flowcell"] = read_header[2]
    # Get metadata from regexp
    matches = re.search(args.regex, row["data"])
    if not matches:
        logging.warning(f"cannot match regex to sample {row['data']}. Skipping...")
        continue
    for key, value in matches.groupdict().items():
        # Remove leading zeros
        if key == "run_n":
            value = value.lstrip("0")
        logging.debug(f"adding {key} metadata: {value}")
        assert (
            key not in row or row[key] == value
        ), f"{key} metadata does not match: {value} != {row[key]}."
        row[key] = value
    # Replace read info with generic placeholder
    read_pos = matches.span("read")
    row["data"] = row["data"][: read_pos[0]] + "R{Read}" + row["data"][read_pos[1] :]

    row = pd.DataFrame([row])
    logging.debug(row)
    units = pd.concat([units, row])

if units.shape[0] == 0:
    logging.warning("No valid data files found!")
    exit(0)


# Format date column (if present)
if "date" in units.columns.values:
    units["date"] = pd.to_datetime(units["date"])


# Add metadata
for metadata_default in args.metadata_default:
    key, value = metadata_default.split("=")
    if key not in units:
        units[key] = value


# Fix seq_type info
units["seq_type"] = "SE"
# Collapse PE libraries
units = units.reset_index(drop=True).sort_values(by=["data"])
units.loc[units["data"].duplicated(keep=False), "seq_type"] = "PE"
del units["read"]
# Fix data for SE
mask = units["seq_type"].eq("SE")
units.loc[mask, "data"] = units.loc[mask, "data"].str.replace(
    "R{Read}", "R1", regex=False
)
units.drop_duplicates(inplace=True)


# Fix invalid values
fix_cols = units.columns.drop("data")
units[fix_cols] = units[fix_cols].replace(args.rm_chars, value="", regex=True)


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
units.sort_values(by=list(units.columns.values), inplace=True)
logging.info(f"Units file has {units.shape[0]} rows and {units.shape[1]} columns.")
logging.debug(units)
logging.debug(units.dtypes)


# Make samples DF
samples = units["sample"].copy().to_frame()
samples["alias"] = np.nan
samples["group"] = np.nan
samples["condition"] = args.condition
samples.drop_duplicates(inplace=True)
samples.sort_values(by=["sample"], inplace=True)
logging.info(
    f"Samples file has {samples.shape[0]} rows and {samples.shape[1]} columns."
)
logging.debug(samples)


# Define grouping
from string import Formatter

wildcards = list(
    set([key.split("[")[0] for _, key, _, _ in Formatter().parse(args.out_path) if key])
)
logging.debug(f"Wildcards: {wildcards}")


# Add extra_file_md5 (MD5 hash of extra file), if present in out_path
if args.extra_file.exists() and "extra_file_md5" in wildcards:
    import hashlib

    units["extra_file_md5"] = hashlib.md5(
        open(args.extra_file, "rb").read()
    ).hexdigest()

# Add workflow_ver (current workflow version), if present in out_path
if "workflow_ver" in wildcards:
    import git

    repo = git.Repo(Path(__file__).resolve(strict=True).parent)
    tag_recent = [
        [tag.name, commit.hexsha]
        for commit in repo.iter_commits()
        for tag in repo.tags
        if commit.hexsha == tag.commit.hexsha
    ][0]
    units["workflow_ver"] = tag_recent[0]


datasets = list()
if wildcards:
    for keys, units in units.groupby(wildcards, group_keys=True):
        name = dict(zip(wildcards, keys))
        logging.debug(f"Group wildcards: {name}")
        sample = units["sample"].unique()
        logging.debug(f"Group samples: {sample}")
        logging.debug(units)
        # Create output path
        out_path = Path(args.out_path.format(**name))
        datasets.append(
            (
                out_path,
                samples[samples["sample"].isin(sample)],
                units.drop(["extra_file_md5", "workflow_ver"], axis=1, errors="ignore"),
            )
        )
else:
    # Create output path
    out_path = Path(args.out_path)
    datasets.append((out_path, samples, units))


out_stats = []
for out_path, sample, units in sorted(datasets):
    logging.debug(f"Gathering stats for unit {units.data}")
    r1_sizes_kb = [
        round(Path(data.format(Read=1)).stat().st_size / 1024, 1)
        for data in units["data"]
    ]
    r2_sizes_kb = [
        round(Path(data.format(Read=2)).stat().st_size / 1024, 1)
        for data in units["data"]
    ]
    total_reads = pd.NA
    if any(r1_size_kb < args.min_file_size for r1_size_kb in r1_sizes_kb):
        total_reads = [
            int(gzip_n_lines(data.format(Read=1)) / 4) for data in units["data"]
        ]
    out_stats.append([out_path.parent, total_reads, r1_sizes_kb, r2_sizes_kb])


# Save stats to file
if args.out_stats:
    logging.info(f"Saving stats to file {args.out_stats}")
    assert not args.out_stats.exists(), "Output stats file already exists!"
    with open(args.out_stats, "a") as out_stat_fh:
        out_stat_fh.write(f"# {args}\n")
        pd.DataFrame(
            out_stats, columns=["id", "total_reads", "R1_size_kb", "R2_size_kb"]
        ).to_csv(out_stat_fh, sep="\t", index=False)


# Check if groups exist
datasets_exist = [out_path.is_dir() for out_path, _, _ in datasets]
if any(datasets_exist):
    if args.force:
        logging.warning(
            f"Some of the datasets already exist ({datasets_exist}). Overwritting!"
        )
    else:
        logging.error(f"Some of the datasets already exist ({datasets_exist})!")
        exit(-1)


# Create files/folders
if not args.dryrun:
    for out_path, samples, units in datasets:
        out_path.mkdir(parents=True, exist_ok=args.force)
        # Save samples.tsv file
        samples.to_csv(
            out_path / "samples.tsv",
            sep="\t",
            index=False,
            mode="w" if args.force else "x",
        )
        # Save units.tsv file
        units.to_csv(
            out_path / "units.tsv",
            sep="\t",
            index=False,
            mode="w" if args.force else "x",
        )
        # Copy extra file
        if args.extra_file.exists():
            logging.debug("Copying extra file.")
            open(out_path / args.extra_file.name, "w" if args.force else "x").write(
                open(args.extra_file, "r").read()
            )

        logging.info(f"Unit {out_path} created!")


exit(0)
