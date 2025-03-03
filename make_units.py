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
    nargs="*",
    help="Regex to extract sample and library identifiers. For help, see: https://docs.python.org/3/library/re.html#regular-expression-syntax",
)
parser.add_argument(
    "-r",
    "--regex",
    action="store",
    default=r"\/(?P<date>\d{8})_(?P<machine>A\d{5})_(?P<run_n>\d{4})_(?P<flowcell>[AB]H[A-Z0-9]{8})(_(?P<pool_tag>[A-Z0-9]+))?(_\w+)?\/(?P<project>[^\/]+)\/(?P<library>LV\d{10})-(?P<subsample>[^_-]+)-(?P<archive>[^_]+)",
    help="Regex to extract identifiers. For help, see: https://docs.python.org/3/library/re.html#regular-expression-syntax",
)
parser.add_argument(
    "--min-file-size",
    action="store",
    type=int,
    default=20,
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
    "--metadata-extra",
    action="store",
    nargs="+",
    default=["sample=Lib", "center=CAEG", "platform=ILLUMINA", "material=DNA"],
    help="Extra metadata.",
)
parser.add_argument(
    "--out-path",
    action="store",
    default="{library[0]}{library[1]}{library[2]}/{library[3]}{library[4]}{library[5]}/{library[6]}{library[7]}{library[8]}/{library}/{date:%Y%m%d}_{flowcell}/{workflow_ver}/{extra_file_md5}/config",
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
print(f"# {args}")


# Add extra metadata
if args.library_type and np.isnan(args.adapters):
    args.adapters = adapters.get(args.library_type, np.nan)
for key in ["library_type", "adapters"]:
    if value := getattr(args, key):
        args.metadata_extra.append(f"{key}={value}")


# Set logger
loglevel = getattr(logging, args.loglevel.upper(), None)
logging.basicConfig(encoding="utf-8", level=loglevel)
logging.info(f"Found {len(args.fq_files)} input files")


units = pd.DataFrame()
for fq_file in sorted(args.fq_files):
    row = dict()
    fq_file = Path(fq_file.rstrip())
    row["data"] = str(fq_file.resolve(strict=True))
    # Get info from read IDs
    if not args.metadata_ignore_header:
        with gzip.open(row["data"], "rt") as gz:
            read_header = gz.readline().lstrip("@").rstrip().split(":")
            if len(read_header) > 2:
                row["machine"] = read_header[0]
                row["run_n"] = read_header[1]
                row["flowcell"] = read_header[2]
    # Get info from file name
    fq_prefix = (
        fq_file.with_suffix("").with_suffix("")
        if fq_file.suffix in [".gz", ".bz2"]
        else fq_file.with_suffix("")
    )
    fq_prefix = str(fq_prefix).rsplit("_", 1)[0]
    logging.debug(f"FQ prefix: {fq_prefix}")
    row["read"] = fq_prefix.rsplit("_", 1)[1]
    row["lane"] = fq_prefix.rsplit("_", 2)[1]
    row["sample_n"] = fq_prefix.rsplit("_", 3)[1]
    # Get info from regexp
    matches = re.search(args.regex, fq_prefix.rsplit("_", 3)[0])
    if not matches:
        logging.warning(f"cannot match regex to sample {fq_prefix}. Skipping...")
        continue
    matches = matches.groupdict()
    for key, value in matches.items():
        logging.debug(f"adding {key} information: {value}")
        if key in row:
            assert value.endswith(
                row[key]
            ), f"{key} information does not match: {value} != {row[key]}."
        row[key] = value

    row = pd.DataFrame([row])
    logging.debug(row)
    units = pd.concat([units, row])


# Add metadata
units["seq_type"] = "SE"
for metadata_extra in args.metadata_extra:
    key, value = metadata_extra.split("=")
    units[key] = value


# Format date column (if present)
if "date" in units.columns.values:
    units["date"] = pd.to_datetime(units["date"])

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
logging.debug(units)
logging.debug(units.dtypes)
if not units.shape[0]:
    logging.warning("No valid data files found!")
    exit(0)

# Reset index and sort data
units = units.reset_index(drop=True).sort_values(by=["data"])
# Collapse PE libraries
units["data"] = units["data"].str.replace(r"_R[12]_", "_R{Read}_", regex=True)
units.loc[units["data"].duplicated(keep=False), "seq_type"] = "PE"
del units["read"]
# Fix data for SE
mask = units["seq_type"].eq("SE")
units.loc[mask, "data"] = units.loc[mask, "data"].str.replace(
    "_R{Read}_", "_R1_", regex=False
)
units.drop_duplicates(inplace=True)
units.sort_values(by=list(units.columns.values), inplace=True)
# Fix invalid values
fix_cols = units.columns.drop("data")
units[fix_cols] = units[fix_cols].replace(args.rm_chars, value="", regex=True)
logging.debug(units)
logging.debug(units.dtypes)


# Make samples DF
samples = units["sample"].copy().to_frame()
samples["alias"] = np.nan
samples["group"] = np.nan
samples["condition"] = args.condition
samples.drop_duplicates(inplace=True)
samples.sort_values(by=["sample"], inplace=True)
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
                units.drop(
                    ["extra_file_md5", "workflow_ver"], axis=1, errors="ignore"
                ),
            )
        )
else:
    # Create output path
    out_path = Path(args.out_path)
    datasets.append((out_path, samples, units))


for out_path, sample, units in sorted(datasets):
    file_sizes_kb = [
        round(Path(data.format(Read=1)).stat().st_size / 1024, 1)
        for data in units["data"]
    ]
    if any(file_size_kb < args.min_file_size for file_size_kb in file_sizes_kb):
        total_reads = [
            int(gzip_n_lines(data.format(Read=1)) / 4) for data in units["data"]
        ]
        print(
            f"{out_path.parent}\t# Total reads: {total_reads}; File size (KiB): {file_sizes_kb};"
        )
    else:
        print(out_path.parent)


# Check if groups exist
datasets_exist = [out_path.is_dir() for out_path, _, _ in datasets]
if any(datasets_exist):
    logging.warning(f"Some of the datasets already exist ({datasets_exist})!")


# Create files/fodlers
if args.dryrun:
    logging.debug("Dry-run finished successfully.")
else:
    for out_path, samples, units in datasets:
        if out_path.exists() and not args.force:
            logging.warning(f"Unit {out_path} already exists. Skipping!")
            continue

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
