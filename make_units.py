#!/usr/bin/env python

import argparse
import sys
import gzip
import re
import logging
import numpy as np
import pandas as pd
from pathlib import Path


# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Prepare `sample` and `unit` files from list of FASTQ files (on stdin).",
    allow_abbrev=False,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-r",
    "--regex",
    action="store",
    default=r"\/(?P<date>\d{8})_A.+\/(?P<library>LV\d+)-LV\d+-(?P<sample>LV\d+)",
    help="Regex to extract sample and library identifiers. For help, see: https://docs.python.org/3/library/re.html#regular-expression-syntax",
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
    "-p",
    "--platform",
    action="store",
    default="ILLUMINA",
    choices=[
        "CAPILLARY",
        "LS454",
        "ILLUMINA",
        "SOLID",
        "HELICOS",
        "IONTORRENT",
        "ONT",
        "PACBIO",
    ],
    help="Sequencing platform.",
)
parser.add_argument(
    "-m",
    "--material",
    action="store",
    default="DNA",
    choices=["DNA", "RNA"],
    help="Material type.",
)
parser.add_argument(
    "--rm-chars",
    action="store",
    default="[\s_\-/\/]",
    help="Invalid characters (will be removed).",
)
parser.add_argument(
    "--out-path",
    action="store",
    default="{sample[0]}{sample[1]}{sample[2]}{sample[3]}/{sample[4]}{sample[5]}{sample[6]}{sample[7]}/{sample}/{date}_{flowcell}_{library}/config",
    help="Output folder structure.",
)
parser.add_argument(
    "--extra-file",
    action="store",
    type=Path,
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
print(f"# {args}")
fq_files = sys.stdin.readlines()
units = pd.DataFrame()


loglevel = getattr(logging, args.loglevel.upper(), None)
logging.basicConfig(encoding="utf-8", level=loglevel)
logging.info("Found {} input files".format(len(fq_files)))


for fq_file in sorted(fq_files):
    row = dict()
    fq_file = Path(fq_file.rstrip())
    row["data"] = str(fq_file.resolve(strict=True))
    # Get info from read IDs
    with gzip.open(row["data"], "rt") as gz:
        read_id = gz.readline().lstrip("@").rstrip().split(":")
    row["machine"] = read_id[0]
    row["run_n"] = read_id[1]
    row["flowcell"] = read_id[2]
    # Get info from arguments
    row["platform"] = args.platform
    row["material"] = args.material
    row["type"] = "SE"
    row["adapters"] = np.nan
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
    match = re.search(args.regex, fq_prefix.rsplit("_", 3)[0])
    if not match:
        logging.warning(f"cannot match regex to sample {fq_prefix}. Skipping...")
        continue
    match = match.groupdict()
    row["sample"] = match.get("sample")
    row["library"] = match.get("library")
    row["barcode"] = match.get("barcode")
    row["date"] = match.get("date")

    row = pd.DataFrame([row])
    logging.debug(row)
    units = pd.concat([units, row])


# Reorder columns
col_order = {
    "sample": 0,
    "library": 1,
    "barcode": 2,
    "date": 3,
    "machine": 4,
    "run_n": 5,
    "flowcell": 6,
    "lane": 7,
    "sample_n": 8,
    "platform": 9,
    "type": 10,
    "material": 11,
    "data": 12,
    "adapters": 13,
}
units = units[sorted(units.columns.values, key=lambda x: col_order.get(x, 999))]
logging.debug(units)
if not units.shape[0]:
    logging.warning("No valid data files found!")
    exit(0)

# Reset index and sort data
units = units.reset_index(drop=True).sort_values(by=["data"])
# Collapse PE libraries
units["data"] = units["data"].str.replace(r"_R[12]_", "_R{Read}_", regex=True)
units.loc[units["data"].duplicated(keep=False), "type"] = "PE"
del units["read"]
# Fix data for SE
mask = units["type"].eq("SE")
units.loc[mask, "data"] = units.loc[mask, "data"].str.replace(
    "_R{Read}_", "_R1_", regex=False
)
units.drop_duplicates(inplace=True)
units.sort_values(by=list(units.columns.values), inplace=True)
# Fix values
fix_cols = [
    "sample",
    "library",
    "barcode",
    "date",
    "machine",
    "run_n",
    "flowcell",
    "lane",
    "sample_n",
]
units[fix_cols] = units[fix_cols].replace(args.rm_chars, value="", regex=True)
logging.info(units)


# Make samples DF
samples = units["sample"].copy().to_frame()
samples["alias"] = np.nan
samples["group"] = np.nan
samples["condition"] = args.condition
samples.drop_duplicates(inplace=True)
samples.sort_values(by=["sample"], inplace=True)
logging.info(samples)


# Define grouping
keys = list(
    set([key.split("}")[0].split("[")[0] for key in args.out_path.split("{")[1:]])
)
logging.debug(f"Keys: {keys}")
datasets = list()
if keys:
    for name, group in units.groupby(keys, group_keys=True):
        logging.debug(group)
        name = dict(zip(keys, name))
        logging.debug(f"Group name: {name}")
        sample = group["sample"].unique()
        logging.debug(f"Group samples: {sample}")
        # Create output path
        out_path = Path(args.out_path.format(**name))
        datasets.append((out_path, samples[samples["sample"].isin(sample)], group))
else:
    # Create output path
    out_path = Path(args.out_path)
    datasets.append((out_path, samples, units))


for out_path, _, _ in sorted(datasets):
    print(out_path.parent)

logging.debug(datasets)


# Check if groups exist
datasets_exist = [out_path.is_dir() for out_path, _, _ in datasets]
assert args.force or not any(
    datasets_exist
), f"Some of the datasets already exist ({datasets_exist})!"


# Create files/fodlers
if args.dryrun:
    logging.debug("Dry-run finished successfully.")
else:
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
        if args.extra_file:
            open(out_path / args.extra_file.name, "w" if args.force else "x").write(
                open(args.extra_file, "r").read()
            )


exit(0)
