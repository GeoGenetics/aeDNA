from snakemake.utils import validate


# Get basedir
base_dir = Path(workflow.basedir)


########################
###### YAML input ######
########################

### config.yaml
validate(config, schema=base_dir / "schemas/config.schema.yaml")


### samples.yaml
samples = (
    pd.read_csv(
        config["samples"], sep="\t", comment="#", dtype={"sample": str, "group": str}
    )
    .set_index("sample", drop=False)
    .sort_index()
)
# If no "group" provided, set it the same as "sample"
samples["group"] = [_item_or_sample(row, "group") for row in samples.itertuples()]
# If no "alias" provided, set it the same as "sample"
samples["alias"] = [_item_or_sample(row, "alias") for row in samples.itertuples()]
validate(samples, schema=base_dir / "schemas/samples.schema.yaml")


### units.yaml
index = ["sample", "library", "lane"]
units = (
    pd.read_csv(
        config["units"],
        sep="\t",
        comment="#",
        dtype={
            "sample": str,
            "library": str,
            "barcode": str,
            "flowcell": str,
            "lane": str,
            "platform": str,
            "data": str,
        },
    )
    .set_index(index, drop=False)
    .sort_index()
)
# Validate unit data
validate(units, schema=base_dir / "schemas/units.schema.yaml")
# Check for duplicated data sources
units_dup = units.index.duplicated()
if units_dup.any():
    raise ValueError(
        f"duplicated unit description. Please check that your unit IDs ({index}) are unique:\n{units[units_dup]}"
    )
data_dup = units.duplicated(subset=["data"])
if data_dup.any():
    raise ValueError(
        f"duplicated data source. Please check that your FASTQ files are unique:\n{units[data_dup]}"
    )
# format sequencing type
units["seq_type"] = units["seq_type"].str.lower()
# Check if {Read} is specified for PE
samples_pe_no_read = units[
    units["seq_type"].eq("pe") & ~units["data"].str.contains("{Read}")
].data
if samples_pe_no_read.any():
    raise ValueError(
        f"some PE units do not have '{{Read}}' specified: {samples_pe_no_read.to_string()}"
    )

# Only select units from samples present in "samples.tsv"
units = units[units["sample"].isin(samples["sample"])]
# Check if all samples have a unit
samples_without_units = samples[~samples["sample"].isin(units["sample"])]
if not samples_without_units.empty:
    raise ValueError(
        "some samples have no units:\n{}".format(
            samples_without_units.to_string(index=False)
        )
    )


#############################
###### Pipeline config ######
#############################


wildcard_constraints:
    sample="|".join(get_samples()),
    group="|".join(get_groups()),
    library="|".join(get_libraries()),
    lane="|".join(get_lanes()),
