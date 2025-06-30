# aeDNA - a customizable Snakemake workflow for ancient environmental DNA

[![Snakemake](https://img.shields.io/badge/snakemake-≥8.11.2-brightgreen.svg)](https://snakemake.bitbucket.io)

This workflow combines several modules to build a workflow for ancient environmental DNA (aeDNA) analyses and QC:
- [Read trimming](https://github.com/GeoGenetics/ngs-trim)
- [Read dereplication](https://github.com/GeoGenetics/ngs-derep)
- [Taxonomy assignment](https://github.com/GeoGenetics/ngs-taxon)

## Authors

* Filipe G. Vieira

## Requirements

These workflows require:
* [pixi](https://www.pixi.sh)
* [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)
* [snakemake](https://snakemake.github.io/)

## Install and configure Snakemake environment

The easiet way to deploy `snakemake` is to [install pixi](https://pixi.sh/latest/installation/) and use the included environment.


## Usage

#### Step 1: Install and configure workflow

1. [Clone](https://help.github.com/en/articles/cloning-a-repository) this repository to your local system
2. Test `pixi` environment: `pixi run snakemake --version`
2. Configure workflow according to your needs via editing the file `config/config.yaml`.
3. Add samples to analyze:
    * Add samples to `config/samples.tsv` (column `sample` is mandatory).
    * For each sample, add one or more sequencing units (runs, lanes, libraries or replicates) to `config/units.tsv`, as well as adapters used, and path to FASTQ files (if paired-end, use `R{Read}` to represent `R1/R2` files).
    * Missing values can be specified by empty columns or by writing `NA`.
    * To facilitate things, the helper script `make_units.py` is provided that, from a list of FASTQ files, generates a folder structure with all necessary files to run (NB: always manually check files before running workflow!).

4. Define your `TOKEN`
```
export GITHUB_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

#### Step 2: Test workflow

5. Test your configuration by performing a dry-run via:
```
pixi run snakemake --configfile config/config.yaml --dry-run
```

and confirm workflow analyses by checking the DAG:
```
pixi run snakemake --configfile config/config.yaml --dag | dot -Tsvg > dag.svg
```

You can also create all needed `conda` environments beforehand:
```
pixi run snakemake --configfile config/config.yaml --jobs 1 --conda-create-envs-only
```

#### Step 3: Execute workflow
6. Execute the workflow, using `$N` cores, either:

  6.1. locally via:
```
pixi run snakemake --configfile config/config.yaml --jobs $N --software-deployment-method conda
```

  6.2. in a [SLURM](https://slurm.schedmd.com/overview.html) cluster environment via:
```
pixi run snakemake --configfile config/config.yaml --jobs $N --software-deployment-method conda --executor slurm
```

#### Step 4: Check results

7. After successful execution, you can create a self-contained interactive HTML report:
```
pixi run snakemake --configfile config/config.yaml --report report.html
```
