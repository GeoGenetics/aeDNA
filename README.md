# aeDNA - a customizable Snakemake workflow for ancient environmental DNA

[![Snakemake](https://img.shields.io/badge/snakemake-≥7.25.0-brightgreen.svg)](https://snakemake.bitbucket.io)

This workflow combines several modules to build a workflow for ancient environmental DNA (aeDNA) analyses and QC:
- [Read trimming](https://github.com/GeoGenetics/ngs-trim)
- [Read dereplication](https://github.com/GeoGenetics/ngs-derep)
- [Taxonomy assignment](https://github.com/GeoGenetics/ngs-taxon)

## Authors

* Filipe G. Vieira

## Requirements

These workflows require:
* `conda` / `mamba` / `micromamba`
* `snakemake`

## Install and configure Snakemake environment

If you don't have the required packages (and cannot install them), `snakemake` [recommends](https://snakemake.readthedocs.io/en/stable/getting_started/installation.html) installing [mambaforge](https://mamba.readthedocs.io/en/latest/installation.html). If you are looking for a more lightweight solution, try `micromamba` (info [here](https://mamba.readthedocs.io/en/latest/installation.html#micromamba)):
```
mkdir micromamba/
cd micromamba
curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba
./bin/micromamba shell init --shell bash --prefix `pwd`
```

Independently of what you choose (`conda`, `mamba`, or `micromamba`), create an environment from the `environment.yaml` file in this repo, activate it (e.g. for `micromamba`) and, the first time you use it, set channel priority to `strict`:
```
micromamba create --name snakemake_env --file environment.yaml
micromamba activate snakemake_env
micromamba config set channel_priority strict
```


## Usage

#### Step 1: Install and configure workflow

1. [Clone](https://help.github.com/en/articles/cloning-a-repository) this repository to your local system
2. Configure workflow according to your needs via editing the file `config/config.yaml`.
3. Add samples to analyze (to generate draft units, use the script `make_units.py`; NB: always manually check files before running workflow!):
    * Add samples to `config/samples.tsv` (column `sample` is mandatory).
    * For each sample, add one or more sequencing units (runs, lanes, libraries or replicates) to `config/units.tsv`, as well as adapters used, and path to FASTQ files (if paired-end, use `R{Read}` to represent `R1/R2` files).

Missing values can be specified by empty columns or by writing `NA`.

4. Define your `TOKEN`
```
export GITHUB_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

#### Step 2: Test workflow

5. Test your configuration by performing a dry-run via:
```
snake-wrapper.py --dry-run
```

and confirm workflow analyses by checking the DAG:
```
snake-wrapper.py --dag | dot -Tsvg > dag.svg
```

You can also create all needed `conda` environments beforehand:
```
snake-wrapper.py --jobs 1 --conda-create-envs-only
```

#### Step 3: Execute workflow
6. Execute the workflow, using `$N` cores, either:

  6.1. locally via:
```
snake-wrapper.py --jobs $N
```

  6.2. in a [SLURM](https://slurm.schedmd.com/overview.html) cluster environment via:
```
snake-wrapper.py --jobs $N --slurm
```

  6.3. under a singularity container (if you not only want to fix the software stack but also the underlying OS), in combination with any of the modes above:
```
snake-wrapper.py --use-singularity
```

#### Step 4: Check results

7. After successful execution, you can create a self-contained interactive HTML report:
```
snake-wrapper.py --report report.html
```
