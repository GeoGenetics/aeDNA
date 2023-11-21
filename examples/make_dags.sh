#!/bin/bash

SNAKEMAKE_OPTS="--cores 20 --snakefile ../../workflow/Snakefile --configfile config/config.yaml --use-conda --conda-prefix /tmp/cache/conda $@"

for TEST in robot_tests robot_tests_big
do
    cd $TEST/
    snakemake $SNAKEMAKE_OPTS --dryrun
    snakemake $SNAKEMAKE_OPTS --rulegraph | dot -Tsvg > rulegraph.svg
    snakemake $SNAKEMAKE_OPTS --filegraph | dot -Tsvg > filegraph.svg
    snakemake $SNAKEMAKE_OPTS --dag | dot -Tsvg > dag.svg
    cd ../
done
