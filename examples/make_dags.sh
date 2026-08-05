#!/bin/bash

set -euxo pipefail

SNAKEMAKE_OPTS="--snakefile ../../workflow/Snakefile --configfile config/config.yaml --software-deployment-method conda --keep-storage-local-copies --forceall $@"

for TEST in euk_prok_virus dragen
do
    cd $TEST/
    snakemake multiqc_taxon_upload $SNAKEMAKE_OPTS --dryrun
    snakemake multiqc_taxon_upload $SNAKEMAKE_OPTS --rulegraph | dot -Tsvg > rulegraph.svg
    snakemake multiqc_taxon_upload $SNAKEMAKE_OPTS --filegraph | dot -Tsvg > filegraph.svg
    snakemake multiqc_taxon_upload $SNAKEMAKE_OPTS --dag | dot -Tsvg > dag.svg

    if [ -d .tests/unit/ ]; then
        pytest -vvvv -r a -p no:cacheprovider .tests/unit/
    fi
    cd ../
done
