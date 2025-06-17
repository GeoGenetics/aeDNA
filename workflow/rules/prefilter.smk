
rule prefilter_reads_taxonomy:
    input:
        bam=rules.taxon_prefilter_align_merge.output.bam,
        taxonomy=config["prefilter"]["ref"]["hires_organelles_viruses_smags"]["acc2taxid"],
    output:
        read_id=temp(
            "temp/reads/prefilter/taxonomy/{sample}_{library}_{read_type_map}.read_ids.txt.gz"
        ),
    log:
        "logs/reads/prefilter/taxonomy/{sample}_{library}_{read_type_map}.log",
    benchmark:
        "benchmarks/reads/prefilter/taxonomy/{sample}_{library}_{read_type_map}.jsonl"
    params:
        out_prefix=lambda w, output: output.read_id.removesuffix(".read_ids.txt.gz"),
        extra="""--rank '{"domain":["d__Bacteria", "d__Archaea", "d__Viruses"]}' --only-read-ids --combine --unique""",
    conda:
        Path(workflow.basedir) / "envs" / "get_reads_taxonomy.yaml"
    threads: 8
    resources:
        mem=lambda w, attempt: f"{20* attempt} GiB",
        runtime=lambda w, attempt: f"{2* attempt} h",
    shell:
        "getRTax --bam {input.bam} --taxonomy-file {input.taxonomy} {params.extra} --prefix {params.out_prefix} > {log} 2>&1"


rule prefilter_reads_extract:
    input:
        fastq=workflow_taxon_prefilter.get_data,
        pattern=rules.prefilter_reads_taxonomy.output.read_id,
    output:
        fq="results/reads/prefilter/extract/{sample}_{library}_{read_type_map}.fastq.gz",
    log:
        "logs/reads/prefilter/extract/{sample}_{library}_{read_type_map}.log",
    benchmark:
        "benchmarks/reads/prefilter/extract/{sample}_{library}_{read_type_map}.jsonl"
    params:
        command="grep",
        extra="--invert-match --delete-matched",
    threads: 4
    resources:
        mem=lambda w, attempt: f"{1* attempt} GiB",
        runtime=lambda w, attempt: f"{30* attempt} m",
    wrapper:
        f"{wrapper_ver}/bio/seqkit"


use rule bowtie2 from workflow_taxon_prefilter as taxon_prefilter_bowtie2 with:
    input:
        sample=workflow_taxon.get_data,
        idx=workflow_taxon_prefilter.get_index,


use rule bowtie2 from workflow_taxon as taxon_bowtie2 with:
    input:
        sample=[rules.prefilter_reads_extract.output.fq],
        idx=workflow_taxon.get_index,


##########
### QC ###
##########


rule prefilter_fastqc:
    input:
        rules.prefilter_reads_extract.output.fq,
    output:
        html="stats/reads/fastqc/prefilter/{sample}_{library}_{read_type_map}.html",
        zip="stats/reads/fastqc/prefilter/{sample}_{library}_{read_type_map}_fastqc.zip",
    log:
        "logs/reads/fastqc/prefilter/{sample}_{library}_{read_type_map}.log",
    benchmark:
        "benchmarks/reads/fastqc/prefilter/{sample}_{library}_{read_type_map}.jsonl"
    threads: 2
    resources:
        # Memory is hard-coded to 250M per thread (https://github.com/bcbio/bcbio-nextgen/issues/2989)
        mem=lambda w, threads: f"{512* threads} MiB",
        runtime=lambda w, attempt: f"{1* attempt} h",
    wrapper:
        f"{wrapper_ver}/bio/fastqc"
