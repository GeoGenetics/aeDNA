
rule prefilter_reads_taxonomy:
    input:
        bam=rules.taxon_prefilter_align_merge.output.bam,
        names=rules.taxon_prefilter_metadmg_lca.input.names,
        nodes=rules.taxon_prefilter_metadmg_lca.input.nodes,
        acc2taxid=rules.taxon_prefilter_metadmg_lca.input.acc2taxid,
    output:
        read_id=temp(
            "temp/reads/prefilter/taxonomy/{sample}_{library}_{read_type_map}.read_ids"
        ),
    log:
        "logs/reads/prefilter/taxonomy/{sample}_{library}_{read_type_map}.log",
    benchmark:
        "benchmarks/reads/prefilter/taxonomy/{sample}_{library}_{read_type_map}.jsonl"
    params:
        extra="-taxnames d__Bacteria,d__Archaea,d__Viruses",
    conda:
        urlunparse(
            baseurl._replace(path=str(Path(baseurl.path) / "envs" / "metadmg.yaml"))
        )
    threads: 1
    resources:
        mem=lambda w, input, attempt: f"{3* attempt} GiB",
        runtime=lambda w, input, attempt: f"{(0.06* input.size_gb+2)* attempt} h",
    shell:
        "(extract_reads bytaxid -hts {input.bam} -names {input.names} -nodes {input.nodes} -acc2tax <(cat {input.acc2taxid}) {params.extra} -strict 1 -forcedump 1 -out - -type sam | awk '!/^@/' | cut -f 1 | uniq > {output.read_id}) 2> {log}"


rule prefilter_reads_merge:
    input:
        taxonomy=rules.prefilter_reads_taxonomy.output.read_id,
        saturated=rules.taxon_prefilter_shard_saturated_reads_filter.output.read_id,
    output:
        read_id=temp(
            "temp/reads/prefilter/merge/{sample}_{library}_{read_type_map}.read_ids"
        ),
    log:
        "logs/reads/prefilter/merge/{sample}_{library}_{read_type_map}.log",
    benchmark:
        "benchmarks/reads/prefilter/merge/{sample}_{library}_{read_type_map}.jsonl"
    localrule: True
    threads: 1
    resources:
        mem=lambda w, attempt: f"{1* attempt} GiB",
        runtime=lambda w, attempt: f"{30* attempt} m",
    shell:
        "cat {input} > {output} 2> {log}"


rule prefilter_reads_extract:
    input:
        fastq=workflow_taxon_prefilter.get_data,
        pattern=branch(
            is_activated("prefilter/filter/saturated_reads"),
            then=rules.prefilter_reads_merge.output.read_id,
            otherwise=rules.prefilter_reads_taxonomy.output.read_id,
        ),
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
        mem=lambda w, input, attempt: f"{(0.2* input.size_gb+5)* attempt} GiB",
        runtime=lambda w, input, attempt: f"{(0.06* input.size_gb+0.5)* attempt} h",
    wrapper:
        f"{wrapper_ver}/bio/seqkit"


use rule shard_bowtie2 from workflow_taxon_prefilter as taxon_prefilter_shard_bowtie2 with:
    input:
        sample=workflow_taxon.get_data,
        idx=workflow_taxon_prefilter.get_index,


use rule shard_bowtie2 from workflow_taxon as taxon_shard_bowtie2 with:
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
    threads: 4
    resources:
        mem=lambda w, attempt: f"{5* attempt} GiB",
        runtime=lambda w, attempt: f"{1* attempt} h",
    wrapper:
        f"{wrapper_ver}/bio/fastqc"
