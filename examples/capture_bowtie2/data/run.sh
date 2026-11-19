


###########
### REF ###
###########

N_LINES=100000

# Get FASTA files
wget -O - ftp://ftp.ncbi.nlm.nih.gov/refseq/release/mitochondrion/mitochondrion.1.1.genomic.fna.gz | gunzip | head -n $N_LINES | bgzip > mitoch.1-of-1.fas.gz
wget -O - ftp://ftp.ncbi.nlm.nih.gov/refseq/release/plastid/plastid.1.1.genomic.fna.gz | gunzip | head -n $N_LINES | bgzip > plastid.1-of-1.fas.gz
wget -O - ftp://ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1.1.genomic.fna.gz | gunzip | head -n $N_LINES | bgzip > prok.1-of-2.fas.gz
wget -O - ftp://ftp.ncbi.nlm.nih.gov/refseq/release/bacteria/bacteria.1.1.genomic.fna.gz | gunzip | head -n $N_LINES | bgzip > prok.2-of-2.fas.gz
wget -O - ftp://ftp.ncbi.nlm.nih.gov/refseq/release/viral/viral.1.1.genomic.fna.gz | gunzip | head -n $N_LINES | bgzip > virus.1-of-1.fas.gz

ls *.fas.gz | xargs -I {} samtools faidx {}
ls *.fas.gz | xargs -I {} bowtie2-build {} {}


# acc2taxids
wget ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/nucl_gb.accession2taxid.gz
wget ftp://ftp.ncbi.nih.gov/pub/taxonomy/accession2taxid/nucl_wgs.accession2taxid.gz
zgrep -hw -f <(cut -f 1 mitoch.1-of-1.fas.gz.fai) nucl_gb.accession2taxid.gz | gzip > mitoch.acc2taxid.gz
zgrep -hw -f <(cut -f 1 plastid.1-of-1.fas.gz.fai) nucl_gb.accession2taxid.gz | gzip > plastid.acc2taxid.gz
zgrep -hw -f <(cut -f 1 prok.*.fas.gz.fai) nucl_wgs.accession2taxid.gz | gzip > prok.acc2taxid.gz
zgrep -hw -f <(cut -f 1 virus.1-of-1.fas.gz.fai) nucl_gb.accession2taxid.gz | gzip > virus.acc2taxid.gz
rm nucl_gb.accession2taxid.gz nucl_wgs.accession2taxid.gz


# Prune NCBI taxonomy
git clone https://github.com/lskatz/taxdb.git
wget -O - ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz | gunzip | tar -xf - names.dmp nodes.dmp
perl taxdb/scripts/taxdb_create.pl ncbi.sqlite
perl taxdb/scripts/taxdb_add.pl ncbi.sqlite . && rm names.dmp nodes.dmp
TAXIDS=`cat *.acc2taxid.gz | gunzip | cut -f 3 | sort -u | tr "\n" ","`
perl taxdb/scripts/taxdb_extract.pl --taxon $TAXIDS ncbi.sqlite --outdir taxdump



#############
### FASTQ ###
#############
parallel ~/appz/NGSNGS/ngsngs -i {} -r 1000 -ld Norm,70,30 -qs 30 -seq PE -f fq.gz -o {...} ::: *.fas.gz
cat *_R1.fq.gz > test_R1.fq.gz
cat *_R2.fq.gz > test_R2.fq.gz
seqkit split2 -1 test_R1.fq.gz -2 test_R2.fq.gz --by-part 3 --by-part-prefix "test_R{read}." -O .
