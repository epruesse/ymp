#!/usr/bin/env Rscript

#' We expect to be called from snakemake script directive, so having
#' `snakemake` object with `snakemake@input` etc containing paths.

#' We also need to redirect our output to log ourselves...
R.home()
logfile <- file(snakemake@log[[1]], open="wt")
sink(logfile)
sink(logfile, type="message")

R.home()

message("Importing Salmon data into R using tximport")

message("1. ----------- Loading packages ----------")
library(tximport)
library(readr)
library(GenomicFeatures)
library(rtracklayer)
library(SummarizedExperiment)

message("2. ----------- Loading GTF ----------")
message("Filename = ", snakemake@input$gtf)
gr <- rtracklayer::import.gff(snakemake@input$gtf)

message("3. ----------- Loading quant.sf files ----------")
files <- snakemake@input$counts
names(files) <- gsub(".salmon", "", basename(dirname(snakemake@input$counts)))
txi <- tximport(files, type="salmon", txOut=TRUE)

message("4. ----------- Assembling SummarizedExperiment w/ rowData ----------")
txmeta <- mcols(gr)[mcols(gr)$type=="transcript", ]  # only transcript rows
txmeta <- subset(txmeta, select = -type)
rownames(txmeta) <- txmeta$transcript_id  # set names
txmeta <- txmeta[rownames(txi$counts), ]  # only rows for which we have counts
txmeta <- Filter(function(x)!all(is.na(x)), txmeta)  # remove all-NA columns

se <- SummarizedExperiment(
    assays = txi[c("counts", "abundance", "length")],
    rowData = txmeta,
    metadata = list(
        countsFromAbundance = txi$countsFromAbundance  # should be no
    )
)

message("5. ----------- Writing RDS with transcript se object ----------")
message("Filename = ", snakemake@output$transcripts)
saveRDS(se, snakemake@output$transcripts)


message("6. ----------- Summarizing transcript counts to gene counts ----------")
txi_genes <- summarizeToGene(txi, txmeta[,c("transcript_id", "gene_id")])

message("7. ----------- Assembling SummarizedExperiment w/ rowData ----------")
gmeta <-  mcols(gr)[mcols(gr)$type=="gene", ]  # only transcript rows
gmeta <- subset(gmeta, select = -type)
rownames(gmeta) <- gmeta$gene_id  # set names
gmeta <- gmeta[rownames(txi_genes$counts), ]  # only rows for which we have counts
gmeta <- Filter(function(x)!all(is.na(x)), gmeta)  # remove all-NA columns

gse <- SummarizedExperiment(
    assays = txi_genes[c("counts", "abundance", "length")],
    rowData = gmeta,
    metadata = list(
        countsFromAbundance = txi_genes$countsFromAbundance  # should be no
    )
)

message("Rounding counts to keep DESeq2 happy")
assay(gse) <- round(assay(gse))
mode(assay(gse)) <- "integer"

## Rename length assay IFF we are having counts, not TPM
## (not sure if otherwise is possible with Salmon, but since this is
## checked inside of deseq/tximeta, let's do check here as well).
if (txi_genes$countsFromAbundance == "no") {
    message("Renaming length assay to avgTxLength so DESeq2 will use for size estimation")
    assayNames(gse)[assayNames(gse) == "length"] <- "avgTxLength"    
}

message("8. ----------- Writing RDS with gene se object ----------")
message("Filename = ", snakemake@output$transcripts)
saveRDS(gse, snakemake@output$counts)

message("done")
