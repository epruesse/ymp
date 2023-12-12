#!/usr/bin/env Rscript
                                        
#' We expect to be called from snakemake script directive, so having
#' `snakemake` object with `snakemake@input` etc containing paths.

#' We also need to redirect our output to log ourselves...

R.home()

logfile <- file(snakemake@log[[1]], open="wt")
sink(logfile)
sink(logfile, type="message")

R.home()

message("Importing RSEM gene and isoform count files into R using tximport")


message("1. ----------- Loading packages ----------")
library(tximport)
library(readr)
library(GenomicFeatures)
library(rtracklayer)
library(SummarizedExperiment)

message("2. ----------- Loading GTF ----------")
message("Filename = ", snakemake@input$gtf)
gr <- rtracklayer::import.gff(snakemake@input$gtf)

message("3. ----------- Loading per transcript count files ----------")
samples <- gsub(".genes.results", "", basename(snakemake@input$counts))
tx_files <- setNames(snakemake@input$transcripts, samples)
txi <- tximport(tx_files, type = "rsem", txIn = TRUE, txOut = TRUE)

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
        countsFromAbundance = txi$countsFromAbundance
    )
)

message("5. ----------- Writing RDS with transcript se object ----------")
message("Filename = ", snakemake@output$transcripts)
saveRDS(se, snakemake@output$transcripts)

message("6. ----------- Loading per gene count files ----------")
gene_files <- setNames(snakemake@input$counts, samples)
txi_genes <- tximport(gene_files, type = "rsem", txIn = FALSE, txOut = FALSE)

## Something inside of tximport seems to reset the log sink on the
## second call. Resetting it here:
sink(logfile)
sink(logfile, type="message")

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
        countsFromAbundance = txi_genes$countsFromAbundance
    )
)

message("Rounding counts to keep DESeq2 happy")
assay(gse) <- round(assay(gse))
mode(assay(gse)) <- "integer"

message("8. ----------- Writing RDS with gene se object ----------")
message("Filename = ", snakemake@output$transcripts)
saveRDS(gse, snakemake@output$counts)

message("done")
