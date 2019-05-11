#!/usr/bin/env Rscript
#
# Loading the ballgown ctag files takes quite a while and lots of memory.
# We use this script to pre-compute the R object and then save it as rda
#
# Synopsis:
# Rscript ballgown_collect.R out.rda sample1_dir sample2_dir ...

args <- commandArgs(trailingOnly=TRUE)
outfile <- args[1]
samples <- args[2:length(args)]

library(ballgown)

bg <- ballgown(samples=samples)

save(bg, file=outfile)
