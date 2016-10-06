#!/usr/bin/env python3
import pandas
import os, sys
import csv
import logging
import profile

log = logging.getLogger(__name__)


def merge(out, files):
    prefix = os.path.commonprefix(files)
    suffix = os.path.commonprefix([x[::-1] for x in files])[::-1]
    s1 = len(prefix)
    s2 = -len(suffix)
    #log.debug("Prefix = {}, Suffix = {}".format(prefix, suffix))
    for filename in files:
        with open(filename) as stream:
            attrs = {}
            headers = []
            for line in stream:
                cols = line[1:].strip().split("\t")
                if len(cols) == 2:
                    attrs[cols[0]] = cols[1]
                else:
                    headers = cols
                    break
            #log.warning("Reading {}".format(filename))
            df = pandas.read_csv(stream, sep="\t",
                                 names=[x if x != "Cov" else filename[s1:s2] for x in headers],
                                 index_col = [x for x,y in enumerate(headers) if y != "Cov"],
                                 nrows=1000
                                 )
        if filename == files[0]:
            all = df
        else:
            #log.warning("Merging...")
            all = all.join(df)
    all.to_csv(out)

def merge2(out, files, collect = [], ignore = []):
    prefix = os.path.commonprefix(files)
    suffix = os.path.commonprefix([x[::-1] for x in files])[::-1]
    s1 = len(prefix)
    s2 = -len(suffix)
    log.debug("Prefix = {}, Suffix = {}".format(prefix, suffix))

    names = []
    fps = []
    try:
        for filename in files:
            names += [filename[s1:s2]]
            fps += [open(filename, "rb", buffering=81920)]
        outfp = open(out, "wb")
    except:
        raise "unable to open {}".format(filename)

    headers = None
    for fp in fps:
        cols = []
        while len(cols) <= 2:
            cols = fp.readline()[1:].strip().split(b"\t")
            
        if not headers:
            headers = cols
        if headers != cols:
            raise "Headers in {} do not match: {} != {}".format(
                            filename, headers, cols)

        icollect = [ header in collect for header in headers ]
    iignore = [ header in ignore for header in headers ]
    imatch = [ not header and not collect for header, collect in zip(icollect, iignore) ]

    outfp.write(b",".join([ x for x, match in zip(headers, imatch) if match ]))
    outfp.write(b",")
    outfp.write(",".join(names).encode('ascii'))
    outfp.write(b"\n")

    for row, line in enumerate(fps[0]):
        arr = line.rstrip().split(b"\t")
        res = [ x for x, collect in zip(arr, icollect) if collect ]
        match = [ x for x, match in zip(arr, imatch) if match ]
        assert len(arr) == len(headers)
        for fp in fps[1:]:
            arr2 = fp.readline().split(b"\t")
           
            #match2 = [ x for x, match in zip(arr, imatch) if match ]
            #if match != match2:
            #    raise "Mismatch in row {}: {} != {}".format(row, match, match2)
            res += [ x for x, collect in zip(arr2, icollect) if collect ]

        #x = sum([1 for x in res if float(x)>1])
        #print(x)
        outfp.write(b",".join(match + res) + b"\n")
        #if row > 1000: break

    outfp.close()
    for fp in fps:
        fp.close()

            


if __name__ == "__main__":
    #profile.run('merge(sys.argv[1], sys.argv[2:])')
    #profile.run('merge2(sys.argv[1], sys.argv[2:], collect=b"Cov")')
    merge2(sys.argv[1], sys.argv[2:], collect=b"Cov")
