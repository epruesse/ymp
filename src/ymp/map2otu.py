import sys, csv, re
from argparse import ArgumentParser
import fileinput

class emirge_info:
    def __init__(self, line):
        ## C5-14863|JF198678.1.1366;Prior=0.000387;Length=1357;NormPrior=0.000383;size=383
        ## <sample>-<id>|<acc>;(<key>=<value>)*[;]
        field_list = line.split(";");
        self.sid, self.acc = field_list[0].split("|")
        self.sample, self.id, _ = re.split('-([0-9]*)$', self.sid)

        for kv in field_list[1:]:
            try:
                (key, value) = kv.split("=")
                try:
                    self.__dict__[key] = float(value)
                except ValueError:
                    self.__dict__[key] = value
            except:
                pass

    def __str__(self):
        return "sample=%s id=%s acc=%s prior=%f" % (self.sample, self.id, self.acc, self.Prior)


class MapfileParser(object):
    def __init__(self, minid=0):
        self.samples = set()
        self.by_centroid_sums={}
        self.by_sample_sums={}
        self.minid=0

    def read(self, mapfiles):
        mapfile = fileinput.input(mapfiles)
        try:
            for line in mapfile:
                items = line.split("\t")
                src = emirge_info(items[0])
                centroid = emirge_info(items[1])
                id = float(items[2])
                if id < self.minid:
                    continue

                self.samples.add(src.sample)
            
                if src.sample in self.by_sample_sums:
                    self.by_sample_sums[src.sample] += src.NormPrior
                else:
                    self.by_sample_sums[src.sample] = src.NormPrior

                if not centroid.sid in self.by_centroid_sums:
                    self.by_centroid_sums[centroid.sid] = {"centroid": centroid.sid}

                if src.sample in self.by_centroid_sums[centroid.sid]:
                    self.by_centroid_sums[centroid.sid][src.sample] += src.NormPrior
                else:
                    self.by_centroid_sums[centroid.sid][src.sample] = src.NormPrior
        finally:
            mapfile.close()

    def write(self, outfile):
        if outfile == "-":
            outfile = sys.stdout
        else:
            outfile = open(outfile, "w")

        try:
            writer = csv.DictWriter(outfile, fieldnames = ["centroid"] + sorted(self.samples))
            writer.writeheader()
            for key in sorted(self.by_centroid_sums.keys()):
                writer.writerow(self.by_centroid_sums[key])
        finally:
            if outfile != sys.stdout:
                outfile.close()


def main():
    parser = ArgumentParser()
    parser.add_argument("-o","--output", default="-", metavar="file")
    parser.add_argument("--id", type=float, default=0.0)
    parser.add_argument("input", nargs="*", default="-")
    args = parser.parse_args()

    parser = MapfileParser(args.id)
    parser.read(args.input)
    parser.write(args.output)
    

if __name__ == '__main__':
    main()
