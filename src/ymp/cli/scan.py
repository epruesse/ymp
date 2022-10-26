import sys
import re
import os
import csv
import click


class Scanner:
    re_illumina = (
        "(?P<unit>{sample_pattern})"
        "_S(?P<slot>\d+)"
        "(_L(?P<lane>\d{{3}}))?"
        "_R(?P<pair>[12])"
        "_001.fastq.gz$"
    )
    _re_compiled = None
    header_order = ["unit", "sample", "slot", "lane", "run", "pool", "fq1", "fq2"]

    def __init__(self, folders):
        self.folders = folders
        self.sample_pattern = ".*"
        self.folder_pattern = ".*"
        self.units = {}
        self.verbosity = 0
        self.extra_keys = []
        self.keys = ["unit", "sample", "run", "pool", "fq1", "fq2"]

    def set_sample_pattern(self, pattern):
        self.sample_pattern = pattern
        self._re_compiled = None

    def set_folder_pattern(self, pattern):
        self.folder_pattern = pattern

    def set_verbosity(self, verbosity):
        self.verbosity = verbosity

    def set_extra_keys(self, extra_keys):
        self.extra_keys = extra_keys

    def log(self, message):
        if self.verbosity > 0:
            print(message)

    def get_regex(self):
        if self._re_compiled is None:
            regex = self.re_illumina.format(sample_pattern = self.sample_pattern)
            self.log(f"Regex: {regex}")
            self._re_compiled = re.compile(regex)
        return self._re_compiled

    def scan(self):
        "Iterate over configured folders, call scan_folder on each"
        for folder in self.folders:
            self.scan_folder(folder.rstrip("/"))

    def scan_folder(self, folder):
        "Walk folder"
        run = os.path.basename(folder)
        self.log(f"Scanning run {run}")
        for root, _dirs, files in os.walk(folder):
            if re.search(self.folder_pattern, root):
                self.scan_files(run, root, files)

    def scan_files(self, run, root, files):
        "Detect files"
        regex = self.get_regex()
        for fname in files:
            match = regex.search(fname)
            if match:
                self.parse_match(run, root, fname, match)

    def find_unit(self, data, num=1):
        unit_name = data["unit"]
        if num > 1:
            unit_name = f"{unit_name}_{num}"
            data["unit"] = unit_name
        unit = self.units.setdefault(unit_name, {})
        if unit and any(data[key] != unit[key] for key in data if key in unit):
            if num > 20:
                print("Too many units for one sample?!")
                print(data)
                print(unit)
                sys.exit(1)
            return self.find_unit(data, num+1)
        return unit

    def parse_match(self, run, root, fname, match):
        self.log(f"Splitting {fname}")
        data = match.groupdict()
        data["fq" + data["pair"]] = os.path.join(root, fname)
        del data["pair"]
        data["run"] = run
        pool = os.path.basename(root)
        if run != pool:
            data["pool"] = pool
        data = {key:value for key, value in data.items() if value}
        for key in self.extra_keys:
            try:
                data[key] = int(data[key])
            except:
                pass
        #data["unit"] = data["unit"].replace("-", "_")

        data["sample"] = data["unit"]
        data = {
            key: value for key, value in data.items()
            if key in self.keys + self.extra_keys
        }
        unit = self.find_unit(data)
        unit.update(data)

    def write_csv(self, outfd):
        keys = set()
        for row in self.units.values():
            keys.update(set(row.keys()))
        headers = [
            header for header in self.header_order if header in keys
        ]
        writer = csv.DictWriter(outfd, fieldnames=headers)
        writer.writeheader()
        writer.writerows(self.units[unit] for unit in sorted(self.units))


@click.command()
@click.option("--out", type=click.File('w'))
@click.option("--sample-re", default=".*")
@click.option("--folder-re", default=".*")
@click.option("-s", "--export-slot", flag_value="slot")
@click.option("-l", "--export-lane", flag_value="lane")
@click.option("-v", "--verbose", count=True)
@click.argument("folders", nargs=-1)
def scan(folders, out, sample_re, folder_re, export_slot, export_lane, verbose):
    if (out is None):
        raise click.UsageError("--out parameter required")
    scanner = Scanner(folders)
    scanner.set_sample_pattern(sample_re)
    scanner.set_folder_pattern(folder_re)
    scanner.set_verbosity(verbose)
    extra_keys = [export_slot, export_lane]
    extra_keys = [key for key in extra_keys if key is not None]
    scanner.set_extra_keys(list(extra_keys))
    scanner.scan()
    scanner.write_csv(out)


if __name__ == "__main__":
    scan()
