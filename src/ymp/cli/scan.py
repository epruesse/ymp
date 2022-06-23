import sys
import re
import os
import csv
import click

all_headers = ["unit", "sample", "slot", "lane", "run", "pool", "fq1", "fq2"]

class Scanner:
    re_illumina = (
        "(?P<unit>{sample_pattern})"
        "_S(?P<slot>\d+)"
        "(_L(?P<lane>\d{{3}}))?"
        "_R(?P<pair>[12])"
        "_001.fastq.gz"
    )

    def __init__(self, folders):
        self.folders = folders
        self.sample_pattern = ".*"
        self.folder_pattern = ".*"
        self.units = {}

    def set_sample_pattern(self, pattern):
        self.sample_pattern = pattern

    def set_folder_pattern(self, pattern):
        self.folder_pattern = pattern

    def scan(self):
        for folder in self.folders:
            self.scan_folder(folder.rstrip("/"))

    def scan_folder(self, folder):
        run = os.path.basename(folder)
        for root, _dirs, files in os.walk(folder):
            if re.search(self.folder_pattern, root):
                self.scan_files(run, root, files)

    def get_regex(self):
        regex = self.re_illumina.format(sample_pattern = self.sample_pattern)
        return re.compile(regex)

    def scan_files(self, run, root, files):
        regex = self.get_regex()
        for fname in files:
            match = regex.search(fname)
            if match:
                self.parse_match(run, root, fname, match)

    def parse_match(self, run, root, fname, match):
        data = match.groupdict()
        data["fq" + data["pair"]] = os.path.join(root, fname)
        del data["pair"]
        data["run"] = run
        pool = os.path.basename(root)
        if run != pool:
            data["pool"] = pool
        data = {key:value for key, value in data.items() if value}
        for key in ("slot", "lane"):
            try:
                data[key] = int(data[key])
            except:
                pass
        data["unit"] = data["unit"].replace("-", "_")

        data["sample"] = data["unit"]
        unit = self.find_unit(data)
        unit.update(data)

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

    def write_csv(self, outfd):
        keys = set()
        for row in self.units.values():
            keys.update(set(row.keys()))
        headers = [
            header for header in all_headers if header in keys
        ]
        writer = csv.DictWriter(outfd, fieldnames=headers)
        writer.writeheader()
        writer.writerows(self.units[unit] for unit in sorted(self.units))


@click.command()
@click.option("--out", type=click.File('w'))
@click.option("--sample-re", default=".*")
@click.option("--folder-re", default=".*")
@click.argument("folders", nargs=-1)
def scan(folders, out, sample_re, folder_re):
    if (out is None):
        raise click.UsageError("--out parameter required")
    scanner = Scanner(folders)
    scanner.set_sample_pattern(sample_re)
    scanner.set_folder_pattern(folder_re)
    scanner.scan()
    scanner.write_csv(out)


if __name__ == "__main__":
    scan()
