import functools
import os
import re
import textwrap
import gzip

from typing import Optional, Sequence, Callable

from snakemake.io import Namedlist  # type: ignore
from snakemake.utils import format as snake_format  # type: ignore

from ymp.exceptions import YmpRuleError


def make_local_path(icfg, url: str):
    """Rewrites remote URLs to point to downloads folder so they will be
    retrieved by the download rules

    """
    url_match = re.match("^(http|https|ftp|ftps)://", url)
    if url_match:
        return os.path.join(
            icfg.dir.downloads,
            url_match.group(1),
            url[url_match.end():]
        )
    return url


def is_fq(path):
    return isinstance(path, str) and (
        path.endswith(".fq.gz")
        or path.endswith(".fastq.gz")
        or path.endswith(".fq")
        or path.endswith(".fastq")
    )


def file_not_empty(fn, minsize=1):
    "Checks is a file is not empty, accounting for gz mininum size 20"
    if fn.endswith('gz'):
        with gzip.open(fn, "r") as fd:
            return len(fd.read(1)) > 0
    return os.path.getsize(fn) > 0


def filter_out_empty(*args):
    """
    Removes empty sets of files from input file lists.

    Takes a variable number of file lists of equal length and removes
    indices where any of the files is empty. Strings are converted to
    lists of length 1.

    Returns a generator tuple.

    Example:
    r1, r2 = filter_out_empty(input.r1, input.r2)
    """
    args = ([arg] if isinstance(arg, str) else arg
            for arg in args)
    return zip(*(t for t in zip(*args)
                 if all(map(file_not_empty, t))))


def ensure_list(arg):
    if isinstance(arg, str):
        return [arg]
    return arg


def filter_input(name: str,
                 also: Optional[Sequence[str]] = None,
                 join: Optional[str] = None,
                 minsize: Optional[int] = None) -> Callable:
    def filter_input_func(wildcards, input):
        outfiles = []
        files = ensure_list(getattr(input, name))
        if also is None:
            extra_files = [[None] for _ in files]
        else:
            extra_files = [ensure_list(getattr(input, extra))
                           for extra in ensure_list(also)]
        all_files = [
            fname
            for fnamell in ([files], extra_files)
            for fnamel in fnamell
            for fname in fnamel
            if fname is not None
        ]
        files_exist = [os.path.exists(fname) for fname in all_files]

        if all(files_exist):
            for fname, *extra_fnames in zip(files, *extra_files):
                if isinstance(extra_fnames, str):
                    extra_fnames = [extra_fnames]
                if minsize is not None:
                    if not file_not_empty(fname, minsize=minsize):
                        continue
                    if not all (file_not_empty(fn) for fn in extra_fnames):
                        continue
                outfiles.append(fname)
        elif any(files_exist):
            raise YmpRuleError(None, "Missing files to check for length")
        else:
            outfiles = files
        if join is None:
            return outfiles
        return join.join(outfiles)
    return filter_input_func


def check_input(names: Sequence[str],
                minlines: int = 0,
                minbytes: int = 0) -> Callable:
    def check_input_func(wildcards, input):
        files = [
            fname
            for name in ensure_list(names)
            for fname in ensure_list(getattr(input, name))
        ]
        files_exist = [os.path.exists(fname) for fname in files]

        if all(files_exist):
            if minlines == 0 and minbytes == 0:
                return True
            nbytes = 0
            nlines = 0
            for fname in files:
                if minlines == 0:
                    size = os.path.getsize(fname)
                    if fname.endswith(".gz"):
                        size -= 1000
                    if size > minbytes:
                        continue
                if fname.endswith(".gz"):
                    openfunc = gzip.open
                else:
                    openfunc = open
                try:
                    with openfunc(fname, "rb") as fd:
                        btes = fd.read(8192)
                        while btes:
                            nlines += btes.count(b"\n")
                            nbytes += len(btes)
                            if nbytes >= minbytes and nlines >= minlines:
                                break
                            btes = fd.read(8192)
                    if nbytes < minbytes or nlines < minlines:
                        return False
                except (IOError, EOFError):
                    raise YmpRuleError(
                        None, f"Failed to read file '{fname}'"
                    )
        elif any(files_exist):
            raise YmpRuleError(
                None,
                f"Missing files to check for length: "
                f"{files}"
            )
        return True
    return check_input_func


@functools.lru_cache()
def fasta_names(fasta_file):
    res = []
    with open(fasta_file, "r") as f:
        for line in f:
            if line[0] != ">":
                continue
            res += [line[1:].split(" ", 1)[0]]
    return res


def read_propfiles(files):
    if isinstance(files, str):
        files = [files]
    props = {}
    for file in files:
        with open(file, "r") as f:
            props.update({
                key: int(float(value))
                for line in f
                for key, value in [line.strip().split(maxsplit=1)]
            })
    return props


def glob_wildcards(pattern, files=None):
    """
    Glob the values of the wildcards by matching the given pattern to the
    filesystem.
    Returns a named tuple with a list of values for each wildcard.
    """
    from snakemake.io import _wildcard_regex, namedtuple, regex
    import regex as re

    pattern = os.path.normpath(pattern)
    first_wildcard = re.search("{[^{]", pattern)
    dirname = os.path.dirname(pattern[:first_wildcard.start(
    )]) if first_wildcard else os.path.dirname(pattern)
    if not dirname:
        dirname = "."

    names = [match.group('name')
             for match in _wildcard_regex.finditer(pattern)]
    Wildcards = namedtuple("Wildcards", names)
    wildcards = Wildcards(*[list() for name in names])

    pattern = regex(pattern)
    # work around partial matching bug in python regex module
    # by replacing matches for "\" with "[/\0]" (0x0 can't occur in filenames)
    pattern = re.sub('\\\\/', '[/\0]', pattern)
    cpattern = re.compile(pattern)

    def walker(dirname, pattern):
        """finds files/dirs matching `pattern` in `dirname`"""
        for dirpath, dirnames, filenames in os.walk(dirname):
            dirpath = os.path.normpath(dirpath)
            for f in filenames:
                if dirpath != ".":
                    f = os.path.join(dirpath, f)
                match = pattern.match(f)
                if match:
                    yield match
            for i in range(len(dirnames)-1, -1, -1):
                d = dirnames[i]
                if dirpath != ".":
                    d = os.path.join(dirpath, d)
                match = pattern.match(os.path.join(d, ""), partial=True)
                if not match:
                    del dirnames[i]
                    continue
                if match.partial:
                    continue
                yield match

    print("searching {}".format(pattern))
    if files is None:
        for match in walker(dirname, cpattern):
            for name, value in match.groupdict().items():
                getattr(wildcards, name).append(value)
    else:
        for f in files:
            match = re.match(cpattern, os.normpath(f))
            if match:
                for name, value in match.groupdict().items():
                    getattr(wildcards, name).append(value)
    print("searching {}: done".format(pattern))
    return wildcards


def activate_R():
    from rpy2.robjects import default_converter, conversion
    from rpy2 import robjects, rinterface

    @default_converter.py2ri.register(dict)
    def _1(obj):
        keys = list(obj.keys())
        res = rinterface.ListSexpVector([
            conversion.py2ri(obj[x])
            for x in keys
        ])
        res.do_slot_assign('names', rinterface.StrSexpVector(keys))
        return res

    @default_converter.py2ri.register(tuple)
    def _2(obj):
        return conversion.py2ri(list(obj))

    @default_converter.py2ri.register(list)
    def _3(obj):
        # return sequence_to_vector(obj)
        obj = rinterface.ListSexpVector([conversion.py2ri(x) for x in obj])
        return robjects.r.unlist(obj, recursive=False)


def R(code="", **kwargs):
    """Execute R code

    This function executes the R code given as a string. Additional arguments
    are injected into the R environment. The value of the last R statement
    is returned.

    The function requires rpy2 to be installed.

    Args:
        code (str): R code to be executed
        **kwargs (dict): variables to inject into R globalenv
    Yields:
        value of last R statement

    >>>  R("1*1", input=input)
    """
    try:
        import rpy2.robjects as robjects
        from rpy2.rlike.container import TaggedList
        from rpy2.rinterface import RNULLType
    except ImportError:
        raise ValueError(
            "Python 3 package rpy2 needs to be installed to use"
            "the R function.")

    activate_R()

    # translate Namedlists into rpy2's TaggedList to have named lists in R
    for key in kwargs:
        value = kwargs[key]
        if isinstance(value, Namedlist):
            kwargs[key] = TaggedList([y for x, y in value.allitems()],
                                     [x for x, y in value.allitems()])

    code = snake_format(textwrap.dedent(code), stepout=2)
    # wrap code in function to preserve clean global env and execute
    rval = robjects.r("function({}){{ {} }}"
                      "".format(",".join(kwargs), code))(**kwargs)

    # Reduce vectors of length 1 to scalar, as implicit in R.
    if isinstance(rval, RNULLType):
        rval = None
    if rval and len(rval) == 1:
        return rval[0]
    return rval


def Rmd(rmd, out, **kwargs):
    R("""
    library(rmarkdown)
    print(out)
    rmarkdown::render(rmd, params=paramx, output_file=out)
    """,
      rmd=rmd,
      out=os.path.abspath(out[0]), paramx=kwargs)
