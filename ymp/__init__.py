from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    from setuptools_scm import get_version
    __version__ = get_version(root="..", relative_to=__file__)

__release__ = __version__

# Set to 1 to print the next rule during parsing (debug)
print_rule = 0
