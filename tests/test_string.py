import re
import string
from itertools import product

import pytest

import ymp.string

MERGED = 0
PRODUCT = 1


class FormatterTest(object):
    pattern = "{A} and {B}"
    fields = {'A': [1, 2], 'B': [3, 4]}
    result = '[1, 2] and [3, 4]'
    formatter = string.Formatter()

    def format(self, pattern, *args, **kwargs):
        return self.formatter.format(pattern, *args, **kwargs)

    def test_formatting(self):
        assert self.format(self.pattern, **self.fields) == self.result


class OverrideJoinFormatterTest(FormatterTest):
    formatter = ymp.string.OverrideJoinFormatter()


class ProductFormatterTest(FormatterTest):
    formatter = ymp.string.ProductFormatter()
    result = ['1 and 3', '1 and 4', '2 and 3', '2 and 4']


class RegexFormatterTest(FormatterTest):
    regex = re.compile(
        r"""
        \{
            (?=(
                (?P<name>[^{}]+)
            ))\1
        \}
        """, re.VERBOSE)
    formatter = ymp.string.RegexFormatter(regex)


class PartialFormatterTest(FormatterTest):
    formatter = ymp.string.PartialFormatter()

    def test_partial_formatting(self):
        pat = self.pattern
        for key in self.fields:
            pat = self.format(pat, **{key: self.fields[key]})
        assert pat == self.result


@pytest.fixture(params=[
    "{A} and {B}"
])
def pattern(request):
    return request.param


@pytest.fixture(params=[
    {'A': [1, 2], 'B': [3, 4]},
    {'A': ['1', '2'], 'B': ['3', '4']},
    {'A': 1, 'B': [3, 4]},
    {'A': '12', 'B': [3, 4]}
], ids=[
    'numbers',
    'strings',
    'int',
    'string'
])
def values(request):
    return request.param


@pytest.fixture(params=[product, None], ids=['Product', 'Flatten'])
def combine(request, pattern, values):
    if request.param is None:
        result = pattern.format(**values)
    else:
        result = [
            pattern.format(**dict(zip(values, x)))
            for x in request.param(*(value
                                     if isinstance(value, list)
                                     else (value,)
                                     for value in values.values()))
        ]

    return {
        'product': 1 if request.param is not None else None,
        'result': result,
        'pattern': pattern,
        'values': values
    }


@pytest.fixture(params=[
    (None, "{", "}"),
    (r"\{(?=((?P<name>[^{}]+)))\1\}", "{", "}"),
    (r"\{:(?=(\s*(?P<name>(?:.(?!\s*\:\}))*.)\s*))\1:\}", "{:", ":}"),
], ids=[
    'NoRegex',
    'Braces',
    'BraceColon'
])
def combine_regex(request, combine):
    combine['regex'] = request.param[0]
    combine['pattern'] = (combine['pattern']
                          .replace("{", request.param[1])
                          .replace("}", request.param[2])
                          )
    return combine


def test_make_formatter(combine_regex):
    do_product = combine_regex['product']
    do_regex = combine_regex['regex']
    values = combine_regex['values']
    pattern = combine_regex['pattern']
    expected_result = combine_regex['result']

    formatter = ymp.string.make_formatter(
        product=do_product,
        regex=do_regex,
        partial=None)
    result = formatter.format(pattern, **values)
    assert result == expected_result
