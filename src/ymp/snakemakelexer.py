"""
ymp.snakemakelexer
~~~~~~~~~~~~~~~~~~

"""

from pygments.lexers.python import Python3Lexer
from pygments.token import Keyword, Text, Name
from pygments.lexer import bygroups, inherit, words, include


class SnakemakeLexer(Python3Lexer):
    name = 'Snakemake'
    tokens = {
        'root': [
            (r'(rule)((?:\s|\\\s)+)', bygroups(Keyword, Text), 'rulename'),
            include('rulekeyword'),
            include('globalkeyword'),
            inherit,
        ],
        'rulename': [
            ('[a-zA-Z_]\w*', Name.Class, '#pop')
        ],
        'rulekeyword': [
            (words((
                'benchmark', 'conda', 'input', 'log', 'message', 'output',
                'params', 'priority', 'resources', 'shadow', 'shell',
                'singularity', 'threads', 'version', 'wildcard_constraints',
                'wrapper', 'run', 'script'
            ), suffix=r'(?=\s*:)'), Keyword),
        ],
        'globalkeyword': [
            (words((
                'include', 'workdir', 'configfile', 'ruleorder',
                'global_wildcard_constraints', 'subworkflow',
                'localrules', 'onsuccess', 'onerror', 'onstart'
                ), suffix=r'\b'), Keyword),
        ]
    }

# uncomment to print token stream for debug
#    def get_tokens_unprocessed(self, text):
#        import sys
#        print("\n".join(str(l) for l in super().get_tokens_unprocessed(text)), file=sys.stderr)
#        yield from super().get_tokens_unprocessed(text)
