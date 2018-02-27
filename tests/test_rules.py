import logging

from .data import parametrize_target

log = logging.getLogger(__name__)


@parametrize_target(large=False, exclude_targets=['phyloFlash'])
def test_run_rules(target):
    from click.testing import CliRunner
    from ymp.cli import make as ymp_make
    runner = CliRunner()
    result = runner.invoke(ymp_make, ["-j2", target])
    if result.exit_code != 0:
        for line in result.output.splitlines():
            log.error("out: %s", line)
    assert result.exit_code == 0, result.output
