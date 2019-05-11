"""
Module handling talking to cluster management systems

>>> python -m ymp.cluster slurm status <jobid>
"""

import subprocess as sp
import sys


def error(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class ClusterMS(object):
    pass


class Slurm(ClusterMS):
    "Talking to Slurm"

    states = {
        'BOOT_FAIL':   'failed',   # node launch failure
        'CANCELLED':   'failed',   # killed manually or by resource monitoring
        'COMPLETED':   'success',  # terminated with exit code 0
        'CONFIGURING': 'running',  # resources allocated, waiting for node
        'COMPLETING':  'running',  # some processes still active
        'DEADLINE':    'failed',   # job ran into deadline and was killed
        'FAILED':      'failed',   # job exited with code <> 0 or similar
        'NODE_FAIL':   'failed',   # job terminated due to node(s) failed
        'PENDING':     'running',  # job waiting for resources
        'PREEMPTED':   'failed',   # job terminated to make room
        'RUNNING':     'running',  # job has allocation and should be working
        'RESIZING':    'running',  # job is about to change size
        'SUSPENDED':   'running',  # job is paused
        'TIMEOUT': 'failed',       # job reached time limit
        # questionable states:
        'SPECIAL_EXIT': 'running',  # job failed but flagged "special_exit"
        'REVOKED': 'running'  # job removed due to other cluster starting it
        # Both of the above technically mean that the job isn't "done" yet,
        # running either on another cluster or on hold for a re-run after
        # initial failure.
    }

    @staticmethod
    def status(jobid):
        """Print status of job @param jobid to stdout (as needed by snakemake)

        Anectotal benchmarking shows 200ms per invocation, half used
        by Python startup and half by calling sacct. Using ``scontrol
        show job`` instead of ``sacct -pbs`` is faster by 80ms, but
        finished jobs are purged after unknown time window.
        """

        header = None
        jobs = []
        res = sp.run(['sacct', '-pbj', jobid], stdout=sp.PIPE)
        for line in res.stdout.decode('ascii').splitlines():
            line = line.strip().split("|")
            if header is None:
                header = line
                continue
            try:
                job = {key: line[header.index(key)]
                       for key in ('JobID', 'State', 'ExitCode')}
                job['snakestate'] = Slurm.states[job['State'].split(' ')[0]]
                jobs.append(job)
            except ValueError as e:
                error(e)
                error(res.stdout)
                sys.exit(1)
        snakestates = [job['snakestate'] for job in jobs]
        if 'running' in snakestates:
            print('running')
        elif 'failed' in snakestates:
            print('failed')
        else:
            print('success')
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        error("""{self} ENGINE COMMAND ...

        Talks to cluster engine.

        Supported engines:

          - slurm

        Supported commands:

          - status <jobid> Returns running|success|failed status for
        jobid to be used by snakemake.
        """.format(self=sys.argv[0]))
        sys.exit(0 if sys.argv[1] == "-h" else 1)

    engine_name = sys.argv[1]
    command = sys.argv[2]

    if engine_name == "slurm":
        engine = Slurm()
    else:
        error("Don't know about cluster engine named '{}'".format(engine_name))
        sys.exit(1)

    if command == "status":
        if len(sys.argv) != 4:
            error("Status command needs exactly one argument, the job id.")
            sys.exit(1)
        engine.status(sys.argv[3])
    else:
        error("Command '{}' not understood.".format(command))
        sys.exit(1)
