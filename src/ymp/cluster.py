"""
Module handling talking to cluster management systems

>>> python -m ymp.cluster slurm status <jobid>
"""

import subprocess as sp
import sys
import re
import logging

log = logging.getLogger(__name__)  # pylint: disable=invalid-name

ATTEMPTS = 20
DEFAULT_STATE = "running"

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
        'TIMEOUT':     'failed',   # job reached time limit
        'OUT_OF_MEMORY': 'failed', # job ran out of memory
        # questionable states:
        'SPECIAL_EXIT': 'running',  # job failed but flagged "special_exit"
        'REVOKED': 'running'  # job removed due to other cluster starting it
        # Both of the above technically mean that the job isn't "done" yet,
        # running either on another cluster or on hold for a re-run after
        # initial failure.
    }

    @staticmethod
    def run_sacct(jobid):
        try:
            res = sp.run(['sacct', '-pbj', jobid], stdout=sp.PIPE)
        except sp.CalledProcessError as exc:
            log.error("Failed to run sacct: %s", exc)
            return {}
        try:
            lines = [line.strip().split("|")
                     for line in res.stdout.decode().splitlines()]
            header = lines.pop(0)
            state_idx = header.index("State")
            jobid_idx = header.index("JobID")
            return {
                line[jobid_idx]: line[state_idx].split(" ", 1)[0]
                for line in lines
            }
        except IndexError as exc:
            log.error("Failed to parse sacct: %s", exc)
            return {}

    @staticmethod
    def run_scontrol(jobid):
        try:
            res = sp.run(['scontrol', '-o', 'show', 'job', jobid], stdout=sp.PIPE)
        except sp.CalledProcessError as e:
            log.error("Failed to run scontrol: %s", e)
            return {}
        try:
            return {jobid: re.search("JobState=(\w+)", res.stdout.decode()).group(1)}
        except (AttributeError, IndexError) as exc:
            log.error("Failed to parse sacct: %s", exc)
            return {}

    @classmethod
    def status(cls, jobid):
        """Print status of job @param jobid to stdout (as needed by snakemake)

        Anectotal benchmarking shows 200ms per invocation, half used
        by Python startup and half by calling sacct. Using ``scontrol
        show job`` instead of ``sacct -pbs`` is faster by 80ms, but
        finished jobs are purged after unknown time window.
        """
        for i in range(ATTEMPTS):
            jobs = cls.run_sacct(jobid)
            if jobid not in jobs:
                jobs = cls.run_scontrol(jobid)
            if jobid in jobs:
                slurmstate = jobs[jobid]
                snakestate = cls.states[slurmstate]
                print(snakestate)
                sys.exit(0)
            time.sleep(1)
        log.error("Failed to obtain job info after %i attempts, claiming job %s", ATTEMPTS, DEFAULT_STATE)
        print(DEFAULT_STATE)
        sys.exit(0)


class Lsf(ClusterMS):
    "Talking to LSF"

    states = {
        'PEND': 'running',
        'RUN': 'running',
        'DONE': 'success',
        'PSUSP': 'running',
        'USUSP': 'running',
        'SSUSP': 'running',
        'WAIT': 'running',
        'EXIT': 'failed',
        'POST_DONE': 'success',
        'POST_ERR': 'failed',
        'UNKWN': 'running',
    }

    @staticmethod
    def status(jobid):
        header = None
        jobs = []
        res = sp.run(['bjobs', jobid], stdout=sp.PIPE, stderr=sp.PIPE)
        for line in res.stdout.decode('ascii').splitlines():
            if line[0] == " ":
                continue
            if header is None:
                colstarts = [(x, line.index(x)) for x in line.strip().split()]
                endcol = None
                header = {}
                for col in reversed(colstarts):
                    header[col[0]] = (col[1], endcol)
                    endcol=col[1]
                continue
            try:
                start, end = header['STAT']
                status = line[start:end].strip()
                snakestate = Lsf.states[status]
                print(snakestate)
                sys.exit(0)
            except (KeyError, ValueError) as e:
                error(e)
                for l in res.stdout.splitlines():
                    print(b"STDOUT: " + l)
                for l in res.stderr.splitlines():
                    print(b"ERROUT: " + l)
                raise
        print(Lsf.states['EXIT'])
        sys.exit(0)

    @staticmethod
    def submit(args):
        res = sp.run(['bsub'] + args, stdout=sp.PIPE)
        match = re.search("Job <(\d+)>", res.stdout.decode('ascii'))
        if match:
            print(match.group(1))
        else:
            print(res.stdout)
            return -1


if __name__ == "__main__":
    if len(sys.argv) < 3:
        error("""{self} ENGINE COMMAND ...

        Talks to cluster engine.

        Supported engines:

          - slurm
          - lsf

        Supported commands:

          - status <jobid> Returns running|success|failed status for
          - submit <args>

        jobid to be used by snakemake.
        """.format(self=sys.argv[0]))
        sys.exit(0 if sys.argv[1] == "-h" else 1)

    engine_name = sys.argv[1]
    command = sys.argv[2]

    if engine_name == "slurm":
        engine = Slurm()
    elif engine_name == "lsf":
        engine = Lsf()
    else:
        error("Don't know about cluster engine named '{}'".format(engine_name))
        sys.exit(1)

    if command == "status":
        if len(sys.argv) != 4:
            error("Status command needs exactly one argument, the job id.")
            sys.exit(1)
        engine.status(sys.argv[3])
    if command == "submit":
        engine.submit(sys.argv[3:])
    else:
        error("Command '{}' not understood.".format(command))
        sys.exit(1)
