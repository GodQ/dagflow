__author__ = 'godq'
import subprocess
from subprocess import PIPE, STDOUT


def run_cmd(command, daemon=False):
    process = subprocess.Popen(command, stdin=PIPE, stdout=PIPE,
                             stderr=STDOUT, shell=True, bufsize=0,
                             universal_newlines=True)
    lines = list()
    while True:
        output = process.stdout.readline()
        if not output and process.poll() is not None:
            break
        output = output.strip()
        if output:
            if daemon is True:
                print(output)
            else:
                lines.append(output)

    return_code = process.poll()
    return return_code, "\n".join(lines)

