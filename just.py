#!/usr/bin/python

import argparse
#import subprocess
import sys
import re
import os.path
import IO
from collections import OrderedDict


QSUB_HEADER = """#$ -M tlevinbo@nd.edu
#$ -m ae
#$ -r n
#$ -pe smp 16

date
fsync -d 10 $SGE_STDOUT_PATH &   # updated log every 20 seconds
##
"""

class Struct():
    pass


def parseArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('main', type=str)
    parser.add_argument('--workdir', type=str, required=True, help='working directory')
    parser.add_argument('--stages', '-s', default="all", type=str, help='all or 1 or 1-5')
    parser.add_argument('--evaluate', '-e', default=None, type=str, help="a bash command evaluated before each task")
    parser.add_argument('--bashheader', default="#!/bin/bash", type=str, help='bash header')
    parser.add_argument('--verbose', '-v', action='store_true', help="verbose bash files (-v)")
    parser.add_argument('--debug', '-x', action='store_true', help="debug bash files (-x)")
    parser.add_argument('--list', action='store_true', help="list which stages are available")
    parser.add_argument('--qsub', '-q', default=None, type=str, help='qsub queue (works for a single task only)')

    args = parser.parse_args()
    if args.verbose:
        args.bashheader += ' -v'
    if args.debug:
        args.bashheader += ' -x'

    args.files = Struct()
    args.files.main = sys.argv[1]

    args.start_stage = -1
    args.final_stage = -1
    args.all_stages = args.stages.lower() == 'all'
    if not args.all_stages:
        args.all_stages = False
        stages = args.stages.split('-')
        args.start_stage = int(stages[0])
        args.final_stage = int(stages[0])   # default to only running this task
        if '-' in args.stages:              # unless there's a range, indicated by '-'
            args.final_stage = int(stages[1])
    return args


def parseTasks(args):
    lines = IO.readlines(args.files.main, strip=True, skip=['empty', 'comment'])
    tasks = dict()

    is_in = False
    task_id = None
    task_name = None
    task_body = []
    for line in lines:
        m_start = re.match(r"(.*):(.*):.*{{", line)
        m_end = re.match(r"^}}\s*$", line)

        if m_end:
            is_in = False
            task_id = int(task_id)
            tasks[task_id] = {'task_name': task_name, 'task_body': task_body}
            task_body = []

        if is_in:
            task_body.append(line)

        if m_start:
            is_in = True
            task_id, task_name = m_start.groups(0)
    return tasks


def parseParams(lines, params=None):
    if params is None:
        params = OrderedDict()  # order of insertion matters.
    for i, line in enumerate(lines):
        if '=' not in line: IO.logError('Error: cannot parse line %d:\n%s' % (i, line))
        vname, value = line.split('=')
        if ':' in vname:
            vname, vtype = vname.split(':')
            if vtype == 'int': value = int(value)
            if vtype == 'float': value = float(value)
            if vtype == 'ifile': assert os.path.isfile(value), "file '%s' does not exist" % value

        params[vname] = value
    return params


def write_body(cmd_args, config_params, task_id, task_name, task_body):
    # Files are saved in the working directory, under the name t$id.$name.sh
    # note 1: qsub fails on file names starting with a digit.
    # note 2: shorter names are better for qstat which displays the first 10 characters of the submitted script name
    path = "%s/t%d.%s.sh" % (cmd_args.workdir, task_id, task_name)
    with open(path, 'wb') as f:
        f.write(cmd_args.bashheader + '\n\n')                               # bash header

        if task_id != 0 and cmd_args.qsub is not None:                      # write the qsub header if required
            f.write(QSUB_HEADER)

        if cmd_args.evaluate is not None:
            f.write(cmd_args.evaluate + "# --evaluate")
        # write the parsed config
        f.write('## CONFIG ##\n')
        for vname in config_params:
            assign_line = vname + '=' + str(config_params[vname]) + '\n'
            f.write(assign_line)
        # write the commands.
        if task_id != 0:
            f.write('\n## BODY ##\n')
            for line in task_body:
                f.write(line + '\n')
    return path


MSG_PREFIX = "JUST:"
def msg(str):
    print >> sys.stderr, MSG_PREFIX, str


def executeTasks(cmd_args, tasks, config_params):
    do_task = lambda x: cmd_args.all_stages or (x >= cmd_args.start_stage and x <= cmd_args.final_stage)
    qsub_id = None
    for task_id in sorted(tasks.keys()):

        task_name = tasks[task_id]['task_name']
        task_body = tasks[task_id]['task_body']
        if task_id == 0:
            msg("Parsing fixed params in %s task (task_id=%d)." % (task_name, task_id))
            config_params = parseParams(task_body, config_params)
        if do_task(task_id) or task_id == 0:
            msg("Executing task #%d: '%s'" % (task_id, task_name))
            path = write_body(cmd_args, config_params, task_id, task_name, task_body)
            os.chmod(path, 0755)    # make executable.

            if task_id != 0 and cmd_args.qsub is not None:
                depend = "" if qsub_id is None else " -hold_jid %d" % qsub_id # depend on previous qsub task id (Univa grid)
                path = "qsub %s -q %s %s" % (depend, cmd_args.qsub, path)
            # execute
            if cmd_args.qsub:
                output = os.popen(path).read()  # TODO, change to subprocess
                msg("`output:\n" + output)
            else:
                output = os.system(path)

            if cmd_args.qsub is not None:
                possible_ids = [int(s) for s in output.split() if s.isdigit()]
                if len(possible_ids) > 0: qsub_id = possible_ids[0]  # extract qsub task id

    return output


if __name__ == '__main__':
    cmd_args = parseArgs()
    tasks = parseTasks(cmd_args)
    if not os.path.exists(cmd_args.workdir):
        msg("creating directory " + cmd_args.workdir)
        os.makedirs(cmd_args.workdir)

    config_params = parseParams([], OrderedDict({'workdir': cmd_args.workdir}))

    if cmd_args.list:
        for task_id, task_params in tasks.items():
            msg("stage %d: %s" % (task_id, task_params['task_name']))
    else:
        executeTasks(cmd_args, tasks, config_params)
