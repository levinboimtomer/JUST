#!/usr/bin/python

import argparse
import sys
import re
import os.path
import subprocess

import IO

class Struct():
    pass


def parseArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('main', type=str)
    parser.add_argument('--workdir', type=str, required=True, help='working directory')
    parser.add_argument('--stages', '-s', default="-", type=str, help='1 or 1-5 (default all)')
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

    return args


def parseTasks(args):
    lines = IO.readlines(args.main, strip=True, skip=['empty'])
    tasks = dict()

    is_in = False
    task_id = None
    task_name = None
    task_body = []
    for line in lines:
        m_start = re.match(r"(.*):(.*):.*{{", line)
        m_end = re.match(r"^#*}}\s*$", line)

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
            if '-' in task_name:
                msg("WARNING: task name should not contain -")
    return tasks


def write_body(cmd_args, prologue_body, task_id, task_name, task_body):
    # Files are saved in the working directory, under the name s$id.$name.sh
    # note 1: qsub fails on file names starting with a digit.
    # note 2: shorter names are better for qstat which displays the first 10 characters of the submitted script name
    path = "%s/s%d.%s.sh" % (cmd_args.workdir, task_id, task_name)
    with open(path, 'wb') as f:

        f.write(cmd_args.bashheader + '\n\n')                               # bash header

        if cmd_args.evaluate is not None:
            f.write('\n## From -e option ##\n')
            f.write(cmd_args.evaluate + '\n')

        # write the parsed config
        f.write('## CONFIG ##\n')
        for line in prologue_body:
            f.write(line + '\n')

        # write the commands.
        f.write('\n## BODY ##\n')
        for line in task_body:
            f.write(line + '\n')
    return path


MSG_PREFIX = "JUST:"
def msg(str):
    print >> sys.stderr, MSG_PREFIX, str


def lookupTask(task_id, tasks):
    try:
        task_id = int(task_id)
        if task_id not in tasks:
            raise ValueError("task {} not found".format(task_id))
        return task_id
    except ValueError:
        cands = [i for i in tasks if tasks[i]['task_name'] == task_id]
        if len(cands) == 0:
            raise ValueError("task {} not found".format(task_id))
        elif len(cands) > 1:
            raise ValueError("task name {} is ambiguous".format(task_id))
        return cands[0]

def executeTasks(cmd_args, tasks):

    try:
        start, stop = cmd_args.stages.split('-', 1)
    except ValueError:
        start = stop = cmd_args.stages

    if start == "": start = min(t for t in tasks if t > 0)
    if stop == "": stop = max(tasks)

    try:
        start = lookupTask(start, tasks)
        stop = lookupTask(stop, tasks)
    except ValueError as e:
        msg(e)
        sys.exit(1)

    qsub_id = None
    prologue_body = []
    for task_id in sorted(tasks.keys()):

        task_name = tasks[task_id]['task_name']
        task_body = tasks[task_id]['task_body']
        if task_id == 0:
            prologue_body = task_body

        elif start <= task_id <= stop:
            msg("Executing task #%d: '%s'" % (task_id, task_name))
            path = write_body(cmd_args, prologue_body, task_id, task_name, task_body)

            if cmd_args.qsub is not None:
                cmd = ['qsub']
                if qsub_id is not None:
                    cmd.extend(['-hold_jid', str(qsub_id)])
                cmd.extend(['-q', cmd_args.qsub])
                cmd.extend(['-N', "s{}.{}.wd={}".format(task_id, task_name, cmd_args.workdir)])
                cmd.extend(['-v', 'workdir='+cmd_args.workdir])
                cmd.append(path)
                msg("running: " + ' '.join(cmd))
                output = subprocess.check_output(cmd)
                msg("qsub output:\n" + output)
                possible_ids = [int(s) for s in output.split() if s.isdigit()]
                if len(possible_ids) > 0: qsub_id = possible_ids[0]  # extract qsub task id

            else:
                os.chmod(path, 0755)    # make executable.
                env = dict(os.environ)
                env['workdir'] = cmd_args.workdir
                status = subprocess.check_call(path, env=env)


def make_dir(input_dir):
    if not os.path.exists(input_dir):
        msg("Creating directory " + input_dir)
        os.makedirs(input_dir)


if __name__ == '__main__':
    cmd_args = parseArgs()
    tasks = parseTasks(cmd_args)
    make_dir(cmd_args.workdir)


    if cmd_args.list:
        # output the list of tasks.
        for task_id, task_params in tasks.items():
            msg("stage %d: %s" % (task_id, task_params['task_name']))
    else:
        executeTasks(cmd_args, tasks)
