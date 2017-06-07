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
    return tasks


def write_body(cmd_args, prologue_body, task_id, task_name, task_body):
    # Files are saved in the working directory, under the name s$id.$name.sh
    # note 1: qsub fails on file names starting with a digit.
    # note 2: shorter names are better for qstat which displays the first 10 characters of the submitted script name
    path = "%s/s%d.%s.sh" % (cmd_args.workdir, task_id, task_name)
    with open(path, 'wb') as f:

        f.write(cmd_args.bashheader + '\n\n')                               # bash header

        if cmd_args.evaluate is not None:
            f.write(cmd_args.evaluate + "# JUST: user evaluate input\n")

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


def executeTasks(cmd_args, tasks):
    do_task = lambda x: cmd_args.all_stages or (x >= cmd_args.start_stage and x <= cmd_args.final_stage)
    qsub_id = None
    prologue_body = []
    for task_id in sorted(tasks.keys()):

        task_name = tasks[task_id]['task_name']
        task_body = tasks[task_id]['task_body']
        if task_id == 0:
            prologue_body = task_body

        elif do_task(task_id):
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
