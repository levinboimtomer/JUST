#!/usr/bin/python

import argparse
#import subprocess
import sys
import re
import os.path
import IO
from collections import OrderedDict

#
# def exec_cmd(cmd):
#     process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
#     stdout, stderr = process.communicate()
#     return stdout, stderr, process


class Struct():
    pass


def parseArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('main', type=str)
    parser.add_argument('--config', '-c', default=None, type=str, help='global configuration file')
    parser.add_argument('--stages', '-s', default="all", type=str, help='all or 1 or 1-5')
    parser.add_argument('--workdir', type=str, required=True, help='working directory')
    parser.add_argument('--bashheader', default="#!/bin/bash", type=str, help='bash header')
    parser.add_argument('--verbose', action='store_true', help="verbose bash files (-v)")
    parser.add_argument('--debug', action='store_true', help="debug bash files (-x)")
    parser.add_argument('--list', action='store_true', help="list which stages are available")
    parser.add_argument('--qsub', '-q', default=None, type=str, help='qsub queue (works for a single task only)')

    args = parser.parse_args()
    if args.verbose:
        args.bashheader += ' -v'
    if args.debug:
        args.bashheader += ' -x'


    args.files = Struct()
    args.files.main = sys.argv[1]
    args.files.config = args.config

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


def write_body(cmd_args, task_id, config_params, task_body):
    path = "%s/%d.sh" % (cmd_args.workdir, task_id)
    with open(path, 'wb') as f:
        f.write(cmd_args.bashheader + '\n\n')
        # write the config
        f.write('## CONFIG ##\n')
        for vname in config_params:
            assign_line = vname + '=' + str(config_params[vname]) + '\n'
            f.write(assign_line)
        # write the commands.
        f.write('\n## BODY ##\n')
        for line in task_body:
            f.write(line + '\n')
    return path

MSG_PREFIX = "JUST:"
def msg(str):
    print >> sys.stderr, MSG_PREFIX, str


def executeTasks(cmd_args, tasks, config_params):
    do_task = lambda x: cmd_args.all_stages or (x >= cmd_args.start_stage and x <= cmd_args.final_stage)
    for task_id in sorted(tasks.keys()):

        task_name = tasks[task_id]['task_name']
        task_body = tasks[task_id]['task_body']
        if task_id == 0:
            msg("Parsing fixed params in %s task (task_id=%d)." % (task_name, task_id))
            config_params = parseParams(task_body, config_params)
        if do_task(task_id) or task_id == 0:
            msg("Executing task #%d: '%s'" % (task_id, task_name))
            path = write_body(cmd_args, task_id, config_params, task_body)
            os.chmod(path, 0755)    # make executable.

            if cmd_args.qsub is not None:
                path = "qsub -q %s %s" % (cmd_args.qsub, path)
            output = os.system(path)


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
        sys.exit(0)

    executeTasks(cmd_args, tasks, config_params)
