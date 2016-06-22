__author__ = 'tomerlevinboim'
import sys

def readlines(filename, strip=False, skip=[]):
    lines = []
    with open(filename, 'rb') as f:
        for (i, line) in enumerate(f):
            if strip:
                line = line.strip()
            if 'empty' in skip and len(line) == 0: continue
            if 'comment' in skip and line.startswith('#'): continue

            lines.append(line)

    return lines

def logError(*args):
    print >> sys.stderr, " ".join(args)
