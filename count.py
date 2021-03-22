#!/usr/bin/env python

import sys


def linecount(filename):
    count = 0
    for line in open(filename).xreadlines():
        count += 1
    return count


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: count <infile>")
        exit(-1)

    print(linecount(sys.argv[1]))
