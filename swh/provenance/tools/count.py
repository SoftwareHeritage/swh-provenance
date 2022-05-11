#!/usr/bin/env python

import io
import sys


def linecount(filename: str) -> None:
    count = 0
    for _ in io.open(filename).xreadlines():
        count += 1
    return count


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: count <infile>")
        exit(-1)

    print(linecount(sys.argv[1]))
