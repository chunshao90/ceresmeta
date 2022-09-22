#!/usr/bin/env bash
# Check all source files have a license header.
set -eu

for i in $(git ls-files --exclude-standard | grep "\.go$"); do
    # first line -> match -> print line -> quit
    matches=$(sed -n "1{/Copyright [0-9]\{4\} CeresDB Project Authors. Licensed under Apache-2.0./p;};q;" $i)
    if [ -z "${matches}" ]; then
        echo "License header is missing from $i."
        exit 1
    fi
done