#!/usr/bin/env bash

# This script moves files to the trash directory

# if exists then move to trash

if [ -e ./users.parquet ]; then
    trash ./users.parquet
fi

if [ -e ./users.delta ]; then
    trash ./users.delta
fi


