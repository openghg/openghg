#!/bin/bash
set -x
echo Cloning Acqurie
git clone git@github.com:chryswoods/acquire.git
echo Setting environment variable for Acquire
export ACQUIRE_DIR=.
echo Setting