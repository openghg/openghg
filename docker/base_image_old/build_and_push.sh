#!/bin/bash
rsync -a --verbose ../../HUGS . --exclude '__pycache__'
rsync -a --verbose ../admin . --exclude '__pycache__'

docker build --network=host --no-cache -t chryswoods/hugs-base:latest .

rm -rf HUGS admin
