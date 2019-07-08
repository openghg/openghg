#!/bin/bash

rsync -a --verbose ../../hugs . --exclude '__pycache__'
rsync -a --verbose ../admin . --exclude '__pycache__'

docker build -t chryswoods/hugs-base:latest .

rm -rf hugs admin
