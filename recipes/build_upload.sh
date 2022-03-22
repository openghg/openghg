#!/bin/bash
set -xe

# Get the filename to upload
BUILD_DIR=${CONDA}/envs/openghg-build/conda-bld/noarch

# For now we only have one build
BUILD=$(find "$BUILD_DIR" -name *.tar.bz2)

# Upload the file to anaconda
anaconda \
    --token "$ANACONDA_TOKEN" upload \
    --user openghg \
    --label main \
    "$BUILD"