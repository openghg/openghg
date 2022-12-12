#!/bin/bash
set -xe

# Get the filename to upload
BUILD_DIR=${GITHUB_WORKSPACE}/build

# For now we only have one build
BUILD=$(find "$BUILD_DIR" -name '*.tar.bz2')

# Upload the file to anaconda
anaconda \
    --token "$ANACONDA_TOKEN" upload \
    --user openghg \
    --label main \
    "$BUILD"
