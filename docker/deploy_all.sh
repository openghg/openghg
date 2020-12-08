#!/bin/bash

cd base_image && ./build_and_push.sh && cd -
cd hugs_service && fn --verbose deploy --local --all && cd -
