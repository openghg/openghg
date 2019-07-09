FROM chryswoods/acquire-base:latest

WORKDIR /function
USER root

ADD hugs $PYTHON_EXT/hugs

RUN python3 -m compileall $PYTHON_EXT/hugs/*

ENTRYPOINT ["bash"]

# Become the $FN_USER so that nothing runs as root
USER $FN_USER
