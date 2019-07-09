FROM chryswoods/acquire-base:latest

WORKDIR /function
USER root

ADD HUGS $PYTHON_EXT/HUGS

RUN python3 -m compileall $PYTHON_EXT/HUGS/*

ENTRYPOINT ["bash"]

# Become the $FN_USER so that nothing runs as root
USER $FN_USER
