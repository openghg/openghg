FROM openghg/openghg-base:latest

# Just need to add in OpenGHG
WORKDIR /function
USER root

# Copy in openghg and services
ADD openghg $PYTHON_EXT/openghg

RUN mkdir $PYTHON_EXT/openghg_service
ADD openghg/openghg_service/*.py $PYTHON_EXT/openghg_service/



RUN python3 -m compileall $PYTHON_EXT/openghg/*


ENTRYPOINT ["bash"]

# Become the $FN_USER so that nothing runs as root
USER $FN_USER
