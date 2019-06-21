FROM chryswoods/acquire-base:latest

# Need to be user root or Fn exits with
# {"message":"internal server error"}
USER root

WORKDIR $HOME
RUN mkdir $PYTHON_EXT/hugs

ADD *.py $PYTHON_EXT/hugs/
RUN python3 -m compileall $PYTHON_EXT/hugs/*.py

ADD route.py secret_key ./

ENTRYPOINT ["python", "route.py"]
