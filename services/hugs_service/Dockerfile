FROM chryswoods/hugs-base:latest

# Need to be user root or Fn exits with
# {"message":"internal server error"}
USER root

WORKDIR $HOME
RUN mkdir $PYTHON_EXT/hugs_service

ADD *.py $PYTHON_EXT/hugs_service/
RUN python3 -m compileall $PYTHON_EXT/hugs_service/*.py

ADD route.py secret_key ./

ENTRYPOINT ["python", "route.py"]
