FROM fnproject/python:3.8.5-dev as build-stage

ADD requirements.txt requirements-server.txt ./

RUN apt-get update && apt-get install git -y
# pip 20.2.4 is the version before the new resolver was introduced
RUN pip3 install pip==20.2.4 wheel setuptools
RUN pip3 install --target /python/ -r requirements.txt -r requirements-server.txt

FROM fnproject/python:3.8.5

COPY --from=build-stage /python/ /python/
ENV PYTHONPATH=/python

ENTRYPOINT ["bash"]
