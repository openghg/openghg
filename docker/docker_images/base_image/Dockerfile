FROM fnproject/python:3.8.5

ADD requirements.txt /function/
WORKDIR /function

RUN apt-get update && apt-get install git -y
RUN pip3 install --upgrade pip wheel setuptools
RUN pip3 install --target /python/  --no-cache --no-cache-dir -r requirements.txt

RUN rm -fr ~/.cache/pip /tmp* requirements.txt func.yaml Dockerfile .venv

ENTRYPOINT ["bash"]


# FROM fnproject/python:3.8.5

# COPY --from=build-stage /function /function
# COPY --from=build-stage /python /python
# ENV PYTHONPATH=/python

# ENTRYPOINT ["/python/bin/fdk", "/function/func.py", "handler"]