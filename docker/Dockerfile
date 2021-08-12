FROM openghg/openghg-base:latest

ADD openghg_services/route.py /function/
WORKDIR /function

# Copy in openghg
ADD openghg /python/openghg
RUN python3 -m compileall /python/openghg/*

# Copy in the service files
RUN mkdir /python/openghg_services
ADD openghg_services /python/openghg_services
RUN python3 -m compileall /python/openghg_services/*.py

ENV PYTHONPATH=/python
ENV OPENGHG_CLOUD=1
# Become the $FN_USER so that nothing runs as root
USER $FN_USER

ENTRYPOINT ["/python/bin/fdk", "/function/route.py", "handle_invocation"]
