FROM fnproject/python:3.7

USER root

ADD requirements.txt .

# Create the directory structure that exists locally so the adding to path
# functions normally
RUN mkdir hugs
ADD ./HUGS HUGS
ADD ./Acquire Acquire

RUN pip3 install --upgrade pip

RUN pip3 install --no-cache --no-cache-dir -r requirements.txt && \
	rm -rf ~/.cache/pip /tmp* requirements.txt func.yaml Dockerfile .venv

RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension
RUN jupyter nbextension install --py --symlink --sys-prefix bqplot
RUN jupyter nbextension enable --py --sys-prefix bqplot

# Add the user that will run voila
RUN useradd -m -s /bin/bash -N vuser

# RUN mkdir -p interface/notebooks
USER vuser

ADD interface_container.ipynb .

RUN jupyter trust interface_container.ipynb
# Become the $V_USER so that nothing runs as root


CMD ["voila", "interface_container.ipynb", "--no-browser"]