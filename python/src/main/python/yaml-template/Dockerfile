FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

ARG WORKDIR=/template
RUN mkdir -p ${WORKDIR}
COPY main.py /template
COPY requirements.txt /template
WORKDIR ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=main.py

# Install dependencies to launch the pipeline and download to reduce startup time
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -U -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Avoid downloading dependencies with pip to reduce startup time
ENV PIP_NO_DEPS=True

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]