FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

ARG WORKDIR=/template
RUN mkdir -p ${WORKDIR}
COPY main.py /template
COPY requirements.txt /template
WORKDIR ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=main.py

# Install dependencies to launch the pipeline
RUN pip install -U -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE
RUN pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]