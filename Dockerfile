
#ARG TARGET=base

#flex template base image
FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:public-image-latest as template_launcher
FROM nvidia/cuda:12.2.2-runtime-ubuntu22.04 as base

# Configure the Flex Template here.
COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
COPY mainGPU.py /template/
COPY requirements.txt /template/
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/mainGPU.py"


# Install python and pip
RUN apt-get update \
    # Install Python and other system dependencies.
    && apt-get install -y --no-install-recommends \
        curl g++ python3.10-dev python3.10-venv python3-distutils \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.10 10 \
    && curl https://bootstrap.pypa.io/get-pip.py | python \
    # Install the pipeline requirements.
    && pip install --no-cache-dir -r requirements.txt \
    && pip check

# Copy the Apache Beam worker dependencies from the Beam Python 3.6 SDK image.
COPY --from=apache/beam_python3.10_sdk:2.52.0 /opt/apache/beam /opt/apache/beam
# Set the entrypoint to Apache Beam SDK worker launcher.
ENTRYPOINT [ "/opt/apache/beam/boot" ]

