
#ARG TARGET=base

#flex template base image
FROM gcr.io/dataflow-templates-base/python310-template-launcher-base:public-image-latest as template_launcher
FROM nvidia/cuda:12.2.2-runtime-ubuntu22.04 as base


# Install python and pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update
RUN apt-get install -y apt-transport-https ca-certificates gnupg curl gcc g++

# Install git.
RUN apt-get install -y git

# Install dependencies.
RUN pip install flit
RUN pip install --upgrade pip

# Configure the Flex Template here.
COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher
COPY mainGPU.py /template/
COPY requirements.txt /template/
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/mainGPU.py"

# Copy the Apache Beam worker dependencies from the Beam Python 3.6 SDK image.
COPY --from=apache/beam_python3.9_sdk:2.52.0 /opt/apache/beam /opt/apache/beam
# Set the entrypoint to Apache Beam SDK worker launcher.
ENTRYPOINT [ "/opt/apache/beam/boot" ]

