ARG BASE=tensorflow/tensorflow:2.5.0-gpu
FROM $BASE

# Check that the chosen base image provides the expected version of Python interpreter.
ARG PY_VERSION=3.6
RUN [[ $PY_VERSION == `python -c 'import sys; print("%s.%s" % sys.version_info[0:2])'` ]] \
   || { echo "Could not find Python interpreter or Python version is different from ${PY_VERSION}"; exit 1; }

RUN pip install --upgrade pip \
    && pip install --no-cache-dir apache-beam[gcp]==2.29.0 \
    && pip install nvidia-ml-py
    # Verify that there are no conflicting dependencies.
    && pip check

# Copy the Apache Beam worker dependencies from the Beam Python 3.6 SDK image.
COPY --from=apache/beam_python3.6_sdk:2.29.0 /opt/apache/beam /opt/apache/beam

# Apache Beam worker expects pip at /usr/local/bin/pip by default.
# Some images have pip in a different location. If necessary, make a symlink.
# This line can be omitted in Beam 2.30.0 and later versions.
RUN [[ `which pip` == "/usr/local/bin/pip" ]] || ln -s `which pip` /usr/local/bin/pip

# Set the entrypoint to Apache Beam SDK worker launcher.
ENTRYPOINT [ "/opt/apache/beam/boot" ]
