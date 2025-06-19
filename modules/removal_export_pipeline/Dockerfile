# Use a PyTorch base image with Python 3.12 and PyTorch 2.4
FROM pytorch/pytorch:2.4.0-cuda12.4-cudnn9-runtime

# Update apt-get and install necessary packages
RUN apt-get update && \
    apt-get install -y \
        wget curl unzip \
        libgl1-mesa-glx \
        libglib2.0-0 \
        libsm6 \
        libxext6 \
        libxrender1 && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Define variables
ARG ROOT=/geo-mapping
ARG CUR_MODULE=modules/removal_export_pipeline
ARG CUR_DIR=$ROOT/$CUR_MODULE

# Set the working directory in the container
WORKDIR $CUR_DIR

# Copy only the Poetry files to leverage Docker cache
COPY $CUR_MODULE/pyproject.toml $CUR_MODULE/poetry.lock ./

# Install dependencies for feature_pipeline
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi --no-root

# Copy the configuration files
COPY conf/ ../../conf

# Copy the rest of the application code
COPY $CUR_MODULE/ .

# Set environment variables
ENV PYTHONPATH="${PYTHONPATH}:${CUR_DIR}/image_matching:${CUR_DIR}/utils"

# Expose a port
EXPOSE 8080

# Define the entrypoint script
ENTRYPOINT ["poetry", "run", "python", "main.py"]
