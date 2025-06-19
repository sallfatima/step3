# Use a base image with CUDA 12.1 and Ubuntu 20.04
FROM nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu20.04

# Set environment variables to prevent interaction and ensure non-root installations work smoothly
ENV DEBIAN_FRONTEND=noninteractive
ENV NCCL_P2P_DISABLE=1
ENV NCCL_IB_DISABLE=1
ENV PATH="/opt/conda/bin:$PATH"

# Define variables
ARG ROOT=/geo-mapping
ARG CUR_MODULE=modules/inference_pipeline
ARG CUR_DIR=$ROOT/$CUR_MODULE
ARG CONDA_ENV=geo-env

# Install system packages
RUN apt-get update && apt-get install -y \
    wget curl unzip \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    git bzip2 && \
    rm -rf /var/lib/apt/lists/*

# Install Miniconda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda && \
    rm Miniconda3-latest-Linux-x86_64.sh && \
    conda clean -afy

# Create a conda environment with PyTorch 1.11 and CUDA 11.3
RUN conda create -n $CONDA_ENV python=3.9 && \
    conda run -n $CONDA_ENV pip install -U pip && \
    conda run -n $CONDA_ENV conda install pytorch==1.11.0 torchvision==0.12.0 torchaudio==0.11.0 cudatoolkit=11.3 -c pytorch && \
    conda clean -afy

# Activate conda environment for future RUN commands
SHELL ["conda", "run", "-n", "geo-env", "/bin/bash", "-c"]

# Install OpenMIM, MMEngine, and MMCV in the conda environment
RUN pip install -U openmim && \
    mim install mmengine && \
    mim install "mmcv-full==1.5.0" -f https://download.openmmlab.com/mmcv/dist/cu113/torch1.11.0/index.html

# Set the working directory in the container
WORKDIR $CUR_DIR

# Clone Co-DETR and install its dependencies
RUN git clone https://github.com/Sense-X/Co-DETR.git && \
    cd Co-DETR && \
    pip install -e . && \
    pip install yapf==0.40.1

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Copy only the Poetry files to leverage Docker cache
COPY $CUR_MODULE/pyproject.toml $CUR_MODULE/poetry.lock ./

# Install dependencies for feature_pipeline
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi --no-root

# Copy the configuration files
COPY conf/ ../../conf

# Copy the rest of the application code
COPY $CUR_MODULE/ .

# Set environment variables
ENV PYTHONPATH="${PYTHONPATH}:${CUR_DIR}/utils"

# Expose a port
EXPOSE 8080

# Define the entrypoint script
ENTRYPOINT ["conda", "run", "-n", "geo-env", "poetry", "run", "python", "main.py"]
