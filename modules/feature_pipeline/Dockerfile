# Use an official Python runtime as the base image
FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y \
        gcc libffi-dev libpq-dev build-essential \
        wget curl unzip libfontconfig1 fonts-liberation \
        libasound2 libatk-bridge2.0-0 libatk1.0-0 libatspi2.0-0 \
        libcups2 libdbus-1-3 libdrm2 libgbm1 libgtk-3-0 libnspr4 \
        libnss3 libwayland-client0 libxcomposite1 libxdamage1 \
        libxfixes3 libxkbcommon0 libxrandr2 xdg-utils libu2f-udev \
        libvulkan1 && \
    curl -LO https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Define env variable paths
ENV ROOT=/geo-mapping
ENV CUR_MODULE=modules/feature_pipeline
ENV CUR_DIR=$ROOT/$CUR_MODULE

# Ensure Poetry binaries are in PATH
ENV PATH="/root/.local/bin:$PATH"

# Set the working directory in the container
WORKDIR $CUR_DIR

# Copy only the Poetry files to leverage Docker cache
COPY $CUR_MODULE/pyproject.toml $CUR_MODULE/poetry.lock ./

# Install dependencies with Poetry
RUN poetry install --no-interaction --no-ansi --no-root

# Copy the configuration files
COPY conf/ ../../conf

# Copy the rest of the application code
COPY $CUR_MODULE/ .

# Set environment variables
ENV PYTHONPATH="${PYTHONPATH}:${CUR_DIR}/osm_utils:${CUR_DIR}/utils"

# Expose a port
EXPOSE 8080

# Define the entrypoint script
ENTRYPOINT ["poetry", "run", "python", "main.py"]
