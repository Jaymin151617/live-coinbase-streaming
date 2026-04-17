# ------------------------------------------------------------------------------
# Base Image
# ------------------------------------------------------------------------------
# Use a lightweight Python 3.12 image to minimize container size
FROM python:3.12-slim

# ------------------------------------------------------------------------------
# Environment Variables
# ------------------------------------------------------------------------------
# PYTHONDONTWRITEBYTECODE:
#   Prevents Python from writing .pyc files to disk (reduces clutter)
# PYTHONUNBUFFERED:
#   Ensures stdout/stderr are unbuffered (important for real-time logging)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ------------------------------------------------------------------------------
# Working Directory
# ------------------------------------------------------------------------------
# Set the working directory inside the container
WORKDIR /app

# ------------------------------------------------------------------------------
# System Dependencies
# ------------------------------------------------------------------------------
# Update package lists and install minimal required system packages
# ca-certificates:
#   Required for SSL/TLS verification (important for secure network calls)
# --no-install-recommends:
#   Avoids installing unnecessary packages to keep image small
# Cleanup:
#   Remove apt cache to reduce final image size
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------------------------
# Python Dependencies
# ------------------------------------------------------------------------------
# Copy only the requirements file first to leverage Docker layer caching
# This ensures dependencies are not reinstalled unless requirements.txt changes
COPY src/kafka/requirements.txt .

# Install Python dependencies
# --no-cache-dir:
#   Prevents pip from caching packages, reducing image size
RUN pip install --no-cache-dir -r requirements.txt

# ------------------------------------------------------------------------------
# Application Source Code
# ------------------------------------------------------------------------------
# Copy the entire source directory into the container
COPY src /app/src

# ------------------------------------------------------------------------------
# Directory Setup
# ------------------------------------------------------------------------------
# Create required directories for logs and secrets
# These directories can be used for runtime file storage
RUN mkdir -p /app/secrets

# ------------------------------------------------------------------------------
# Container Startup Command
# ------------------------------------------------------------------------------
# Run the Kafka producer script when the container starts
CMD ["python", "src/kafka/producer.py"]