# Dockerfile for the Market Data Pipeline Project

# Base image
FROM python:3.14.4-slim


# PYTHONUNBUFFERED=1 is an environment variable that forces Python to flush its standard output (stdout)
# and standard error (stderr) streams immediately.
# By default, Python buffers its output to optimize performance, meaning it collects print() statements
# and logs in temporary memory before writing them out in batches. 
# Setting this variable bypasses that internal memory buffer so your application outputs text in real-time.
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1


# Set the working directory. Creates the directory if it doesn't exist.
WORKDIR /data-pipeline


# Install GCC and required build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*


# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Note:
# It is a security best practice of "least privilege" by preventing your container from running
# as the root user. By default, Docker containers run commands as the root user,
# which possesses unrestricted administrative privileges.
# If an attacker exploits a vulnerability in your Python application, they inherit that root access
# and could potentially compromise the host machine hosting the container.

# We are adding this at the end of the Dockerfile to ensure that all the necessary installations and configurations
# are completed before switching to a non-root user. This way, the non-root user can
RUN adduser --disabled-password --gecos '' appuser
USER appuser

# Copy the entire project directory into the container
COPY . .