# Use Python 3.13 slim image
FROM python:3.13-slim

# Set environment variable placeholders
ARG OUTPUT_PATH
ARG INPUT_FOLDER
ARG MOCK=false
WORKDIR /app

COPY . /app

# Install Poetry
RUN pip install --no-cache-dir poetry

ENV PATH="/root/.local/bin:${PATH}"

# Install dependencies with Poetry
RUN poetry install --no-root --no-interaction --no-ansi

RUN chmod +x /app/entrypoint.sh
WORKDIR /app
CMD ["/app/entrypoint.sh"]