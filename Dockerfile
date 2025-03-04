FROM python:3.13-slim

RUN mkdir /app
WORKDIR /app

COPY . /app

RUN pip install poetry

ENV PATH="/root/.local/bin:${PATH}"

RUN poetry install --no-root --no-interaction --no-ansi